//! The control file: two 4 KiB superblocks, written in turn. The newest one that reads back whole
//! is in charge, so a superblock torn by a crash leaves the one before it in charge.
//!
//! ```text
//! magic: u64 | version: u32 | reserved: u32 | seq: u64 | redo: u64 | segment_bytes: u64 |
//! file_bytes: u64 | database_id: [u8; 16] | heights: [[u8; CLASS_COUNT]; 3] | clock: u64 |
//! writers: u64 | continuations: u64 | (ended: u64, resumed: u64) * continuations | zeros |
//! crc32c: u32
//! ```
//!
//! A superblock whole but of a version this build does not know fails the open: it is neither
//! absent nor torn, and the superblock before it may lack what that one recorded.
//!
//! A file with no superblock in charge is created over only when it holds nothing a database
//! wrote ([`judge`]): it is empty, or it is a creation a crash cut short. Creation writes its first
//! superblock, seq 1, to the second slot while the first was never written, so its first slot is
//! all zeros and its second starts with zeros or the magic, however the write tore. Every other
//! file, a SQLite database among them, is refused and never written.

use super::aligned::AlignedBlock;
use super::log::{GroupStart, Lsn};
use super::meta::TREES;
use super::page::CLASS_COUNT;
use super::whole;
use crate::io::{Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};

const MAGIC: u64 = u64::from_le_bytes(*b"ATOLLSB1");
const VERSION: u32 = 1;
const SIZE: usize = 4096;
const HEIGHTS: usize = 64;
const CLOCK: usize = HEIGHTS + TREES * CLASS_COUNT;
const WRITERS: usize = CLOCK + 8;
const CONTINUATIONS: usize = WRITERS + 8;
/// Continuations a superblock holds: all that fit between their count and the checksum.
pub const MAX_CONTINUATIONS: usize = (SIZE - 4 - CONTINUATIONS - 8) / 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Superblock {
    pub seq: u64,
    /// Where recovery starts reading the log.
    pub redo: Lsn,
    pub segment_bytes: u64,
    pub file_bytes: u64,
    pub database_id: [u8; 16],
    /// Levels of the page table (`[0]`), free space (`[1]`) and retired marks (`[2]`) of each
    /// size class.
    pub heights: [[u8; CLASS_COUNT]; TREES],
    /// The transaction clock when the superblock was written: above every timestamp of a change
    /// logged before the redo point.
    pub clock: u64,
    /// Where the writer table of the checkpoint that wrote this starts in the log (DESIGN.md
    /// §10): the writers that may still need undo by the log before `redo`. `None` for none.
    pub writers: Option<GroupStart>,
    /// Where the log goes on past a gap, in order: it ended at `.0`, and goes on at `.1`. A
    /// recovery reopens the log past where it ends, at the next block, and records the gap here,
    /// durably, before it appends anything: a recovery that crashes and starts again from this
    /// superblock then reads all it appended. A gap stays recorded until the redo point passes it.
    pub continuations: Vec<(Lsn, Lsn)>,
}

impl Superblock {
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![0u8; SIZE];
        out[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        out[8..12].copy_from_slice(&VERSION.to_le_bytes());
        out[16..24].copy_from_slice(&self.seq.to_le_bytes());
        out[24..32].copy_from_slice(&self.redo.to_le_bytes());
        out[32..40].copy_from_slice(&self.segment_bytes.to_le_bytes());
        out[40..48].copy_from_slice(&self.file_bytes.to_le_bytes());
        out[48..64].copy_from_slice(&self.database_id);
        for (t, heights) in self.heights.iter().enumerate() {
            let at = HEIGHTS + t * CLASS_COUNT;
            out[at..at + CLASS_COUNT].copy_from_slice(heights);
        }
        out[CLOCK..CLOCK + 8].copy_from_slice(&self.clock.to_le_bytes());
        out[WRITERS..WRITERS + 8].copy_from_slice(&GroupStart::encode(self.writers).to_le_bytes());
        assert!(
            self.continuations.len() <= MAX_CONTINUATIONS,
            "{} continuations",
            self.continuations.len()
        );
        out[CONTINUATIONS..CONTINUATIONS + 8]
            .copy_from_slice(&(self.continuations.len() as u64).to_le_bytes());
        for (i, &(ended, resumed)) in self.continuations.iter().enumerate() {
            let at = CONTINUATIONS + 8 + 16 * i;
            out[at..at + 8].copy_from_slice(&ended.to_le_bytes());
            out[at + 8..at + 16].copy_from_slice(&resumed.to_le_bytes());
        }
        let crc = crc32c::crc32c(&out[..SIZE - 4]);
        out[SIZE - 4..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// The superblock `bytes` hold: `None` if they are not one whole, an error if they are one
    /// of a version this build does not read.
    fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        let crc = u32::from_le_bytes(bytes[SIZE - 4..SIZE].try_into().unwrap());
        if crc != crc32c::crc32c(&bytes[..SIZE - 4])
            || u64::from_le_bytes(bytes[0..8].try_into().unwrap()) != MAGIC
        {
            return Ok(None);
        }
        let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let continuations = match u32::from_le_bytes(bytes[8..12].try_into().unwrap()) {
            VERSION => {
                let count = word(CONTINUATIONS);
                if count > MAX_CONTINUATIONS as u64 {
                    return Err(LimboError::Corrupt(format!(
                        "a superblock records {count} continuations, more than its {MAX_CONTINUATIONS}"
                    )));
                }
                (0..count as usize)
                    .map(|i| {
                        let at = CONTINUATIONS + 8 + 16 * i;
                        (word(at), word(at + 8))
                    })
                    .collect()
            }
            version => {
                return Err(LimboError::Corrupt(format!(
                    "a superblock of format version {version}, which this build does not read \
                     (it reads {VERSION})"
                )))
            }
        };
        let writers = GroupStart::decode(word(WRITERS)).ok_or_else(|| {
            LimboError::Corrupt(format!(
                "a superblock names its writer table at {}, where no group starts",
                word(WRITERS)
            ))
        })?;
        let mut heights = [[0u8; CLASS_COUNT]; TREES];
        for (t, heights) in heights.iter_mut().enumerate() {
            let at = HEIGHTS + t * CLASS_COUNT;
            heights.copy_from_slice(&bytes[at..at + CLASS_COUNT]);
        }
        Ok(Some(Self {
            seq: word(16),
            redo: word(24),
            segment_bytes: word(32),
            file_bytes: word(40),
            database_id: bytes[48..64].try_into().unwrap(),
            heights,
            clock: word(CLOCK),
            writers,
            continuations,
        }))
    }
}

pub struct Control {
    io: Arc<dyn IO>,
    file: Arc<dyn File>,
}

/// What a control file holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found {
    /// No database: an empty file, or a creation a crash cut short before its superblock was whole.
    Nothing,
    /// A database, and its superblock in charge.
    Database(Superblock),
}

/// What the file at a database's path holds, read without writing it: an error for a file that
/// is no Atoll control file. Turso's open asks this before it opens a database in Atoll mode.
pub fn judge(io: &Arc<dyn IO>, file: &Arc<dyn File>) -> Result<Found> {
    let len = file.size()?;
    if len > 2 * SIZE as u64 {
        return Err(not_atoll(&format!(
            "{len} bytes, past a control file's {}",
            2 * SIZE
        )));
    }
    let block = Arc::new(AlignedBlock::zeroed(2 * SIZE));
    // The bytes read, set once the read lands.
    let got = Arc::new(AtomicUsize::new(0));
    let out = got.clone();
    let c = Completion::new_read(block.region(0, 2 * SIZE), move |res| match res {
        Ok((_, n)) => {
            out.store(n.max(0) as usize, Ordering::Release);
            None
        }
        Err(e) => Some(e),
    });
    io.wait_for_completion(file.pread(0, c)?)?;
    let read = got.load(Ordering::Acquire).min(len as usize);
    // SAFETY: the read filled the block, which nothing else uses.
    let bytes = unsafe { std::slice::from_raw_parts(block.as_ptr(), 2 * SIZE) };
    judge_bytes(&bytes[..read])
}

/// [`judge`] over the bytes a control file holds.
fn judge_bytes(bytes: &[u8]) -> Result<Found> {
    let mut newest: Option<Superblock> = None;
    for i in (0..2).filter(|i| bytes.len() >= (i + 1) * SIZE) {
        if let Some(sb) = Superblock::decode(&bytes[i * SIZE..(i + 1) * SIZE])? {
            if newest.as_ref().is_none_or(|n| sb.seq > n.seq) {
                newest = Some(sb);
            }
        }
    }
    if let Some(sb) = newest {
        return Ok(Found::Database(sb));
    }
    let first = &bytes[..bytes.len().min(SIZE)];
    let second = bytes.get(SIZE..(SIZE + 8).min(bytes.len())).unwrap_or(&[]);
    let magic = &MAGIC.to_le_bytes()[..second.len()];
    if first.iter().all(|&b| b == 0) && (second.iter().all(|&b| b == 0) || second == magic) {
        return Ok(Found::Nothing);
    }
    Err(not_atoll(
        "no superblock, and bytes no creation of one writes",
    ))
}

fn not_atoll(why: &str) -> LimboError {
    LimboError::Corrupt(format!(
        "the file at the database's path is not an Atoll database ({why}); it was left as it is"
    ))
}

impl Control {
    /// Opens the control file at `path` and returns the superblock in charge, if any. A file that
    /// is no Atoll control file fails the open and is left as it is ([`judge`]).
    pub fn open(io: Arc<dyn IO>, path: &str) -> Result<(Self, Option<Superblock>)> {
        let file = io.open_file(path, OpenFlags::Create | OpenFlags::Direct, false)?;
        // The control file's creation must survive a crash as its superblocks do.
        let dir = std::path::Path::new(path)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| ".".to_string());
        io.sync_directory(&dir)?;
        let newest = match judge(&io, &file)? {
            Found::Nothing => None,
            Found::Database(sb) => Some(sb),
        };
        Ok((Self { io, file }, newest))
    }

    /// Writes `sb` over the superblock not in charge; done once it is durable.
    pub fn write(&self, sb: &Superblock) -> Result<Completion> {
        self.write_encoded(sb.seq, &sb.encode())
    }

    /// Writes the encoded superblock `bytes` in the place of `seq`.
    fn write_encoded(&self, seq: u64, bytes: &[u8]) -> Result<Completion> {
        let block = Arc::new(AlignedBlock::zeroed(SIZE));
        // SAFETY: a fresh block of the superblock's size.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), block.as_ptr(), SIZE) };
        let at = (seq % 2) * SIZE as u64;
        let durable = Completion::new_wait();
        let done = durable.clone();
        if self.file.supports_durable_writes() {
            let c = Completion::new_write(move |res| finish(&done, whole(SIZE, res)));
            drop(self.file.pwrite_durable(at, block.region(0, SIZE), c)?);
        } else {
            // The file weakly: it holds the IO the write waits in, so a strong hold would keep that
            // IO alive for good once nothing steps it; a file closed before its sync leaves the
            // superblock not durable.
            let file = Arc::downgrade(&self.file);
            let c = Completion::new_write(move |res| match (whole(SIZE, res), file.upgrade()) {
                (Ok(()), Some(file)) => {
                    let after = done.clone();
                    let synced = Completion::new_sync(move |r| finish(&after, r.map(|_| ())));
                    if let Err(e) = file.sync_data(synced) {
                        finish(&done, Err(io_failure(&e)));
                    }
                }
                (Ok(()), None) => finish(&done, Err(CompletionError::Aborted)),
                (Err(e), _) => finish(&done, Err(e)),
            });
            drop(self.file.pwrite(at, block.region(0, SIZE), c)?);
        }
        Ok(durable)
    }

    /// Writes `sb` and waits until it is durable, as opening a database does.
    pub fn write_and_wait(&self, sb: &Superblock) -> Result<()> {
        let c = self.write(sb)?;
        self.io.wait_for_completion(c)
    }

    /// Done once `sb` is durable.
    pub fn step_write(
        &self,
        sb: &Superblock,
        written: &mut Option<Completion>,
    ) -> Result<IOResult<()>> {
        let c = match written {
            Some(c) => c.clone(),
            None => {
                let c = self.write(sb)?;
                *written = Some(c.clone());
                c
            }
        };
        if !c.finished() {
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        match c.get_error() {
            Some(e) => Err(LimboError::CompletionError(e)),
            None => Ok(IOResult::Done(())),
        }
    }
}

fn finish(c: &Completion, res: std::result::Result<(), CompletionError>) {
    match res {
        Ok(()) => c.complete(0),
        Err(e) => c.error(e),
    }
}

fn io_failure(e: &LimboError) -> CompletionError {
    match e {
        LimboError::CompletionError(e) => *e,
        _ => CompletionError::IOError(std::io::ErrorKind::Other, "superblock IO submission"),
    }
}

#[cfg(test)]
fn seal(bytes: &mut [u8]) {
    let crc = crc32c::crc32c(&bytes[..SIZE - 4]);
    bytes[SIZE - 4..].copy_from_slice(&crc.to_le_bytes());
}

/// Rewrites the superblock in charge of the database at `base` in its own place, as `change`
/// leaves it: a control file as a full one, say, left it.
#[cfg(test)]
pub(crate) fn rewrite_in_charge(
    io: Arc<dyn IO>,
    base: &str,
    change: impl FnOnce(&mut Superblock),
) -> Superblock {
    let (control, sb) = Control::open(io.clone(), base).unwrap();
    let mut sb = sb.expect("a superblock in charge");
    change(&mut sb);
    io.wait_for_completion(control.write_encoded(sb.seq, &sb.encode()).unwrap())
        .unwrap();
    sb
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::crash::{CrashIO, Faults};

    fn superblock(seq: u64) -> Superblock {
        Superblock {
            seq,
            redo: 12_288,
            segment_bytes: 1 << 28,
            file_bytes: 1 << 40,
            database_id: [9; 16],
            heights: [[2; CLASS_COUNT], [1; CLASS_COUNT], [3; CLASS_COUNT]],
            clock: 1 << 40,
            writers: Some(GroupStart::at(0)),
            continuations: vec![(20_000, 20_480), (40_100, 40_960)],
        }
    }

    #[test]
    fn a_superblock_tells_a_writer_table_at_zero_from_none_and_refuses_one_off_the_grid() {
        for writers in [None, Some(GroupStart::at(0)), Some(GroupStart::at(1 << 20))] {
            let sb = Superblock {
                writers,
                ..superblock(3)
            };
            assert_eq!(Superblock::decode(&sb.encode()).unwrap(), Some(sb));
        }
        let mut bytes = superblock(3).encode();
        bytes[WRITERS..WRITERS + 8].copy_from_slice(&8u64.to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..SIZE - 4]);
        bytes[SIZE - 4..].copy_from_slice(&crc.to_le_bytes());
        assert!(Superblock::decode(&bytes).is_err());
    }

    #[test]
    fn a_superblock_reads_back_as_written_and_not_once_any_byte_flips() {
        let sb = superblock(7);
        let bytes = sb.encode();
        assert_eq!(Superblock::decode(&bytes).unwrap(), Some(sb));
        for at in [
            0,
            16,
            24,
            HEIGHTS,
            CLOCK,
            WRITERS,
            CONTINUATIONS,
            CONTINUATIONS + 8,
            SIZE - 1,
        ] {
            let mut torn = bytes.clone();
            torn[at] ^= 1;
            assert_eq!(
                Superblock::decode(&torn).unwrap(),
                None,
                "a flip at {at} went unseen"
            );
        }
    }

    #[test]
    fn a_superblock_holds_as_many_continuations_as_fit_and_refuses_a_count_past_them() {
        let mut sb = superblock(3);
        sb.continuations = (0..MAX_CONTINUATIONS as u64)
            .map(|i| (i * 8192 + 100, i * 8192 + 4096))
            .collect();
        assert_eq!(Superblock::decode(&sb.encode()).unwrap(), Some(sb.clone()));
        let mut bytes = sb.encode();
        bytes[CONTINUATIONS..CONTINUATIONS + 8]
            .copy_from_slice(&(MAX_CONTINUATIONS as u64 + 1).to_le_bytes());
        seal(&mut bytes);
        assert!(Superblock::decode(&bytes).is_err());
    }

    /// A whole superblock of a version this build does not know fails the open, whatever the
    /// other place holds: falling back to it could lose what the unknown one recorded.
    #[test]
    fn a_superblock_of_an_unknown_version_fails_the_open() {
        for unknown_seq in [6, 4] {
            let io: Arc<dyn IO> = Arc::new(CrashIO::new(3));
            let (control, _) = Control::open(io.clone(), "/db/unknown").unwrap();
            control.write_and_wait(&superblock(5)).unwrap();
            let mut bytes = superblock(unknown_seq).encode();
            bytes[8..12].copy_from_slice(&3u32.to_le_bytes());
            seal(&mut bytes);
            io.wait_for_completion(control.write_encoded(unknown_seq, &bytes).unwrap())
                .unwrap();
            let err = Control::open(io, "/db/unknown")
                .map(|(_, sb)| sb)
                .unwrap_err();
            assert!(err.to_string().contains("version 3"), "{err}");
        }
    }

    /// A superblock write that lands short has failed, with or without durable writes: counted as
    /// durable, a checkpoint would release the log before the superblock that needs it is whole.
    #[test]
    fn a_short_superblock_write_fails_and_leaves_the_one_before_in_charge() {
        for durable_writes in [true, false] {
            let crash = Arc::new(CrashIO::new(5));
            crash.set_durable_writes(durable_writes);
            let io: Arc<dyn IO> = crash.clone();
            let (control, newest) = Control::open(io.clone(), "/db/short").unwrap();
            assert_eq!(newest, None);
            control.write_and_wait(&superblock(1)).unwrap();
            crash.set_faults(Faults {
                short_write: 1.0,
                ..Faults::default()
            });
            assert!(
                control.write_and_wait(&superblock(2)).is_err(),
                "a short write counted as durable (durable writes: {durable_writes})"
            );
            crash.set_faults(Faults::default());
            let (_, newest) = Control::open(io, "/db/short").unwrap();
            assert_eq!(newest, Some(superblock(1)));
        }
    }

    /// A creation cut short leaves its first superblock torn at any of its sectors, each landed or
    /// not, over a file of any length up to it: every such file reads as nothing, to create again.
    #[test]
    fn a_creation_torn_at_any_sectors_reads_as_nothing() {
        let sectors = SIZE / crate::io::crash::SECTOR;
        let first = superblock(1).encode();
        for landed in 0u32..1 << sectors {
            let mut bytes = vec![0u8; 2 * SIZE];
            for i in (0..sectors).filter(|i| landed & 1 << i != 0) {
                let at = i * crate::io::crash::SECTOR;
                bytes[SIZE + at..SIZE + at + crate::io::crash::SECTOR]
                    .copy_from_slice(&first[at..at + crate::io::crash::SECTOR]);
            }
            // A sector left unwritten that the superblock holds as zeros leaves it whole all the
            // same.
            let whole = bytes[SIZE..] == first[..];
            for len in [0, SIZE, SIZE + 8, SIZE + 100, 2 * SIZE] {
                let found = judge_bytes(&bytes[..len]).unwrap();
                if whole && len == 2 * SIZE {
                    assert_eq!(found, Found::Database(superblock(1)));
                } else {
                    assert_eq!(
                        found,
                        Found::Nothing,
                        "sectors {landed:#b} over {len} bytes"
                    );
                }
            }
        }
    }

    /// A file no creation writes is refused and left as it is: a SQLite database, a file longer
    /// than two superblocks, and a second slot that starts with neither zeros nor the magic.
    #[test]
    fn a_file_no_atoll_creation_writes_is_refused_and_left_as_it_is() {
        let mut sqlite = vec![0u8; SIZE];
        sqlite[..16].copy_from_slice(b"SQLite format 3\0");
        let mut foreign = vec![0u8; 2 * SIZE];
        foreign[SIZE] = 1;
        for (path, bytes) in [
            ("/db/sqlite", sqlite),
            ("/db/long", vec![0u8; 3 * SIZE]),
            ("/db/foreign", foreign),
        ] {
            let io: Arc<dyn IO> = Arc::new(CrashIO::new(7));
            let file = io.open_file(path, OpenFlags::Create, false).unwrap();
            let buffer = Arc::new(crate::io::Buffer::new(bytes.clone()));
            let c = Completion::new_write(|_| {});
            io.wait_for_completion(file.pwrite(0, buffer, c).unwrap())
                .unwrap();
            let err = match Control::open(io.clone(), path) {
                Ok(_) => panic!("{path} opened as a control file"),
                Err(e) => e,
            };
            assert!(
                err.to_string().contains("not an Atoll database"),
                "{path}: {err}"
            );
            let err = super::super::db::Atoll::open(
                io.clone(),
                path,
                super::super::db::AtollConfig::with_budget(super::super::buffer::MIN_BUDGET),
            )
            .err()
            .unwrap_or_else(|| panic!("{path} opened as a database"));
            assert!(
                err.to_string().contains("not an Atoll database"),
                "{path}: {err}"
            );
            assert_eq!(
                file.size().unwrap(),
                bytes.len() as u64,
                "{path} changed size"
            );
            let back = Arc::new(AlignedBlock::zeroed(bytes.len()));
            let c = Completion::new_read(back.region(0, bytes.len()), |_| None);
            io.wait_for_completion(file.pread(0, c).unwrap()).unwrap();
            // SAFETY: the read filled the block, which nothing else uses.
            let read = unsafe { std::slice::from_raw_parts(back.as_ptr(), bytes.len()) };
            assert_eq!(read, &bytes[..], "{path} was written");
        }
    }
}
