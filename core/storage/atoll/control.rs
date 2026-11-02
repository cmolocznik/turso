//! The control file: two 4 KiB superblocks, written in turn. The newest one that reads back whole
//! is in charge, so a superblock torn by a crash leaves the one before it in charge.
//!
//! ```text
//! magic: u64 | version: u32 | reserved: u32 | seq: u64 | redo: u64 | segment_bytes: u64 |
//! file_bytes: u64 | database_id: [u8; 16] | heights: [[u8; CLASS_COUNT]; 2] | clock: u64 |
//! continuations: u64 | (ended: u64, resumed: u64) * continuations | zeros | crc32c: u32
//! ```
//!
//! A superblock whole but of a version this build does not know fails the open: it is neither
//! absent nor torn, and the superblock before it may lack what that one recorded.

use super::aligned::AlignedBlock;
use super::log::Lsn;
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
const CLOCK: usize = HEIGHTS + 2 * CLASS_COUNT;
const CONTINUATIONS: usize = CLOCK + 8;
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
    /// Levels of the page table (`[0]`) and free space (`[1]`) of each size class.
    pub heights: [[u8; CLASS_COUNT]; 2],
    /// The transaction clock when the superblock was written: above every timestamp of a change
    /// logged before the redo point.
    pub clock: u64,
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
        out[HEIGHTS..HEIGHTS + CLASS_COUNT].copy_from_slice(&self.heights[0]);
        out[HEIGHTS + CLASS_COUNT..HEIGHTS + 2 * CLASS_COUNT].copy_from_slice(&self.heights[1]);
        out[CLOCK..CLOCK + 8].copy_from_slice(&self.clock.to_le_bytes());
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
        let mut heights = [[0u8; CLASS_COUNT]; 2];
        heights[0].copy_from_slice(&bytes[HEIGHTS..HEIGHTS + CLASS_COUNT]);
        heights[1].copy_from_slice(&bytes[HEIGHTS + CLASS_COUNT..HEIGHTS + 2 * CLASS_COUNT]);
        Ok(Some(Self {
            seq: word(16),
            redo: word(24),
            segment_bytes: word(32),
            file_bytes: word(40),
            database_id: bytes[48..64].try_into().unwrap(),
            heights,
            clock: word(CLOCK),
            continuations,
        }))
    }
}

pub struct Control {
    io: Arc<dyn IO>,
    file: Arc<dyn File>,
}

impl Control {
    /// Opens the control file at `path` and returns the superblock in charge, if any.
    pub fn open(io: Arc<dyn IO>, path: &str) -> Result<(Self, Option<Superblock>)> {
        let file = io.open_file(path, OpenFlags::Create | OpenFlags::Direct, false)?;
        // The control file's creation must survive a crash as its superblocks do.
        let dir = std::path::Path::new(path)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| ".".to_string());
        io.sync_directory(&dir)?;
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
        let read = got.load(Ordering::Acquire);
        // SAFETY: the read filled the block, which nothing else uses.
        let bytes = unsafe { std::slice::from_raw_parts(block.as_ptr(), 2 * SIZE) };
        let mut newest: Option<Superblock> = None;
        for i in (0..2).filter(|i| read >= (i + 1) * SIZE) {
            if let Some(sb) = Superblock::decode(&bytes[i * SIZE..(i + 1) * SIZE])? {
                if newest.as_ref().is_none_or(|n| sb.seq > n.seq) {
                    newest = Some(sb);
                }
            }
        }
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
            let file = self.file.clone();
            let c = Completion::new_write(move |res| match whole(SIZE, res) {
                Ok(()) => {
                    let after = done.clone();
                    let synced = Completion::new_sync(move |r| finish(&after, r.map(|_| ())));
                    if let Err(e) = file.sync_data(synced) {
                        finish(&done, Err(io_failure(&e)));
                    }
                }
                Err(e) => finish(&done, Err(e)),
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
            heights: [[2; CLASS_COUNT], [1; CLASS_COUNT]],
            clock: 1 << 40,
            continuations: vec![(20_000, 20_480), (40_100, 40_960)],
        }
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
}
