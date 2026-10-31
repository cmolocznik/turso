//! The control file: two 4 KiB superblocks, written in turn. The newest one that reads back whole
//! is in charge, so a superblock torn by a crash leaves the one before it in charge.
//!
//! ```text
//! magic: u64 | version: u32 | reserved: u32 | seq: u64 | redo: u64 | segment_bytes: u64 |
//! file_bytes: u64 | database_id: [u8; 16] | heights: [[u8; CLASS_COUNT]; 2] | zeros | crc32c: u32
//! ```

use super::aligned::AlignedBlock;
use super::log::Lsn;
use super::page::CLASS_COUNT;
use crate::io::{Completion, File, OpenFlags, IO};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};

const MAGIC: u64 = u64::from_le_bytes(*b"ATOLLSB1");
const VERSION: u32 = 1;
const SIZE: usize = 4096;
const HEIGHTS: usize = 64;

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
        let crc = crc32c::crc32c(&out[..SIZE - 4]);
        out[SIZE - 4..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let crc = u32::from_le_bytes(bytes[SIZE - 4..SIZE].try_into().unwrap());
        if crc != crc32c::crc32c(&bytes[..SIZE - 4])
            || u64::from_le_bytes(bytes[0..8].try_into().unwrap()) != MAGIC
            || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != VERSION
        {
            return None;
        }
        let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let mut heights = [[0u8; CLASS_COUNT]; 2];
        heights[0].copy_from_slice(&bytes[HEIGHTS..HEIGHTS + CLASS_COUNT]);
        heights[1].copy_from_slice(&bytes[HEIGHTS + CLASS_COUNT..HEIGHTS + 2 * CLASS_COUNT]);
        Some(Self {
            seq: word(16),
            redo: word(24),
            segment_bytes: word(32),
            file_bytes: word(40),
            database_id: bytes[48..64].try_into().unwrap(),
            heights,
        })
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
        let got = Arc::new(Mutex::new(0usize));
        let out = got.clone();
        let c = Completion::new_read(block.region(0, 2 * SIZE), move |res| match res {
            Ok((_, n)) => {
                *out.lock() = n.max(0) as usize;
                None
            }
            Err(e) => Some(e),
        });
        io.wait_for_completion(file.pread(0, c)?)?;
        let read = *got.lock();
        // SAFETY: the read filled the block, which nothing else uses.
        let bytes = unsafe { std::slice::from_raw_parts(block.as_ptr(), 2 * SIZE) };
        let newest = (0..2)
            .filter(|i| read >= (i + 1) * SIZE)
            .filter_map(|i| Superblock::decode(&bytes[i * SIZE..(i + 1) * SIZE]))
            .max_by_key(|sb| sb.seq);
        Ok((Self { io, file }, newest))
    }

    /// Writes `sb` over the superblock not in charge; done once it is durable.
    pub fn write(&self, sb: &Superblock) -> Result<Completion> {
        let block = Arc::new(AlignedBlock::zeroed(SIZE));
        // SAFETY: a fresh block of the superblock's size.
        unsafe { std::ptr::copy_nonoverlapping(sb.encode().as_ptr(), block.as_ptr(), SIZE) };
        let at = (sb.seq % 2) * SIZE as u64;
        let durable = Completion::new_wait();
        let done = durable.clone();
        if self.file.supports_durable_writes() {
            let c = Completion::new_write(move |res| finish(&done, res.map(|_| ())));
            drop(self.file.pwrite_durable(at, block.region(0, SIZE), c)?);
        } else {
            let file = self.file.clone();
            let c = Completion::new_write(move |res| match res {
                Ok(_) => {
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
mod tests {
    use super::*;

    #[test]
    fn a_superblock_reads_back_as_written_and_not_once_any_byte_flips() {
        let sb = Superblock {
            seq: 7,
            redo: 12_288,
            segment_bytes: 1 << 28,
            file_bytes: 1 << 40,
            database_id: [9; 16],
            heights: [[2; CLASS_COUNT], [1; CLASS_COUNT]],
        };
        let bytes = sb.encode();
        assert_eq!(Superblock::decode(&bytes), Some(sb));
        for at in [0, 16, 24, HEIGHTS, SIZE - 1] {
            let mut torn = bytes.clone();
            torn[at] ^= 1;
            assert_eq!(
                Superblock::decode(&torn),
                None,
                "a flip at {at} went unseen"
            );
        }
    }
}
