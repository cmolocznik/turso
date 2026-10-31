//! Reading the log in order from a position, for recovery and for followers.
//!
//! The log ends at the first position where no valid record starts: a torn or unwritten record,
//! zeros, a missing segment, or bytes a crash left there. Records span segments freely.

use super::record::{self, Header, Lsn, Parsed, BLOCK};
use super::segment::Segments;
use crate::io::{Completion, IO};
use crate::storage::atoll::aligned::AlignedBlock;
use crate::sync::{Arc, Mutex};
use crate::Result;

/// The largest read the reader makes at once.
const READ: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedRecord {
    pub header: Header,
    pub payload: Vec<u8>,
    pub start: Lsn,
    pub lsn: Lsn,
}

pub struct LogReader {
    io: Arc<dyn IO>,
    segments: Arc<Segments>,
    /// Where the next record starts.
    position: Lsn,
    /// The log's bytes from `window_at` on, read so far.
    window: Vec<u8>,
    window_at: Lsn,
    /// No more bytes can be read: a segment is missing or ends.
    exhausted: bool,
    buffer: Arc<AlignedBlock>,
}

impl LogReader {
    pub(super) fn new(io: Arc<dyn IO>, segments: Arc<Segments>, start: Lsn) -> Self {
        assert!(
            start.is_multiple_of(record::ALIGN),
            "reading the log from {start}"
        );
        let window_at = start - start % BLOCK;
        Self {
            io,
            segments,
            position: start,
            window: Vec::new(),
            window_at,
            exhausted: false,
            buffer: Arc::new(AlignedBlock::zeroed(READ)),
        }
    }

    /// Where the next record starts; once [`next_record`](Self::next_record) has returned `None`, where the log
    /// ends.
    pub fn position(&self) -> Lsn {
        self.position
    }

    /// The next record, or `None` at the end of the log.
    pub fn next_record(&mut self) -> Result<Option<OwnedRecord>> {
        loop {
            let offset = (self.position - self.window_at) as usize;
            let parsed =
                record::parse(self.position, &self.window[offset.min(self.window.len())..]);
            match parsed {
                Parsed::Record(r) => {
                    let out = OwnedRecord {
                        header: r.header,
                        payload: r.payload.to_vec(),
                        start: r.start,
                        lsn: r.lsn,
                    };
                    self.position = out.lsn;
                    self.forget_consumed();
                    return Ok(Some(out));
                }
                Parsed::Padding(len) => {
                    self.position += len;
                    self.forget_consumed();
                }
                Parsed::End => return Ok(None),
                Parsed::Need(n) => {
                    let wanted = self.position + n as u64;
                    if !self.read_more(wanted)? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Drops window bytes before the position once they are a whole read's worth.
    fn forget_consumed(&mut self) {
        let consumed = (self.position - self.window_at) as usize;
        if consumed >= READ {
            let keep_from = consumed - consumed % BLOCK as usize;
            self.window.drain(..keep_from);
            self.window_at += keep_from as u64;
        }
    }

    /// Reads until the window reaches `wanted`; false if the log's bytes stop first.
    fn read_more(&mut self, wanted: Lsn) -> Result<bool> {
        while self.window_at + (self.window.len() as u64) < wanted {
            if self.exhausted {
                return Ok(false);
            }
            let at = self.window_at + self.window.len() as u64;
            let number = self.segments.number(at);
            let Some(file) = self.segments.open(number)? else {
                self.exhausted = true;
                return Ok(false);
            };
            let segment_end = self.segments.start(number + 1);
            let len = ((segment_end - at) as usize).min(READ);
            let into = self.buffer.region(0, len);
            let got = Arc::new(Mutex::new(None));
            let out = got.clone();
            let c = Completion::new_read(into, move |res| match res {
                Ok((_, n)) => {
                    *out.lock() = Some(n.max(0) as usize);
                    None
                }
                Err(e) => Some(e),
            });
            let c = file.pread(at - self.segments.start(number), c)?;
            self.io.wait_for_completion(c)?;
            let n = got.lock().take().unwrap_or(0);
            // SAFETY: the read filled the first `n` bytes of the buffer, and nothing else uses it.
            let bytes = unsafe { std::slice::from_raw_parts(self.buffer.as_ptr(), n) };
            self.window.extend_from_slice(bytes);
            if n < len {
                // The segment's file ends early: nothing after it was ever written.
                self.exhausted = true;
            }
        }
        Ok(true)
    }
}
