//! Reading the log in order from a position, for recovery and for followers.
//!
//! The log ends at the first position where no valid record starts: a torn or unwritten record,
//! zeros, a missing segment, or bytes a crash left there. Records span segments freely.
//!
//! A group's records are returned only once all of them are read ([`super::Log::append_group`]).
//! If the log ends inside a group, it ends at the group's start: the group was never appended. A
//! record that does not continue the group being read ends it the same way; that is what a crash
//! leaves when the log restarted past part of a group and later records follow.

use super::record::{self, Header, Lsn, Parsed, Place, BLOCK};
use super::segment::Segments;
use crate::io::{Completion, IO};
use crate::storage::atoll::aligned::AlignedBlock;
use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};

/// The largest read the reader makes at once.
const READ: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedRecord {
    pub header: Header,
    pub place: Place,
    pub payload: Vec<u8>,
    pub start: Lsn,
    pub lsn: Lsn,
    /// Where the record's group starts: where a reader must begin to read the record. Its own
    /// start if it is in no group.
    pub group: Lsn,
}

pub struct LogReader {
    io: Arc<dyn IO>,
    segments: Arc<Segments>,
    /// Where the next record returned starts; at the end, where the log ends.
    position: Lsn,
    /// Where the next record to parse starts: past `position` while a group is read.
    scan: Lsn,
    /// The group being read, and what of it is read so far.
    group: Vec<OwnedRecord>,
    /// A whole group, being returned.
    ready: std::collections::VecDeque<OwnedRecord>,
    ended: bool,
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
            scan: start,
            group: Vec::new(),
            ready: std::collections::VecDeque::new(),
            ended: false,
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
            if let Some(r) = self.ready.pop_front() {
                self.position = r.lsn;
                return Ok(Some(r));
            }
            if self.ended {
                return Ok(None);
            }
            let Some(r) = self.parse_next()? else {
                return Ok(self.end());
            };
            let place = r.place;
            if !place.grouped() {
                if !self.group.is_empty() {
                    return Ok(self.end());
                }
                self.position = r.lsn;
                return Ok(Some(r));
            }
            if place.first() {
                if !self.group.is_empty() {
                    return Ok(self.end());
                }
            } else {
                let continues = self.group.last().is_some_and(|last| {
                    last.place.size == place.size && last.place.index + 1 == place.index
                });
                if self.group.is_empty() {
                    // Only a group's first record can follow a whole record: a reader starting
                    // inside a group was started at a position no checkpoint names.
                    return Err(LimboError::Corrupt(format!(
                        "a record at {} continues no group",
                        r.start
                    )));
                }
                if !continues {
                    return Ok(self.end());
                }
            }
            let group = self.group.first().map_or(r.start, |g| g.start);
            self.group.push(OwnedRecord { group, ..r });
            if place.last() {
                self.ready.extend(self.group.drain(..));
            }
        }
    }

    /// Ends the log where the last whole record or group ends: a group read only in part was
    /// never appended.
    fn end(&mut self) -> Option<OwnedRecord> {
        self.group.clear();
        self.ended = true;
        None
    }

    /// The next record past `scan`, skipping padding; `None` where no valid record starts.
    fn parse_next(&mut self) -> Result<Option<OwnedRecord>> {
        loop {
            let offset = (self.scan - self.window_at) as usize;
            let parsed = record::parse(self.scan, &self.window[offset.min(self.window.len())..]);
            match parsed {
                Parsed::Record(r) => {
                    let out = OwnedRecord {
                        header: r.header,
                        place: r.place,
                        payload: r.payload.to_vec(),
                        start: r.start,
                        lsn: r.lsn,
                        group: r.start,
                    };
                    self.scan = out.lsn;
                    self.forget_consumed();
                    return Ok(Some(out));
                }
                Parsed::Padding(len) => {
                    if !self.group.is_empty() {
                        // A group is contiguous: padding where its next record belongs means the
                        // rest of it was never written.
                        return Ok(None);
                    }
                    self.position += len;
                    self.scan += len;
                    self.forget_consumed();
                }
                Parsed::End => return Ok(None),
                Parsed::Need(n) => {
                    let wanted = self.scan + n as u64;
                    if !self.read_more(wanted)? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Drops window bytes before the scan once they are a whole read's worth.
    fn forget_consumed(&mut self) {
        let consumed = (self.scan - self.window_at) as usize;
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
