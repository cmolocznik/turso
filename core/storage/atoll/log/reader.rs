//! Reading the log in order from a position, for recovery and for followers.
//!
//! The log ends at the first position where no valid record starts: a torn or unwritten record,
//! zeros, a missing segment, or bytes a crash left there. Records span segments freely.
//!
//! A group's records are returned only once all of them are read ([`super::Log::append_group`]).
//! If the log ends inside a group, it ends at the group's start: the group was never appended. A
//! record that does not continue the group being read ends it the same way; that is what a crash
//! leaves when the log restarted past part of a group and later records follow.

use super::record::{self, framed_len, GroupStart, Header, Lsn, Parsed, Place, BLOCK};
use super::segment::Segments;
use crate::io::{Completion, IO};
use crate::storage::atoll::aligned::AlignedBlock;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
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
    /// Bytes read at once: a whole [`READ`] in order, a block where one group is wanted.
    read: usize,
    /// Gaps to read past, in order: where the log ended, and where it goes on.
    continuations: std::collections::VecDeque<(Lsn, Lsn)>,
}

impl LogReader {
    pub(super) fn new(io: Arc<dyn IO>, segments: Arc<Segments>, start: Lsn) -> Self {
        Self::reading(io, segments, start, READ)
    }

    fn reading(io: Arc<dyn IO>, segments: Arc<Segments>, start: Lsn, read: usize) -> Self {
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
            buffer: Arc::new(AlignedBlock::zeroed(read)),
            read,
            continuations: Default::default(),
        }
    }

    /// The group that starts at `at`, read whole and checked to start there: one record outside
    /// any group, or every record of one. Read a block at a time, for a walk from group to group.
    pub(super) fn group_at(
        io: Arc<dyn IO>,
        segments: Arc<Segments>,
        at: GroupStart,
    ) -> Result<Vec<OwnedRecord>> {
        let corrupt = || {
            LimboError::Corrupt(format!(
                "no whole group starts at {}, where a chain names one",
                at.get()
            ))
        };
        let mut reader = Self::reading(io, segments, at.get(), BLOCK as usize);
        let first = reader.next_record()?.ok_or_else(corrupt)?;
        if first.start != at.get() || first.group != at.get() {
            return Err(corrupt());
        }
        let mut out = vec![first];
        while out.last().is_some_and(|r| r.place.grouped() && !r.place.last()) {
            let r = reader.next_record()?.ok_or_else(corrupt)?;
            if r.group != at.get() {
                return Err(corrupt());
            }
            out.push(r);
        }
        Ok(out)
    }

    /// Reads past the gaps a superblock records ([`super::super::control::Superblock`]): where
    /// the log ends at a gap's start, it goes on at the gap's end. Gaps behind the start are
    /// passed already.
    pub fn following(mut self, continuations: &[(Lsn, Lsn)]) -> Self {
        self.continuations = continuations
            .iter()
            .copied()
            .filter(|&(_, resumed)| resumed > self.position)
            .collect();
        self
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
                return self.end();
            };
            let place = r.place;
            if !place.grouped() {
                if !self.group.is_empty() {
                    return self.end();
                }
                self.position = r.lsn;
                return Ok(Some(r));
            }
            if place.first() {
                if !self.group.is_empty() {
                    return self.end();
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
                    return self.end();
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
    /// never appended. At a recorded gap, it goes on past it instead.
    fn end(&mut self) -> Result<Option<OwnedRecord>> {
        self.group.clear();
        match self.continuations.front().copied() {
            Some((ended, resumed)) if ended == self.position => {
                self.continuations.pop_front();
                self.position = resumed;
                self.scan = resumed;
                self.window.clear();
                self.window_at = resumed - resumed % BLOCK;
                self.exhausted = false;
                self.next_record()
            }
            // The log ended before a gap recorded past where it ends: records a recovery read
            // and appended after are gone.
            Some((ended, _)) => Err(LimboError::Corrupt(format!(
                "the log ends at {}, before the gap recorded at {ended}",
                self.position
            ))),
            None => {
                self.ended = true;
                Ok(None)
            }
        }
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
        if consumed >= self.read {
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
            let len = ((segment_end - at) as usize).min(self.read);
            let into = self.buffer.region(0, len);
            // The bytes read, set once the read lands.
            let got = Arc::new(AtomicU64::new(0));
            let out = got.clone();
            let c = Completion::new_read(into, move |res| match res {
                Ok((_, n)) => {
                    out.store(n.max(0) as u64, Ordering::Release);
                    None
                }
                Err(e) => Some(e),
            });
            let c = file.pread(at - self.segments.start(number), c)?;
            self.io.wait_for_completion(c)?;
            let n = got.load(Ordering::Acquire) as usize;
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

/// Reads back records a reader of the log found, each by where it starts and its payload's length:
/// in one read of exactly the blocks it lies in, into a buffer kept from one record to the next,
/// each segment's file opened once. Recovery reads a transaction's spilled parts back so at its
/// Commit, holding one part at a time.
pub struct Rereader {
    io: Arc<dyn IO>,
    segments: Segments,
    /// The blocks read last, from the start of the one the record starts in. Grown to a power of
    /// two past the largest record read: as few reallocations as doublings, under twice its size.
    buffer: Arc<AlignedBlock>,
    /// Bytes read so far.
    read: u64,
}

impl Rereader {
    pub(super) fn new(io: Arc<dyn IO>, segments: Segments) -> Self {
        Self {
            io,
            segments,
            buffer: Arc::new(AlignedBlock::zeroed(BLOCK as usize)),
            read: 0,
        }
    }

    /// The payload of the record outside any group that starts at `at`, `len` bytes long, checked
    /// as a reader of the log checks it.
    pub fn payload(&mut self, at: Lsn, len: usize) -> Result<&[u8]> {
        let corrupt = |what: String| LimboError::Corrupt(format!("the record at {at} {what}"));
        let from = at - at % BLOCK;
        let want = ((at + framed_len(len)).next_multiple_of(BLOCK) - from) as usize;
        if self.buffer.len() < want {
            self.buffer = Arc::new(AlignedBlock::zeroed(want.next_power_of_two()));
        }
        let mut done = 0;
        while done < want {
            let pos = from + done as u64;
            let number = self.segments.number(pos);
            let file = self
                .segments
                .open(number)?
                .ok_or_else(|| corrupt("reads back from a missing segment".into()))?;
            let len = ((self.segments.start(number + 1) - pos) as usize).min(want - done);
            let got = Arc::new(AtomicU64::new(0));
            let out = got.clone();
            let c = Completion::new_read(self.buffer.region(done, len), move |res| match res {
                Ok((_, n)) => {
                    out.store(n.max(0) as u64, Ordering::Release);
                    None
                }
                Err(e) => Some(e),
            });
            let c = file.pread(pos - self.segments.start(number), c)?;
            self.io.wait_for_completion(c)?;
            let n = got.load(Ordering::Acquire) as usize;
            self.read += n as u64;
            if n < len {
                return Err(corrupt(format!("reads back short of its {len} bytes")));
            }
            done += len;
        }
        let bytes = &self.buffer.bytes()[(at - from) as usize..want];
        match record::parse(at, bytes) {
            Parsed::Record(r) if r.payload.len() == len && !r.place.grouped() => Ok(r.payload),
            _ => Err(corrupt(format!(
                "reads back as no record of {len} bytes outside a group"
            ))),
        }
    }

    /// Bytes read so far.
    #[cfg(test)]
    pub fn read(&self) -> u64 {
        self.read
    }
}
