//! Bytes one writer changes while optimistic readers may read them: a page in a buffer frame, a
//! metadata page.
//!
//! A reader holds no latch and validates the version afterwards, so its read can overlap a write.
//! A plain read overlapping a plain write is a data race, undefined behaviour even when validation
//! then throws the result away. So both sides reach such bytes only through relaxed atomic loads
//! and stores of whole 64-bit words, which on the targets Atoll runs on compile to the same moves
//! as plain ones. A reader may see a mix of before and after; its version check decides whether it
//! counts. The writer holds the exclusive latch, so no two writes overlap, and a word it changes
//! in part is loaded, spliced and stored back.
//!
//! [`Shared`] bytes are read by anyone and written only through a [`Writer`], which only a holder
//! of the exclusive latch, or the bytes' sole owner, can make. The page formats read through
//! [`Bytes`] and write through [`BytesMut`], so they run the same over shared bytes and over plain
//! ones a thread owns: a copy on its way to disk, a block just read, a node being built.
//!
//! The words are `std`'s atomics even under `shuttle`: the latch around them is where schedules
//! interleave, and a scheduling point per word would make a page copy cost thousands of them.

use std::cmp::Ordering as Order;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

const WORD: usize = 8;

/// Bytes the page formats read.
pub trait Bytes {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Copies the bytes at `at` into `out`.
    fn copy_to(&self, at: usize, out: &mut [u8]);

    /// The bytes at `at`, of `other`'s length, compared with `other`.
    fn compare(&self, at: usize, other: &[u8]) -> Order {
        let mut chunk = [0u8; 64];
        let mut done = 0;
        while done < other.len() {
            let n = chunk.len().min(other.len() - done);
            self.copy_to(at + done, &mut chunk[..n]);
            match chunk[..n].cmp(&other[done..done + n]) {
                Order::Equal => done += n,
                unequal => return unequal,
            }
        }
        Order::Equal
    }

    /// The `N` bytes at `at`.
    fn bytes<const N: usize>(&self, at: usize) -> [u8; N] {
        let mut out = [0u8; N];
        self.copy_to(at, &mut out);
        out
    }

    /// The bytes in `range`, copied.
    fn copied(&self, range: Range<usize>) -> Vec<u8> {
        let mut out = vec![0u8; range.len()];
        self.copy_to(range.start, &mut out);
        out
    }

    fn u8_at(&self, at: usize) -> u8 {
        self.bytes::<1>(at)[0]
    }

    fn u16_at(&self, at: usize) -> u16 {
        u16::from_le_bytes(self.bytes(at))
    }

    fn u32_at(&self, at: usize) -> u32 {
        u32::from_le_bytes(self.bytes(at))
    }

    fn u64_at(&self, at: usize) -> u64 {
        u64::from_le_bytes(self.bytes(at))
    }
}

/// Bytes the page formats write.
pub trait BytesMut: Bytes {
    /// Writes `bytes` at `at`.
    fn write(&mut self, at: usize, bytes: &[u8]);

    /// Sets every byte in `range` to `v`.
    fn fill(&mut self, range: Range<usize>, v: u8) {
        let chunk = [v; 64];
        let mut at = range.start;
        while at < range.end {
            let n = chunk.len().min(range.end - at);
            self.write(at, &chunk[..n]);
            at += n;
        }
    }

    /// Moves the bytes in `src` to start at `dest`, as `<[u8]>::copy_within` does: the ranges may
    /// overlap.
    fn copy_within(&mut self, src: Range<usize>, dest: usize) {
        let mut chunk = [0u8; 256];
        let len = src.len();
        let mut moved = 0;
        while moved < len {
            let n = chunk.len().min(len - moved);
            // Moving up, the far end goes first, so no byte is overwritten before it is read.
            let from = if dest > src.start {
                src.start + len - moved - n
            } else {
                src.start + moved
            };
            self.copy_to(from, &mut chunk[..n]);
            self.write(dest + (from - src.start), &chunk[..n]);
            moved += n;
        }
    }

    fn put_u8(&mut self, at: usize, v: u8) {
        self.write(at, &[v]);
    }

    fn put_u16(&mut self, at: usize, v: u16) {
        self.write(at, &v.to_le_bytes());
    }

    fn put_u32(&mut self, at: usize, v: u32) {
        self.write(at, &v.to_le_bytes());
    }

    fn put_u64(&mut self, at: usize, v: u64) {
        self.write(at, &v.to_le_bytes());
    }
}

impl Bytes for [u8] {
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn copy_to(&self, at: usize, out: &mut [u8]) {
        out.copy_from_slice(&self[at..at + out.len()]);
    }

    fn compare(&self, at: usize, other: &[u8]) -> Order {
        self[at..at + other.len()].cmp(other)
    }
}

impl BytesMut for [u8] {
    fn write(&mut self, at: usize, bytes: &[u8]) {
        self[at..at + bytes.len()].copy_from_slice(bytes);
    }

    fn fill(&mut self, range: Range<usize>, v: u8) {
        self[range].fill(v);
    }

    fn copy_within(&mut self, src: Range<usize>, dest: usize) {
        <[u8]>::copy_within(self, src, dest);
    }
}

impl Bytes for Vec<u8> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn copy_to(&self, at: usize, out: &mut [u8]) {
        self.as_slice().copy_to(at, out);
    }

    fn compare(&self, at: usize, other: &[u8]) -> Order {
        self.as_slice().compare(at, other)
    }
}

impl BytesMut for Vec<u8> {
    fn write(&mut self, at: usize, bytes: &[u8]) {
        BytesMut::write(self.as_mut_slice(), at, bytes);
    }

    fn fill(&mut self, range: Range<usize>, v: u8) {
        BytesMut::fill(self.as_mut_slice(), range, v);
    }

    fn copy_within(&mut self, src: Range<usize>, dest: usize) {
        BytesMut::copy_within(self.as_mut_slice(), src, dest);
    }
}

/// Bytes reached only through atomic words. Anyone may read them; see [`Writer`] for writing.
#[repr(transparent)]
pub struct Shared {
    words: [AtomicU64],
}

impl Shared {
    /// The `len` bytes at `ptr`, as words.
    ///
    /// # Safety
    ///
    /// `ptr` is aligned to 8 and `len` a multiple of 8; the bytes stay allocated for `'a`, and for
    /// `'a` are reached only through `Shared`, never through a plain reference.
    pub unsafe fn from_raw<'a>(ptr: *mut u8, len: usize) -> &'a Self {
        debug_assert!(ptr.cast::<AtomicU64>().is_aligned() && len.is_multiple_of(WORD));
        let words = std::ptr::slice_from_raw_parts(ptr.cast::<AtomicU64>(), len / WORD);
        // SAFETY: `Shared` is a transparent wrapper of the words, which the caller vouches for.
        unsafe { &*(words as *const Self) }
    }

    /// Words of its own, zeroed: a page held outside the buffer pool.
    pub fn zeroed(len: usize) -> Box<Self> {
        assert!(len.is_multiple_of(WORD), "{len} is not whole words");
        let words: Box<[AtomicU64]> = (0..len / WORD).map(|_| AtomicU64::new(0)).collect();
        // SAFETY: `Shared` is a transparent wrapper of `[AtomicU64]`.
        unsafe { Box::from_raw(Box::into_raw(words) as *mut Self) }
    }

    /// A writer of bytes no one else can reach.
    pub fn owned(&mut self) -> Writer<'_> {
        Writer(self)
    }

    /// A writer, for the holder of the bytes' exclusive latch.
    ///
    /// # Safety
    ///
    /// No other writer of these bytes exists while it lives: its maker holds their exclusive
    /// latch, or owns them.
    pub unsafe fn writer(&self) -> Writer<'_> {
        Writer(self)
    }

    fn word(&self, i: usize) -> [u8; WORD] {
        self.words[i].load(Ordering::Relaxed).to_ne_bytes()
    }
}

impl Bytes for Shared {
    fn len(&self) -> usize {
        self.words.len() * WORD
    }

    fn copy_to(&self, at: usize, out: &mut [u8]) {
        let end = at + out.len();
        assert!(end <= self.len(), "{at}..{end} past {}", self.len());
        // A part word, then whole words, then a part word.
        let head = ((WORD - at % WORD) % WORD).min(out.len());
        let (first, rest) = out.split_at_mut(head);
        if !first.is_empty() {
            let off = at % WORD;
            first.copy_from_slice(&self.word(at / WORD)[off..off + head]);
        }
        let (from, words) = ((at + head) / WORD, rest.len() / WORD);
        let words_of = &self.words[from..from + words];
        let dst = rest.as_mut_ptr();
        for (k, word) in words_of.iter().enumerate() {
            // SAFETY: `k < words`, so the 8 bytes at `k * WORD` lie in `rest`.
            unsafe {
                dst.add(k * WORD)
                    .cast::<u64>()
                    .write_unaligned(word.load(Ordering::Relaxed))
            };
        }
        let tail = &mut rest[words * WORD..];
        if !tail.is_empty() {
            let n = tail.len();
            tail.copy_from_slice(&self.word((end - n) / WORD)[..n]);
        }
    }

    fn u8_at(&self, at: usize) -> u8 {
        self.word(at / WORD)[at % WORD]
    }
}

/// The one writer of some [`Shared`] bytes.
pub struct Writer<'a>(&'a Shared);

impl Writer<'_> {
    fn splice(&self, i: usize, off: usize, bytes: &[u8]) {
        let mut word = self.0.word(i);
        word[off..off + bytes.len()].copy_from_slice(bytes);
        self.0.words[i].store(u64::from_ne_bytes(word), Ordering::Relaxed);
    }

    /// The bytes it writes, to read.
    pub fn shared(&self) -> &Shared {
        self.0
    }
}

impl Bytes for Writer<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn copy_to(&self, at: usize, out: &mut [u8]) {
        self.0.copy_to(at, out);
    }

    fn u8_at(&self, at: usize) -> u8 {
        self.0.u8_at(at)
    }
}

impl BytesMut for Writer<'_> {
    fn write(&mut self, at: usize, bytes: &[u8]) {
        let end = at + bytes.len();
        assert!(end <= self.len(), "{at}..{end} past {}", self.len());
        // A part word, then whole words, then a part word.
        let head = ((WORD - at % WORD) % WORD).min(bytes.len());
        let (first, rest) = bytes.split_at(head);
        if !first.is_empty() {
            self.splice(at / WORD, at % WORD, first);
        }
        let (from, words) = ((at + head) / WORD, rest.len() / WORD);
        let src = rest.as_ptr();
        for (k, word) in self.0.words[from..from + words].iter().enumerate() {
            // SAFETY: `k < words`, so the 8 bytes at `k * WORD` lie in `rest`.
            word.store(
                unsafe { src.add(k * WORD).cast::<u64>().read_unaligned() },
                Ordering::Relaxed,
            );
        }
        let tail = &rest[words * WORD..];
        if !tail.is_empty() {
            self.splice((end - tail.len()) / WORD, 0, tail);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shared bytes and plain ones take the same changes and read back the same at every offset.
    #[test]
    fn reads_and_writes_at_any_offset_match_plain_bytes() {
        let len = 256;
        let mut shared = Shared::zeroed(len);
        let mut w = shared.owned();
        let mut plain = vec![0u8; len];
        let mut state = 1u64;
        let mut next = |bound: usize| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as usize % bound
        };
        for round in 0..20_000 {
            let at = next(len);
            let n = next(len - at + 1);
            match round % 5 {
                0 => {
                    let bytes: Vec<u8> = (0..n).map(|_| next(256) as u8).collect();
                    w.write(at, &bytes);
                    BytesMut::write(plain.as_mut_slice(), at, &bytes);
                }
                1 => {
                    let v = next(256) as u8;
                    w.fill(at..at + n, v);
                    BytesMut::fill(plain.as_mut_slice(), at..at + n, v);
                }
                2 => {
                    let dest = next(len - n + 1);
                    w.copy_within(at..at + n, dest);
                    BytesMut::copy_within(plain.as_mut_slice(), at..at + n, dest);
                }
                3 if at + 8 <= len => {
                    let v = (next(1 << 30) as u64) << 20 | next(1 << 20) as u64;
                    w.put_u64(at, v);
                    plain.as_mut_slice().put_u64(at, v);
                }
                _ => {}
            }
            assert_eq!(w.copied(0..len), plain, "round {round}");
            let at = next(len - 7);
            assert_eq!(w.shared().u64_at(at), plain.as_slice().u64_at(at));
            assert_eq!(w.shared().u16_at(at), plain.as_slice().u16_at(at));
            assert_eq!(w.shared().u8_at(at), plain[at]);
            let n = next(len - at + 1);
            assert_eq!(w.shared().compare(at, &plain[at..at + n]), Order::Equal);
        }
    }
}
