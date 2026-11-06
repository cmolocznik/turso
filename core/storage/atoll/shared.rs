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

    /// Hands the bytes in `range` to `f` in pieces, in order: plain bytes in one, shared ones
    /// through a buffer on the stack.
    fn pieces(&self, range: Range<usize>, mut f: impl FnMut(&[u8])) {
        let mut piece = [0u8; 512];
        let mut at = range.start;
        while at < range.end {
            let n = piece.len().min(range.end - at);
            self.copy_to(at, &mut piece[..n]);
            f(&piece[..n]);
            at += n;
        }
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

    fn pieces(&self, range: Range<usize>, mut f: impl FnMut(&[u8])) {
        f(&self[range]);
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

    fn pieces(&self, range: Range<usize>, f: impl FnMut(&[u8])) {
        self.as_slice().pieces(range, f);
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

    /// The `n` bytes at `at`, `n` at most a word, as the little-endian integer they spell, in the
    /// low `n` bytes: one load, or two where they straddle words. The bytes above are not cleared.
    #[inline(always)]
    fn load(&self, at: usize, n: usize) -> u64 {
        let (i, s) = (at / WORD, at % WORD);
        let low = u64::from_le(self.words[i].load(Ordering::Relaxed)) >> (8 * s);
        if s + n <= WORD {
            return low;
        }
        // `s` is not 0 here, so the shift is less than 64.
        low | u64::from_le(self.words[i + 1].load(Ordering::Relaxed)) << (8 * (WORD - s))
    }

    /// The `n` bytes at `at`, as [`Self::load`], with the bytes above them cleared.
    #[inline(always)]
    fn load_exact(&self, at: usize, n: usize) -> u64 {
        // The words' own bounds check panics on exactly the reads past the end: the last word
        // holds bytes up to the end, and a read past it needs the word after.
        debug_assert!(at + n <= self.len(), "{at}+{n} past {}", self.len());
        let v = self.load(at, n);
        match n {
            WORD => v,
            _ => v & ((1u64 << (8 * n)) - 1),
        }
    }
}

impl Bytes for Shared {
    #[inline(always)]
    fn len(&self) -> usize {
        self.words.len() * WORD
    }

    #[inline]
    fn copy_to(&self, at: usize, out: &mut [u8]) {
        let end = at + out.len();
        assert!(end <= self.len(), "{at}..{end} past {}", self.len());
        if out.is_empty() {
            return;
        }
        if out.len() <= WORD {
            let n = out.len();
            out.copy_from_slice(&self.load(at, n).to_le_bytes()[..n]);
            return;
        }
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
        let mut quads = words_of.chunks_exact(4);
        let mut k = 0;
        for q in &mut quads {
            let w = [
                q[0].load(Ordering::Relaxed),
                q[1].load(Ordering::Relaxed),
                q[2].load(Ordering::Relaxed),
                q[3].load(Ordering::Relaxed),
            ];
            // SAFETY: `k + 4 <= words`, so the 32 bytes at `k * WORD` lie in `rest`.
            unsafe { dst.add(k * WORD).cast::<[u64; 4]>().write_unaligned(w) };
            k += 4;
        }
        for word in quads.remainder() {
            // SAFETY: `k < words`, so the 8 bytes at `k * WORD` lie in `rest`.
            unsafe {
                dst.add(k * WORD)
                    .cast::<u64>()
                    .write_unaligned(word.load(Ordering::Relaxed))
            };
            k += 1;
        }
        let tail = &mut rest[words * WORD..];
        if !tail.is_empty() {
            let n = tail.len();
            tail.copy_from_slice(&self.word((end - n) / WORD)[..n]);
        }
    }

    // Fields and comparisons are read in registers, a word at a time, with nothing copied on the
    // way: what plain bytes would compile to.

    #[inline(always)]
    fn compare(&self, at: usize, other: &[u8]) -> Order {
        // As for a load: the words' bounds check refuses a comparison past the end.
        debug_assert!(
            at + other.len() <= self.len(),
            "{at}+{} past {}",
            other.len(),
            self.len()
        );
        let mut k = 0;
        while k + WORD <= other.len() {
            // Bytes compare as the big-endian integers they spell.
            let mine = self.load(at + k, WORD).swap_bytes();
            let theirs = u64::from_be_bytes(other[k..k + WORD].try_into().unwrap());
            if mine != theirs {
                return mine.cmp(&theirs);
            }
            k += WORD;
        }
        let n = other.len() - k;
        if n == 0 {
            return Order::Equal;
        }
        let top = !0u64 << (8 * (WORD - n));
        let mine = self.load(at + k, n).swap_bytes() & top;
        let mut tail = [0u8; WORD];
        tail[..n].copy_from_slice(&other[k..]);
        mine.cmp(&u64::from_be_bytes(tail))
    }

    #[inline(always)]
    fn u8_at(&self, at: usize) -> u8 {
        self.load_exact(at, 1) as u8
    }

    #[inline(always)]
    fn u16_at(&self, at: usize) -> u16 {
        self.load_exact(at, 2) as u16
    }

    #[inline(always)]
    fn u32_at(&self, at: usize) -> u32 {
        self.load_exact(at, 4) as u32
    }

    #[inline(always)]
    fn u64_at(&self, at: usize) -> u64 {
        self.load_exact(at, WORD)
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

/// A writer reads as its bytes do, word loads and all: the node operations read through it.
impl Bytes for Writer<'_> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn copy_to(&self, at: usize, out: &mut [u8]) {
        self.0.copy_to(at, out);
    }

    #[inline(always)]
    fn compare(&self, at: usize, other: &[u8]) -> Order {
        self.0.compare(at, other)
    }

    #[inline(always)]
    fn u8_at(&self, at: usize) -> u8 {
        self.0.u8_at(at)
    }

    #[inline(always)]
    fn u16_at(&self, at: usize) -> u16 {
        self.0.u16_at(at)
    }

    #[inline(always)]
    fn u32_at(&self, at: usize) -> u32 {
        self.0.u32_at(at)
    }

    #[inline(always)]
    fn u64_at(&self, at: usize) -> u64 {
        self.0.u64_at(at)
    }
}

impl Writer<'_> {
    /// Stores `v`'s low `n` bytes, `n` at most a word, at `at`, little-endian: each word they
    /// touch loaded, spliced and stored back.
    #[inline(always)]
    fn store(&self, at: usize, n: usize, v: u64) {
        // As for a load, the words' own bounds check refuses a store past the end.
        debug_assert!(at + n <= self.len(), "{at}+{n} past {}", self.len());
        let words = &self.0.words;
        let (i, s) = (at / WORD, at % WORD);
        let mask = match n {
            WORD => !0u64,
            _ => (1u64 << (8 * n)) - 1,
        };
        // Relaxed: the bytes of a class 2 object, under the writer's exclusive latch.
        let w = u64::from_le(words[i].load(Ordering::Relaxed));
        let w = w & !(mask << (8 * s)) | (v & mask) << (8 * s);
        words[i].store(w.to_le(), Ordering::Relaxed);
        if s + n > WORD {
            let shift = 8 * (WORD - s);
            let w = u64::from_le(words[i + 1].load(Ordering::Relaxed));
            let w = w & !(mask >> shift) | (v & mask) >> shift;
            words[i + 1].store(w.to_le(), Ordering::Relaxed);
        }
    }
}

impl BytesMut for Writer<'_> {
    #[inline(always)]
    fn put_u8(&mut self, at: usize, v: u8) {
        self.store(at, 1, v as u64);
    }

    #[inline(always)]
    fn put_u16(&mut self, at: usize, v: u16) {
        self.store(at, 2, v as u64);
    }

    #[inline(always)]
    fn put_u32(&mut self, at: usize, v: u32) {
        self.store(at, 4, v as u64);
    }

    #[inline(always)]
    fn put_u64(&mut self, at: usize, v: u64) {
        self.store(at, WORD, v);
    }

    #[inline]
    fn write(&mut self, at: usize, bytes: &[u8]) {
        let end = at + bytes.len();
        assert!(end <= self.len(), "{at}..{end} past {}", self.len());
        if bytes.is_empty() {
            return;
        }
        if bytes.len() <= WORD {
            let mut v = [0u8; WORD];
            v[..bytes.len()].copy_from_slice(bytes);
            return self.store(at, bytes.len(), u64::from_le_bytes(v));
        }
        // A part word, then whole words, then a part word.
        let head = ((WORD - at % WORD) % WORD).min(bytes.len());
        let (first, rest) = bytes.split_at(head);
        if !first.is_empty() {
            self.splice(at / WORD, at % WORD, first);
        }
        let (from, words) = ((at + head) / WORD, rest.len() / WORD);
        let src = rest.as_ptr();
        let to = &self.0.words[from..from + words];
        let mut quads = to.chunks_exact(4);
        let mut k = 0;
        for q in &mut quads {
            // SAFETY: `k + 4 <= words`, so the 32 bytes at `k * WORD` lie in `rest`.
            let w = unsafe { src.add(k * WORD).cast::<[u64; 4]>().read_unaligned() };
            q[0].store(w[0], Ordering::Relaxed);
            q[1].store(w[1], Ordering::Relaxed);
            q[2].store(w[2], Ordering::Relaxed);
            q[3].store(w[3], Ordering::Relaxed);
            k += 4;
        }
        for word in quads.remainder() {
            // SAFETY: `k < words`, so the 8 bytes at `k * WORD` lie in `rest`.
            word.store(
                unsafe { src.add(k * WORD).cast::<u64>().read_unaligned() },
                Ordering::Relaxed,
            );
            k += 1;
        }
        let tail = &rest[words * WORD..];
        if !tail.is_empty() {
            self.splice((end - tail.len()) / WORD, 0, tail);
        }
    }

    /// Whole words stored at once, the part words at either end spliced.
    fn fill(&mut self, range: Range<usize>, v: u8) {
        assert!(range.end <= self.len(), "{range:?} past {}", self.len());
        let first = range.start.next_multiple_of(WORD).min(range.end);
        let last = (range.end / WORD * WORD).max(first);
        let part = [v; WORD];
        if first > range.start {
            self.write(range.start, &part[..first - range.start]);
        }
        let word = u64::from_ne_bytes(part);
        for w in &self.0.words[first / WORD..last / WORD] {
            w.store(word, Ordering::Relaxed);
        }
        if range.end > last {
            self.write(last, &part[..range.end - last]);
        }
    }

    /// Bytes moved a word at a time where the source and the destination lie alike in their
    /// words, in the direction an overlap needs; else through a buffer, as the default does.
    fn copy_within(&mut self, src: Range<usize>, dest: usize) {
        let len = src.len();
        assert!(src.end <= self.len() && dest + len <= self.len());
        if len < 2 * WORD || src.start % WORD != dest % WORD {
            let mut chunk = [0u8; 256];
            let mut moved = 0;
            while moved < len {
                let n = chunk.len().min(len - moved);
                let from = if dest > src.start {
                    src.start + len - moved - n
                } else {
                    src.start + moved
                };
                self.copy_to(from, &mut chunk[..n]);
                self.write(dest + (from - src.start), &chunk[..n]);
                moved += n;
            }
            return;
        }
        // The part word at each end, then the whole words between, each read before the one it
        // lands on is written.
        let head = (WORD - src.start % WORD) % WORD;
        let tail = (src.start + len) % WORD;
        let words = (len - head - tail) / WORD;
        let (s, d) = ((src.start + head) / WORD, (dest + head) / WORD);
        let mut ends = [[0u8; WORD]; 2];
        self.copy_to(src.start, &mut ends[0][..head]);
        self.copy_to(src.end - tail, &mut ends[1][..tail]);
        let all = &self.0.words;
        if d < s {
            for k in 0..words {
                all[d + k].store(all[s + k].load(Ordering::Relaxed), Ordering::Relaxed);
            }
        } else {
            for k in (0..words).rev() {
                all[d + k].store(all[s + k].load(Ordering::Relaxed), Ordering::Relaxed);
            }
        }
        self.write(dest, &ends[0][..head]);
        self.write(dest + len - tail, &ends[1][..tail]);
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
            // Up to the end itself, where only an empty change fits.
            let at = next(len + 1);
            let n = next(len - at + 1);
            match round % 6 {
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
                    let v = (next(1 << 30) as u64) << 34 | (next(1 << 30) as u64) << 4;
                    w.put_u64(at, v);
                    plain.as_mut_slice().put_u64(at, v);
                }
                4 if at + 4 <= len => {
                    let v = next(1 << 30) as u32 | (next(4) as u32) << 30;
                    w.put_u32(at, v);
                    plain.as_mut_slice().put_u32(at, v);
                    let v = next(1 << 16) as u16;
                    w.put_u16(at, v);
                    plain.as_mut_slice().put_u16(at, v);
                    w.put_u8(at + 3, v as u8);
                    plain.as_mut_slice().put_u8(at + 3, v as u8);
                }
                _ => {}
            }
            assert_eq!(w.copied(0..len), plain, "round {round}");
            let at = next(len - 7);
            assert_eq!(w.shared().u64_at(at), plain.as_slice().u64_at(at));
            assert_eq!(w.shared().u32_at(at), plain.as_slice().u32_at(at));
            assert_eq!(w.shared().u16_at(at), plain.as_slice().u16_at(at));
            assert_eq!(w.shared().u8_at(at), plain[at]);
            let n = next(len - at + 1);
            assert_eq!(w.shared().compare(at, &plain[at..at + n]), Order::Equal);
            // Against bytes one of which differs, either way, anywhere in them.
            if n > 0 {
                let mut other = plain[at..at + n].to_vec();
                let d = next(n);
                other[d] = other[d].wrapping_add(1 + next(255) as u8);
                assert_eq!(
                    w.shared().compare(at, &other),
                    plain[at..at + n].cmp(&other[..]),
                    "round {round}: {n} bytes at {at}, byte {d} differs"
                );
            }
            let mut out = vec![0u8; n];
            w.shared().copy_to(at, &mut out);
            assert_eq!(out, plain[at..at + n]);
            // The writer reads what its bytes read.
            assert_eq!(Bytes::u64_at(&w, at), plain.as_slice().u64_at(at));
            assert_eq!(Bytes::u32_at(&w, at), plain.as_slice().u32_at(at));
            assert_eq!(Bytes::u16_at(&w, at), plain.as_slice().u16_at(at));
            assert_eq!(Bytes::u8_at(&w, at), plain[at]);
            assert_eq!(Bytes::compare(&w, at, &plain[at..at + n]), Order::Equal);
            let other: Vec<u8> = (0..n).map(|_| next(256) as u8).collect();
            assert_eq!(
                Bytes::compare(&w, at, &other),
                plain[at..at + n].cmp(&other[..]),
                "round {round}: the writer's comparison of {n} bytes at {at}"
            );
        }
        // Nothing, at the very end.
        w.shared().copy_to(len, &mut []);
        assert_eq!(w.shared().compare(len, &[]), Order::Equal);
        w.write(len, &[]);
    }
}
