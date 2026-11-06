//! A transaction's own log (DESIGN.md §7, §11.4.1; audit 17): the changes its statements made,
//! by key, in the order made.
//!
//! A change is appended with plain stores, under no shared write. A statement rolled back drops
//! its changes and every later one's, from the position where it began. A change is:
//!
//! ```text
//! kind: u8 | key_len: u16 | value_len: u32 | tree: u64 | key | value
//! ```
//!
//! `tree` is the tree's root, which never moves. A put sets the key's value and a delete removes
//! the key, whose value is then empty, in a versioned tree, whose entries carry a version header,
//! or a plain one. A creation makes an empty tree at `tree`, the root of the plain list its undo
//! would put it on as its value; a drain frees the tree at `tree` whole and removes it from the
//! plain list rooted at its value. Replay applies them in order through the trees: a put is a put
//! and a delete a delete, so replaying one again changes nothing.
//!
//! **Chunks** (DESIGN.md §11.4.1). A transaction's changes lie in chunks of the log's chunk size,
//! each keeping room for the header and CRC of the record it becomes. A change starts in a chunk
//! only if its head and key fit in what is left of it, so the two are whole in one chunk; a value
//! longer than what is left runs on into the next chunks. A chunk filled is sealed, and its
//! transaction spills it to the log, where it is then read back from for a rollback
//! ([`Part`]). Changes made outside any transaction ([`Changes::new`]) lie in one chunk that
//! grows.

use super::Statement;
use crate::io::DIRECT_IO_ALIGNMENT;
use crate::storage::atoll::aligned::{AlignedBlock, Lender};
use crate::storage::atoll::log::{GroupStart, Lsn, BLOCK};
use crate::storage::atoll::page::PageId;
use crate::sync::Arc;
use crate::{LimboError, Result};

const PUT: u8 = 1;
const DELETE: u8 = 2;
const CREATE: u8 = 3;
const DRAIN: u8 = 4;
/// A put or delete in a plain tree.
const PLAIN: u8 = 0x80;
/// A put or delete made plainly in a versioned tree by the statement that created it.
const PLAINLY: u8 = 0x40;
/// Bytes of a change before its key.
pub const HEAD: usize = 1 + 2 + 4 + 8;
/// Bytes a chunk keeps before its changes: the header of the record it becomes, and the two words
/// its payload begins with (the transaction's start, or the commit's timestamp, and where the part
/// begins in the changes).
pub const FRONT: usize = 24 + 16;
/// Bytes a chunk keeps after its changes: the record's CRC.
pub const BACK: usize = 4;
/// No change starts in the chunk.
const NONE: usize = usize::MAX;

#[cfg(test)]
crate::thread::thread_local! {
    /// Bytes of changes encoded on this thread: what a measurement counts per operation.
    pub(crate) static ENCODED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The most memory a transaction's buffer on this thread has held since it was last reset.
    pub(crate) static HELD: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// One change, read back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change<'a> {
    Put {
        tree: PageId,
        plain: bool,
        /// Made plainly by the statement that created the versioned tree: replayed as any put,
        /// and undone with the tree, never by key.
        plainly: bool,
        key: &'a [u8],
        value: &'a [u8],
    },
    Delete {
        tree: PageId,
        plain: bool,
        plainly: bool,
        key: &'a [u8],
    },
    /// An empty tree made at `root`, which the list rooted at `list` takes back if it is undone.
    Create { root: PageId, list: PageId },
    /// The tree at `root` freed whole, and its entry on the list rooted at `list` removed.
    Drain { root: PageId, list: PageId },
}

impl Change<'_> {
    pub fn tree(&self) -> PageId {
        match self {
            Change::Put { tree, .. } | Change::Delete { tree, .. } => *tree,
            Change::Create { root, .. } | Change::Drain { root, .. } => *root,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Change::Put { key, .. } | Change::Delete { key, .. } => key,
            Change::Create { .. } | Change::Drain { .. } => &[],
        }
    }
}

/// A change's head and key, read back where they lie whole: what a rollback puts back by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Keyed<'a> {
    /// A put or delete of a versioned tree, neither made plainly: undone by key.
    pub by_key: bool,
    pub tree: PageId,
    pub key: &'a [u8],
    /// Bytes of the whole change.
    pub len: u64,
}

impl<'a> Keyed<'a> {
    /// The change whose head starts `bytes`, its key whole in them.
    pub fn decode(bytes: &'a [u8]) -> Result<Self> {
        let corrupt =
            || LimboError::Corrupt("a transaction's change read back does not decode".into());
        let head = bytes.get(..HEAD).ok_or_else(corrupt)?;
        let kind = head[0];
        let key_len = u16::from_le_bytes([head[1], head[2]]) as usize;
        let value_len = u32::from_le_bytes(head[3..7].try_into().unwrap()) as u64;
        let tree = PageId::from_raw(u64::from_le_bytes(head[7..15].try_into().unwrap()));
        let key = bytes.get(HEAD..HEAD + key_len).ok_or_else(corrupt)?;
        let by_key = kind == PUT || kind == DELETE;
        Ok(Self {
            by_key,
            tree,
            key,
            len: (HEAD + key_len) as u64 + value_len,
        })
    }
}

/// Chunks of changes, all of one size, from the heap, kept for reuse once given back (DESIGN.md
/// §11.4.1). A chunk is lent as an [`AlignedBlock`]: whoever holds it last, a transaction or the
/// log writing a part from it, gives it back here when it drops it, the log once its write lands.
pub struct Chunks {
    bytes: usize,
    /// Chunks given back, by address, for the next taken.
    free: crate::sync::Mutex<Vec<usize>>,
    /// The most kept free.
    keep: usize,
}

impl Chunks {
    /// Chunks of `bytes`, keeping at most `keep` free.
    pub fn new(bytes: usize, keep: usize) -> Arc<Self> {
        assert!(
            bytes > 0 && bytes.is_multiple_of(DIRECT_IO_ALIGNMENT),
            "chunks of {bytes} bytes"
        );
        Arc::new(Self {
            bytes,
            free: crate::sync::Mutex::new(Vec::with_capacity(keep)),
            keep,
        })
    }

    fn layout(&self) -> std::alloc::Layout {
        std::alloc::Layout::from_size_align(self.bytes, DIRECT_IO_ALIGNMENT).expect("a layout")
    }

    /// A chunk: one given back, or a new one.
    pub fn take(self: &Arc<Self>) -> AlignedBlock {
        let kept = self.free.lock().pop();
        let ptr = kept.map_or_else(
            || {
                // SAFETY: the layout has a non-zero size.
                let ptr = unsafe { std::alloc::alloc_zeroed(self.layout()) };
                if ptr.is_null() {
                    std::alloc::handle_alloc_error(self.layout());
                }
                ptr
            },
            |p| p as *mut u8,
        );
        let lender: Arc<dyn Lender> = self.clone();
        // SAFETY: allocated aligned with this size; the allocation lives until given back, and
        // then is kept here or freed, never handed out twice.
        unsafe {
            AlignedBlock::lent(
                std::ptr::NonNull::new(ptr).expect("a chunk"),
                self.bytes,
                lender,
                ptr as u64,
            )
        }
    }
}

impl Lender for Chunks {
    fn give_back(&self, token: u64) {
        let mut free = self.free.lock();
        if free.len() < self.keep {
            free.push(token as usize);
            return;
        }
        drop(free);
        // SAFETY: a chunk this lent, allocated with this layout and no longer held.
        unsafe { std::alloc::dealloc(token as *mut u8, self.layout()) };
    }
}

impl Drop for Chunks {
    fn drop(&mut self) {
        let layout = self.layout();
        for p in self.free.lock().drain(..) {
            // SAFETY: as in `give_back`.
            unsafe { std::alloc::dealloc(p as *mut u8, layout) };
        }
    }
}

/// A part of a transaction's changes spilled to the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Part {
    /// Where it begins in the changes.
    pub from: u64,
    /// Its bytes still the transaction's: fewer than its record holds once a rollback cut it.
    pub len: u64,
    /// Where the first change that starts in it starts, from its first byte; past `len` if none.
    pub first: u64,
    /// Where its record starts.
    pub at: GroupStart,
    /// Its record's LSN: the log durable past it, it can be read back.
    pub lsn: Lsn,
}

/// Where a rollback reads changes back from, newest first ([`Changes::sources_back`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// The chunk in memory at this index.
    Memory(usize),
    /// A part in the log.
    Spilled(Part),
}

struct Chunk {
    block: AlignedBlock,
    /// Bytes of changes it can hold.
    capacity: usize,
    /// Bytes of changes in it, from [`FRONT`].
    len: usize,
    /// Where its first byte lies in the transaction's changes.
    base: u64,
    /// Where the first change that starts in it starts, from its first byte: before it, a value
    /// run on from the chunk before. [`NONE`] while none does.
    first: usize,
}

impl Chunk {
    fn room(&self) -> usize {
        self.capacity - self.len
    }

    fn bytes(&self) -> &[u8] {
        &self.block.bytes()[FRONT..FRONT + self.len]
    }

    fn end(&self) -> u64 {
        self.base + self.len as u64
    }

    /// Writes `bytes` at its end; they fit.
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        let at = FRONT + self.len;
        self.block.bytes_mut()[at..at + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }
}

/// A transaction's changes.
#[derive(Default)]
pub struct Changes {
    /// The chunks in memory, oldest first: each but the last sealed, waiting to be spilled, the
    /// last being filled. Empty before the first change.
    chunks: Vec<Chunk>,
    /// The parts spilled, oldest first, back to back from 0 up to the first chunk in memory.
    parts: Vec<Part>,
    /// Where the changes end.
    end: u64,
    /// Each statement that changed anything, and where its changes begin, in order.
    marks: Vec<(Statement, u64)>,
    /// Every change made and every rollback to a mark, counted: it only rises.
    edits: u64,
    /// The transaction's view moved: every change, rollback, statement and snapshot, counted, and
    /// read without the transaction's lock by a cursor that judged rows at one count.
    views: u64,
    view: Arc<std::sync::atomic::AtomicU64>,
    /// Where chunks come from: these, or the heap without them.
    pool: Option<Arc<Chunks>>,
    /// Bytes of a chunk, its front and back included; 0 for one chunk that grows.
    chunk_bytes: usize,
    /// A chunk's memory kept for the next chunk: one a rollback emptied, or the slot's.
    spare: Option<AlignedBlock>,
}

impl std::fmt::Debug for Changes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Changes")
            .field("end", &self.end)
            .field("chunks", &self.chunks.len())
            .field("parts", &self.parts.len())
            .finish()
    }
}

impl Changes {
    /// Changes made outside any transaction, in one chunk that grows.
    pub fn new() -> Self {
        Self::default()
    }

    /// A transaction's changes, in chunks of `chunk_bytes`, from `pool`, else the heap; the first
    /// in `spare`'s memory, if given.
    pub fn in_chunks(
        pool: Option<Arc<Chunks>>,
        chunk_bytes: usize,
        spare: Option<AlignedBlock>,
    ) -> Self {
        assert!(
            chunk_bytes.is_multiple_of(BLOCK as usize) && chunk_bytes > FRONT + BACK + HEAD,
            "chunks of {chunk_bytes} bytes"
        );
        Self {
            pool,
            chunk_bytes,
            spare,
            ..Self::default()
        }
    }

    /// Statement `statement` put `value` under `key` in the versioned tree rooted at `tree`.
    pub fn put(&mut self, statement: Statement, tree: PageId, key: &[u8], value: &[u8]) {
        self.append(statement, PUT, tree, key, &[value]);
    }

    /// [`put`](Self::put), the value in pieces, copied in in order.
    pub fn put_pieces(&mut self, statement: Statement, tree: PageId, key: &[u8], value: &[&[u8]]) {
        self.append(statement, PUT, tree, key, value);
    }

    /// Statement `statement` deleted `key` from the versioned tree rooted at `tree`.
    pub fn delete(&mut self, statement: Statement, tree: PageId, key: &[u8]) {
        self.append(statement, DELETE, tree, key, &[]);
    }

    /// Statement `statement`, which created the versioned tree rooted at `tree`, put `value`
    /// under `key` there plainly, with no version below.
    pub fn put_plainly(&mut self, statement: Statement, tree: PageId, key: &[u8], value: &[u8]) {
        self.append(statement, PUT | PLAINLY, tree, key, &[value]);
    }

    /// Statement `statement`, which created the versioned tree rooted at `tree`, removed `key`
    /// there plainly.
    pub fn delete_plainly(&mut self, statement: Statement, tree: PageId, key: &[u8]) {
        self.append(statement, DELETE | PLAINLY, tree, key, &[]);
    }

    /// Statement `statement` put `value` under `key` in the plain tree rooted at `tree`.
    pub fn plain_put(&mut self, statement: Statement, tree: PageId, key: &[u8], value: &[u8]) {
        self.append(statement, PUT | PLAIN, tree, key, &[value]);
    }

    /// Statement `statement` deleted `key` from the plain tree rooted at `tree`.
    pub fn plain_delete(&mut self, statement: Statement, tree: PageId, key: &[u8]) {
        self.append(statement, DELETE | PLAIN, tree, key, &[]);
    }

    /// Statement `statement` made an empty tree at `root`, which the list rooted at `list` takes
    /// back if the creation is undone.
    pub fn create(&mut self, statement: Statement, root: PageId, list: PageId) {
        self.append(statement, CREATE, root, &[], &[&list.raw().to_le_bytes()]);
    }

    /// The tree at `root` was freed whole and its entry on the list rooted at `list` removed,
    /// outside any statement.
    pub fn drained(&mut self, root: PageId, list: PageId) {
        self.append(0, DRAIN, root, &[], &[&list.raw().to_le_bytes()]);
    }

    #[inline]
    fn append(
        &mut self,
        statement: Statement,
        kind: u8,
        tree: PageId,
        key: &[u8],
        value: &[&[u8]],
    ) {
        let value_len: usize = value.iter().map(|p| p.len()).sum();
        assert!(
            key.len() <= u16::MAX as usize && value_len <= u32::MAX as usize,
            "a change of a {} byte key and a {value_len} byte value",
            key.len()
        );
        // Statement 0 is outside any statement: such changes are published alone, never rolled
        // back to a mark.
        if statement != 0 && self.marks.last().is_none_or(|&(s, _)| s != statement) {
            debug_assert!(self.marks.last().is_none_or(|&(s, _)| s < statement));
            self.marks.push((statement, self.end));
        }
        self.edits += 1;
        self.moved();
        let whole = HEAD + key.len() + value_len;
        #[cfg(test)]
        ENCODED.with(|n| n.set(n.get() + whole as u64));
        match self.chunks.last_mut() {
            Some(c) if c.room() >= whole => {
                if c.first == NONE {
                    c.first = c.len;
                }
                // SAFETY: the chunk has room for the whole change from its end, and holds its
                // block alone.
                unsafe {
                    let p = c.block.as_ptr().add(FRONT + c.len);
                    p.write(kind);
                    p.add(1)
                        .cast::<u16>()
                        .write_unaligned((key.len() as u16).to_le());
                    p.add(3)
                        .cast::<u32>()
                        .write_unaligned((value_len as u32).to_le());
                    p.add(7).cast::<u64>().write_unaligned(tree.raw().to_le());
                    let mut at = p.add(HEAD);
                    // A rowid's key, the most common, in one store.
                    if let Ok(word) = <[u8; 8]>::try_from(key) {
                        at.cast::<[u8; 8]>().write_unaligned(word);
                    } else {
                        std::ptr::copy_nonoverlapping(key.as_ptr(), at, key.len());
                    }
                    at = at.add(key.len());
                    for piece in value {
                        std::ptr::copy_nonoverlapping(piece.as_ptr(), at, piece.len());
                        at = at.add(piece.len());
                    }
                }
                c.len += whole;
            }
            _ => {
                let mut head = [0u8; HEAD];
                head[0] = kind;
                head[1..3].copy_from_slice(&(key.len() as u16).to_le_bytes());
                head[3..7].copy_from_slice(&(value_len as u32).to_le_bytes());
                head[7..15].copy_from_slice(&tree.raw().to_le_bytes());
                self.append_across(&head, key, value, whole)
            }
        }
        self.end += whole as u64;
    }

    /// [`append`](Self::append) of a change that does not fit what is left of the last chunk.
    #[cold]
    fn append_across(&mut self, head: &[u8], key: &[u8], value: &[&[u8]], whole: usize) {
        if self.chunk_bytes == 0 {
            self.grow(whole);
            let c = self.chunks.last_mut().expect("the chunk grown");
            if c.first == NONE {
                c.first = c.len;
            }
            c.put(head);
            c.put(key);
            for piece in value {
                c.put(piece);
            }
            return;
        }
        if self
            .chunks
            .last()
            .is_none_or(|c| c.room() < head.len() + key.len())
        {
            self.seal();
        }
        let c = self.chunks.last_mut().expect("a chunk");
        assert!(
            c.room() >= head.len() + key.len(),
            "a change of a {} byte key in chunks of {} bytes",
            key.len(),
            self.chunk_bytes
        );
        if c.first == NONE {
            c.first = c.len;
        }
        c.put(head);
        c.put(key);
        for piece in value {
            let mut piece = *piece;
            while !piece.is_empty() {
                if self.chunks.last().is_some_and(|c| c.room() == 0) {
                    self.seal();
                }
                let c = self.chunks.last_mut().expect("a chunk");
                let n = c.room().min(piece.len());
                c.put(&piece[..n]);
                piece = &piece[n..];
            }
        }
    }

    /// Seals the last chunk, if any, and starts the next where it ends.
    fn seal(&mut self) {
        let base = self.chunks.last().map_or(self.end, Chunk::end);
        let block = self.spare.take().unwrap_or_else(|| self.take_block());
        self.chunks.push(Chunk {
            capacity: self.chunk_bytes - FRONT - BACK,
            block,
            len: 0,
            base,
            first: NONE,
        });
        #[cfg(test)]
        HELD.with(|n| n.set(n.get().max(self.memory() as u64)));
    }

    /// A chunk's memory.
    fn take_block(&self) -> AlignedBlock {
        match &self.pool {
            Some(pool) if pool.bytes == self.chunk_bytes => pool.take(),
            _ => AlignedBlock::zeroed(self.chunk_bytes),
        }
    }

    /// Grows the one chunk of changes made outside any transaction to hold `more` bytes more.
    fn grow(&mut self, more: usize) {
        let len = self.chunks.last().map_or(0, |c| c.len);
        let need = (FRONT + len + more + BACK).next_multiple_of(BLOCK as usize);
        let had = self.chunks.last().map_or(0, |c| c.block.len());
        let mut block = AlignedBlock::zeroed(need.max(2 * had));
        let capacity = block.len() - FRONT - BACK;
        match self.chunks.pop() {
            Some(c) => {
                block.bytes_mut()[FRONT..FRONT + c.len].copy_from_slice(c.bytes());
                self.chunks.push(Chunk {
                    block,
                    capacity,
                    ..c
                });
            }
            None => self.chunks.push(Chunk {
                block,
                capacity,
                len: 0,
                base: 0,
                first: NONE,
            }),
        }
    }

    /// Drops the changes of every statement after `statement`: a rollback to it, or of the whole
    /// transaction for 0. What was spilled past where they began stays in the log, and the next
    /// part, or the Commit's, begins there: replay cuts the parts before it at its start
    /// (DESIGN.md §11.4.1).
    pub fn back_to(&mut self, statement: Statement) {
        self.edits += 1;
        self.moved();
        let keep = self.marks.partition_point(|&(s, _)| s <= statement);
        let at = self.marks.get(keep).map(|&(_, at)| at);
        self.marks.truncate(keep);
        if let Some(at) = at {
            self.cut(at);
        }
    }

    /// Drops every change from `at` on.
    fn cut(&mut self, at: u64) {
        self.end = at;
        while let Some(c) = self.chunks.last_mut() {
            if c.base <= at {
                c.len = (at - c.base) as usize;
                if c.first != NONE && c.first >= c.len {
                    c.first = NONE;
                }
                return;
            }
            let c = self.chunks.pop().expect("a chunk");
            if self.spare.is_none() {
                self.spare = Some(c.block);
            }
        }
        // `at` lies in what was spilled: the parts are cut there, and the next chunk begins there.
        while let Some(p) = self.parts.last_mut() {
            if p.from >= at {
                self.parts.pop();
            } else {
                p.len = p.len.min(at - p.from);
                break;
            }
        }
        self.seal();
        self.chunks.last_mut().expect("a chunk").base = at;
    }

    pub fn is_empty(&self) -> bool {
        self.end == 0
    }

    /// Bytes of changes held, spilled and in memory.
    pub fn len(&self) -> u64 {
        self.end
    }

    /// How many changes and rollbacks there have been: equal counts mean the transaction changed
    /// nothing between.
    pub fn edits(&self) -> u64 {
        self.edits
    }

    /// The transaction's view moved on: a change, a rollback, a statement or a snapshot.
    pub fn moved(&mut self) {
        self.views += 1;
        self.view
            .store(self.views, std::sync::atomic::Ordering::Relaxed);
    }

    /// The count of the view's moves, and where it is read without the transaction's lock.
    pub fn view(&self) -> (u64, &Arc<std::sync::atomic::AtomicU64>) {
        (self.views, &self.view)
    }

    /// The bytes of chunk memory it holds: what it grows past only by taking another.
    pub fn capacity(&self) -> usize {
        self.chunks.iter().map(|c| c.block.len()).sum::<usize>()
            + self.spare.as_ref().map_or(0, AlignedBlock::len)
    }

    /// The bytes of memory it holds: its chunks', and its list of parts spilled, which grows by
    /// one part per chunk spilled.
    pub fn memory(&self) -> usize {
        self.capacity() + self.parts.capacity() * std::mem::size_of::<Part>()
    }

    /// Whether a sealed chunk waits to be spilled ([`sealed`](Self::sealed)).
    #[inline]
    pub fn has_sealed(&self) -> bool {
        self.chunks.len() > 1
    }

    /// The bytes of changes in the oldest sealed chunk, if one waits to be spilled.
    pub fn sealed_len(&self) -> Option<usize> {
        self.chunks
            .first()
            .filter(|_| self.has_sealed())
            .map(|c| c.len)
    }

    /// Takes the oldest sealed chunk, to publish in place: where it begins in the changes, its
    /// bytes of changes, where its first change starts in it, and its memory, the changes lying
    /// from [`FRONT`]. The caller records the part it becomes ([`spilled`](Self::spilled)).
    pub fn take_sealed(&mut self) -> (u64, usize, u64, AlignedBlock) {
        assert!(self.has_sealed(), "a spill with no chunk sealed");
        let c = self.chunks.remove(0);
        (c.base, c.len, c.first.min(c.len) as u64, c.block)
    }

    /// A chunk taken ([`take_sealed`](Self::take_sealed)) is in the log as `part`.
    pub fn spilled(&mut self, part: Part) {
        debug_assert!(self
            .chunks
            .first()
            .is_none_or(|c| c.base == part.from + part.len));
        self.parts.push(part);
    }

    /// The changes the Commit carries, those in the last chunk: where they begin, and their bytes.
    /// Every chunk before it has been spilled.
    pub fn tail(&self) -> (u64, &[u8]) {
        assert!(!self.has_sealed(), "a commit with a chunk not spilled");
        match self.chunks.last() {
            Some(c) => (c.base, c.bytes()),
            None => (self.end, &[]),
        }
    }

    /// The parts spilled, oldest first.
    pub fn parts(&self) -> &[Part] {
        &self.parts
    }

    /// Readies it for its slot's next transaction, keeping its memory: a chunk's, its lists'
    /// room, and its view's counter, which only rises, so a cursor of the transaction over never
    /// finds its count again.
    pub fn recycle(&mut self) {
        if self.spare.is_none() {
            self.spare = self.chunks.pop().map(|c| c.block);
        }
        self.spare = self.spare.take().filter(|b| b.len() == self.chunk_bytes);
        self.chunks.clear();
        self.parts.clear();
        self.marks.clear();
        self.end = 0;
        self.moved();
    }

    /// Where the changes of statements after `statement` begin; `None` if they made none.
    pub fn begin_after(&self, statement: Statement) -> Option<u64> {
        let first = self.marks.partition_point(|&(s, _)| s <= statement);
        self.marks.get(first).map(|&(_, at)| at)
    }

    /// The statement that made the change at `at`.
    pub fn statement_at(&self, at: u64) -> Statement {
        let i = self.marks.partition_point(|&(_, from)| from <= at);
        i.checked_sub(1).map_or(0, |i| self.marks[i].0)
    }

    /// Where the changes at or past `from` are read back from, newest first.
    pub fn sources_back(&self, from: u64) -> Vec<Source> {
        let mut out: Vec<Source> = (0..self.chunks.len())
            .rev()
            .filter(|&i| self.chunks[i].end() > from || self.chunks[i].base >= from)
            .map(Source::Memory)
            .collect();
        out.extend(
            self.parts
                .iter()
                .rev()
                .filter(|p| p.from + p.len > from)
                .map(|&p| Source::Spilled(p)),
        );
        out
    }

    /// The chunk in memory at `i`: where it begins, where its first change starts in it, and its
    /// bytes.
    pub fn chunk(&self, i: usize) -> (u64, u64, &[u8]) {
        let c = &self.chunks[i];
        (c.base, c.first.min(c.len) as u64, c.bytes())
    }

    /// The head and key of the change at `at`, in a chunk in memory.
    pub fn keyed_at(&self, at: u64) -> Result<Keyed<'_>> {
        let i = self.chunks.partition_point(|c| c.base <= at);
        let c = i
            .checked_sub(1)
            .map(|i| &self.chunks[i])
            .filter(|c| at < c.end())
            .ok_or_else(|| {
                LimboError::InternalError(format!("no change in memory starts at {at}"))
            })?;
        Keyed::decode(&c.bytes()[(at - c.base) as usize..])
    }

    /// Bytes of changes made outside any transaction, back to back: one chunk, never spilled.
    pub fn bytes(&self) -> &[u8] {
        assert!(
            self.parts.is_empty() && self.chunks.len() <= 1,
            "the bytes of changes in chunks"
        );
        self.chunks.last().map_or(&[], Chunk::bytes)
    }

    /// The changes made outside any transaction, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = Change<'_>> {
        Decode::new(self.bytes()).map(|c| c.expect("a transaction's own changes decode"))
    }
}

/// The positions of the changes that start in `bytes`, which begin at `base` in the changes and
/// hold the first change starting in them at `first`, at or past `from`, oldest first.
pub fn starts(bytes: &[u8], base: u64, first: u64, from: u64) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    let mut at = first;
    while at < bytes.len() as u64 {
        let len = Keyed::decode(&bytes[at as usize..])?.len;
        if base + at >= from {
            out.push(base + at);
        }
        at += len;
    }
    Ok(out)
}

/// The changes `bytes` holds back to back, oldest first, each `Corrupt` where the bytes do not
/// hold one together.
pub struct Decode<'a> {
    bytes: &'a [u8],
    failed: bool,
}

impl<'a> Decode<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            failed: false,
        }
    }
}

impl<'a> Iterator for Decode<'a> {
    type Item = Result<Change<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() || self.failed {
            return None;
        }
        let corrupt = |why: &str| LimboError::Corrupt(format!("a published change {why}"));
        let b = self.bytes;
        let Some(head) = b.get(..HEAD) else {
            self.failed = true;
            return Some(Err(corrupt("is cut short in its head")));
        };
        let kind = head[0];
        let key_len = u16::from_le_bytes([head[1], head[2]]) as usize;
        let value_len = u32::from_le_bytes(head[3..7].try_into().unwrap()) as usize;
        let tree = PageId::from_raw(u64::from_le_bytes(head[7..15].try_into().unwrap()));
        let Some(rest) = b.get(HEAD..HEAD + key_len + value_len) else {
            self.failed = true;
            return Some(Err(corrupt("is cut short in its key or value")));
        };
        let (key, value) = rest.split_at(key_len);
        let (plain, plainly) = (kind & PLAIN != 0, kind & PLAINLY != 0);
        let change = match kind & !(PLAIN | PLAINLY) {
            PUT if !(plain && plainly) => Change::Put {
                tree,
                plain,
                plainly,
                key,
                value,
            },
            DELETE if value.is_empty() && !(plain && plainly) => Change::Delete {
                tree,
                plain,
                plainly,
                key,
            },
            CREATE if !plain && !plainly && key.is_empty() && value.len() == 8 => Change::Create {
                root: tree,
                list: PageId::from_raw(u64::from_le_bytes(value.try_into().unwrap())),
            },
            DRAIN if !plain && !plainly && key.is_empty() && value.len() == 8 => Change::Drain {
                root: tree,
                list: PageId::from_raw(u64::from_le_bytes(value.try_into().unwrap())),
            },
            _ => {
                self.failed = true;
                return Some(Err(corrupt(&format!(
                    "of kind {kind} with a {key_len} byte key and a {value_len} byte value"
                ))));
            }
        };
        self.bytes = &b[HEAD + key_len + value_len..];
        Some(Ok(change))
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
