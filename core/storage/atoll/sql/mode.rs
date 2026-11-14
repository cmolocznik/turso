//! Atoll mode (DESIGN.md §17, R1): the one interface Turso's sites call, each site that branches
//! for MVCC, or that reaches the pager without a branch, gaining one arm that calls it.
//!
//! - **Opening.** [`probe`] says what the file at a database's path holds, by the control file's
//!   own rule ([`control::judge`]), and [`config_for`] what an Atoll database opens with. The main
//!   pager sits over [`RefusingStorage`], which refuses every read and write by name.
//! - **Transactions.** A connection's Atoll transaction is a [`ConnTxn`]. [`ConnTxn::begin`] begins
//!   it at a Transaction opcode, takes the write gate for a writing statement and starts its Atoll
//!   statement; [`Ending`] commits or rolls it back, and is never abandoned: one dropped unfinished
//!   is driven to its end where it drops.
//! - **The header.** Its fields are SQLite's header words as they are on disk ([`header`]), read at
//!   the transaction's view or, outside one, as committed, and written through [`ConnTxn::set_header`],
//!   which records each field it wrote by statement for the commit to publish.
//! - **Trees.** [`ConnTxn::create_btree`] is CreateBtree: a tree id, the tree, its catalog row.
//!   [`SqlCursor`] is a Turso cursor over one SQL tree, resolving its catalog row at its first step.
//!
//! Reads (the header, the catalog) wait only on IO, never on another transaction. Outside a
//! cooperative frame they are driven to their end here; inside one, a read whose caller keeps its
//! state ([`FacadeRead`]) returns the IO it waits for and goes on where it stopped when entered
//! again. Writes wait on other transactions, so they come back to the VDBE.

use super::catalog::{self, Catalog, Entry, Layout, System};
use super::cursor::{AtollCursor, OwedRollback, TxnCell};
use super::header::{Field, Header};
use super::key;
use super::lifecycle::Destroy;
use super::rowid::{Counter, Seed};
use super::wait::{graph, Edge, GroupId, Seat, WaitRecord};
use super::AtollDb;
use crate::io::FileSyncType;
use crate::io::{Completion, OpenFlags, IO};
use crate::storage::atoll::btree::{Mode, Near, Outcome};
use crate::storage::atoll::buffer::MIN_DB_BUDGET;
use crate::storage::atoll::control::{self, Found};
use crate::storage::atoll::db::AtollConfig;
use crate::storage::atoll::{drive, drive_for, drive_or_io};
// Plain data the facade passes in or out, defined where Atoll uses it and named through here.
pub(crate) use super::catalog::SCHEMA_ID;
pub(crate) use super::cursor::GuardKey;
use crate::storage::atoll::locks::Mutex;
use crate::storage::atoll::page::PageId;
pub(crate) use crate::storage::atoll::txn::versioned::Guarded;
use crate::storage::atoll::txn::versioned::{Get, Guard, Write, WriteOp};
pub(crate) use crate::storage::atoll::txn::Statement;
use crate::storage::atoll::txn::{Abort, Blocker, Commit, Hold, Isolation, Taken, Txn};
use crate::storage::btree::{BTreeKey, CursorTrait};
use crate::storage::database::DatabaseStorage;
use crate::storage::database::IOContext;
use crate::storage::pager::{Pager, SavepointResult};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::types::{
    IOCompletions, IOResult, IOResultOr, ImmutableRecord, IndexInfo, SeekKey, SeekOp, SeekResult,
};
use crate::vdbe::Register;
use crate::{Buffer, CompletionError, LimboError, Result, Value};
// Tree ids and pages, hashed for the maps a statement reads at each open: no attacker picks them.
use rustc_hash::FxHashMap as HashMap;
use std::io::ErrorKind;
use std::time::{Duration, Instant};

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// What the file at a database's path holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probed {
    /// Nothing is at the path.
    Absent,
    /// No database: an empty file, or an Atoll creation a crash cut short.
    Nothing,
    /// An Atoll database.
    Atoll,
    /// A file that is no Atoll control file: a SQLite database, say.
    Other,
}

/// What the file at `path` holds, read through a descriptor that takes no lock and is closed
/// before this returns. The caller probes only while no database of the path is open in the
/// process: closing a descriptor drops every lock the process holds on the file (R17).
#[turso_macros::atoll_entry]
pub fn probe(io: &Arc<dyn IO>, path: &str) -> Result<Probed> {
    let file = match io.open_file(path, OpenFlags::ReadOnly, false) {
        Ok(file) => file,
        Err(LimboError::CompletionError(CompletionError::IOError(ErrorKind::NotFound, _))) => {
            return Ok(Probed::Absent)
        }
        Err(e) => return Err(e),
    };
    Ok(match control::judge(io, &file) {
        // A SQLite database in WAL mode, or an MVCC one, can hold every row beside an empty main
        // file: one with a WAL or logical log that holds anything is no Atoll creation (R16).
        Ok(Found::Nothing)
            if holds_anything(io, &format!("{path}-wal"))?
                || crate::storage::journal_mode::logical_log_exists(path) =>
        {
            Probed::Other
        }
        Ok(Found::Nothing) => Probed::Nothing,
        Ok(Found::Database(_)) => Probed::Atoll,
        Err(_) => Probed::Other,
    })
}

/// Whether the file at `path` is there and holds a byte.
fn holds_anything(io: &Arc<dyn IO>, path: &str) -> Result<bool> {
    match io.open_file(path, OpenFlags::ReadOnly, false) {
        Ok(file) => Ok(file.size()? > 0),
        Err(LimboError::CompletionError(CompletionError::IOError(ErrorKind::NotFound, _))) => {
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// The configuration an Atoll database opens with: the caller's, or for one opened without the
/// option, Turso's default page cache, 2000 KiB (`cache_size` −2000), raised to the smallest
/// budget a database takes, the pool with its log's ring (R26).
#[turso_macros::atoll_entry]
pub fn config_for(option: Option<&AtollConfig>) -> AtollConfig {
    option
        .cloned()
        .unwrap_or_else(|| AtollConfig::with_budget(MIN_DB_BUDGET.max(2_048_000)))
}

fn refused() -> LimboError {
    LimboError::InternalError(
        "the main database is in Atoll mode: its pager holds no pages, and this site reached it \
         without its Atoll arm"
            .to_string(),
    )
}

/// The storage under the main database's pager in Atoll mode: every read and write refused by
/// name. Its size is one page, so the pager counts its first page as there and never allocates or
/// reads it (R20).
pub struct RefusingStorage;

impl DatabaseStorage for RefusingStorage {
    #[turso_macros::atoll_entry]
    fn read_header(&self, _c: Completion) -> Result<Completion> {
        Err(refused())
    }

    #[turso_macros::atoll_entry]
    fn read_page(
        &self,
        _page_idx: usize,
        _io_ctx: &IOContext,
        _c: Completion,
    ) -> Result<Completion> {
        Err(refused())
    }

    #[turso_macros::atoll_entry]
    fn write_page(
        &self,
        _page_idx: usize,
        _buffer: Arc<Buffer>,
        _io_ctx: &IOContext,
        _c: Completion,
    ) -> Result<Completion> {
        Err(refused())
    }

    #[turso_macros::atoll_entry]
    fn write_pages(
        &self,
        _first_page_idx: usize,
        _page_size: usize,
        _buffers: Vec<Arc<Buffer>>,
        _io_ctx: &IOContext,
        _c: Completion,
    ) -> Result<Completion> {
        Err(refused())
    }

    #[turso_macros::atoll_entry]
    fn sync(&self, _c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        Err(refused())
    }

    #[turso_macros::atoll_entry]
    fn size(&self) -> Result<u64> {
        Ok(PAGE as u64)
    }

    #[turso_macros::atoll_entry]
    fn truncate(&self, _len: usize, _c: Completion) -> Result<Completion> {
        Err(refused())
    }
}

/// The page size Turso sees: SQL trees' node size (R7).
pub const PAGE: usize = 4096;

/// What `PRAGMA max_page_count` reads in Atoll mode, which has no page cap (R35): SQLite's default,
/// the most its header's page count allows.
pub const NO_PAGE_CAP: u32 = 0xffff_fffe;

/// Each field's offset in SQLite's header, whose word it holds.
fn offset(field: Field) -> usize {
    match field {
        Field::SchemaCookie => 40,
        Field::SchemaFormat => 44,
        Field::DefaultCacheSize => 48,
        Field::TextEncoding => 56,
        Field::UserVersion => 60,
        Field::IncrementalVacuum => 64,
        Field::ApplicationId => 68,
    }
}

/// The offset of the largest root page, which turns auto-vacuum on: Atoll has none, its pages
/// freed as trees drain, so it reads 0 and a write of anything else is refused (R25).
const AUTO_VACUUM: usize = 52;

fn word(header: &DatabaseHeader, at: usize) -> u32 {
    let bytes = bytemuck::bytes_of(header);
    u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap())
}

/// The header Turso reads: SQLite's defaults, the page size Atoll's, and every field's word.
fn header_of(words: &[u32; 7]) -> DatabaseHeader {
    let mut header = DatabaseHeader::default();
    let bytes = bytemuck::bytes_of_mut(&mut header);
    for field in Field::ALL {
        let at = offset(field);
        bytes[at..at + 4].copy_from_slice(&words[field as usize - 1].to_be_bytes());
    }
    header
}

fn words_of(header: &DatabaseHeader) -> [u32; 7] {
    Field::ALL.map(|field| word(header, offset(field)))
}

/// How a connection's statements wait for another transaction (DESIGN.md §17, §3.1; R34 as Clay
/// re-ruled it on 2026-11-04: one model for every client). A wait publishes its edge in the
/// process's wait graph, which fails the youngest member of any cycle with `Deadlock`; the
/// statement then parks on the end it waits for, woken by that end, until its bound passes, when
/// it comes back `Busy`. The edge stays published across `Busy`, so callers that retry in place
/// still close a cycle, until the wait ends, its statement lets it go or its transaction ends.
pub(super) struct Waits {
    seat: Mutex<Option<Seat>>,
    /// The open transaction's group in the graph; 0 for none.
    group: AtomicU64,
    /// A wait's bound in nanoseconds: 0 comes back `Busy` at once, [`UNBOUNDED`] never does.
    bound: AtomicU64,
    /// Whether the last wait came back `Busy` having waited its bound, which the busy handler
    /// then does not wait again.
    spent: AtomicBool,
}

/// The bound of a wait that parks until its blocker ends, the PostgreSQL frontend's.
const UNBOUNDED: u64 = u64::MAX;

impl Waits {
    fn new() -> Self {
        Self {
            seat: Mutex::new(Some(graph().seat())),
            group: AtomicU64::new(0),
            bound: AtomicU64::new(0),
            spent: AtomicBool::new(false),
        }
    }

    /// The open transaction's group, begun if it has none: before its slot is taken, so that a
    /// wait on the transaction is a wait on the group.
    fn group(&self) -> GroupId {
        let group = self.group.load(Ordering::Acquire);
        if group != 0 {
            return group;
        }
        let seat = self.seat.lock();
        let group = graph().begin(seat.as_ref().expect("a seat until the connection closes"));
        self.group.store(group, Ordering::Release);
        group
    }

    /// Ends the transaction's group: its waits are over, and a statement of it still parked wakes.
    fn end_group(&self) {
        let group = self.group.swap(0, Ordering::AcqRel);
        if group != 0 {
            graph().end(group);
        }
    }
}

impl Drop for Waits {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.end_group();
        if let Some(seat) = self.seat.lock().take() {
            graph().unseat(seat);
        }
    }
}

/// What a wait's edge names, so that a wait on something else publishes a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum On {
    Row(Blocker),
    Take(u64),
    Drain(u64),
}

/// One wait of a statement (DESIGN.md §17, §3.1): the completion it parks on, registered on the
/// blocker's end before the write was judged again and reused while unfinished; its edge in the
/// wait graph; and when it began, which its bound counts from.
#[derive(Default)]
struct Waiting {
    on: Option<(Blocker, Completion)>,
    record: Option<(On, Arc<WaitRecord>)>,
    since: Option<Instant>,
    timed: Option<crate::timer::Timed>,
}

fn deadlock(_: super::wait::Deadlock) -> LimboError {
    LimboError::Deadlock
}

impl Waiting {
    /// What a write that came back `w` comes to: done, a park on its blocker's end, or judged
    /// again after registering on it. A wait publishes its edge first.
    fn judge(&mut self, db: &Arc<AtollDb>, waits: &Waits, w: Write) -> Result<Judged> {
        match w {
            Write::Done(_) => {
                self.on = None;
                self.over()?;
                Ok(Judged::Done)
            }
            Write::Conflict => {
                self.on = None;
                let _ = self.over();
                Err(LimboError::WriteWriteConflict)
            }
            Write::Wait(blocker) => {
                self.publish(waits, On::Row(blocker), || Edge::Row {
                    db: db.clone(),
                    blocker,
                })?;
                if let Some((on, c)) = &self.on {
                    if *on == blocker && !c.finished() {
                        return Ok(Judged::Park(c.clone()));
                    }
                }
                let c = match self.on.take() {
                    Some((_, c)) if !c.finished() => c,
                    _ => crate::storage::atoll::notify::remote_wait(),
                };
                db.atoll().transactions().wait_end(blocker, &c);
                self.on = Some((blocker, c));
                Ok(Judged::Again)
            }
        }
    }

    /// Publishes the edge of a wait on `on`, unless this wait published it: `Deadlock` if it
    /// closes a cycle whose victim is this transaction.
    fn publish(&mut self, waits: &Waits, on: On, edge: impl FnOnce() -> Edge) -> Result<()> {
        if self.record.as_ref().is_some_and(|(o, _)| *o == on) {
            return Ok(());
        }
        self.over()?;
        let record = graph().publish(waits.group(), edge()).map_err(deadlock)?;
        self.record = Some((on, record));
        Ok(())
    }

    /// Ends the wait, its edge withdrawn: `Deadlock` if it was chosen as a victim first.
    fn over(&mut self) -> Result<()> {
        self.timed = None;
        self.since = None;
        match self.record.take() {
            Some((_, record)) => graph().withdraw(&record).map_err(deadlock),
            None => Ok(()),
        }
    }

    /// Waits on `c`, which the blocker's end completes: parked until the bound passes, then
    /// `Busy` with the edge kept. `Deadlock` if the wait was chosen as a victim.
    fn wait<T>(&mut self, waits: &Waits, c: Completion) -> Result<IOResult<T>> {
        if let Some((_, record)) = &self.record {
            if graph().park(record, &c).is_err() {
                let _ = self.over();
                return Err(LimboError::Deadlock);
            }
        }
        let bound = waits.bound.load(Ordering::Acquire);
        if bound == UNBOUNDED {
            self.timed = None;
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        let now = Instant::now();
        let deadline = *self.since.get_or_insert(now) + Duration::from_nanos(bound);
        // The timer wakes it at the bound, unless the blocker's end does first.
        if now < deadline {
            if !self
                .timed
                .as_ref()
                .is_some_and(|timed| timed.is_for(deadline, &c))
            {
                self.timed = None;
                self.timed = crate::timer::complete_at(deadline, c.clone());
            }
            if self.timed.is_some() {
                return Ok(IOResult::IO(IOCompletions(c)));
            }
        }
        self.timed = None;
        self.since = None;
        waits.spent.store(bound != 0, Ordering::Release);
        Err(LimboError::Busy)
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        let _ = self.over();
    }
}

enum Judged {
    Done,
    Park(Completion),
    Again,
}

/// What a Transaction opcode asks of a connection's Atoll transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Begin {
    Read,
    /// A write; `schema` when the statement writes the schema, taking the gate exclusive.
    Write {
        schema: bool,
    },
}

/// A header field as a transaction read it: the word, the transaction's slot and start, and the
/// header's changes when it read it.
type FieldRead = (u32, (u16, u64), u64);

/// A facade read in flight (ASYNC1 D2e §2.2): its owner, the operation's top, and where it
/// stands, which are the locals [`drive_for`] keeps across its loop. Kept by whatever enters the
/// read again, from its first call to its end: a VDBE instruction's state, or a Transaction
/// opcode's for a begin's read set. So a read that returns IO in a cooperative frame goes on where
/// it stopped, with its shortage tenure and the page it keeps. Dropped unfinished, at its
/// statement's reset or drop, it lets both go. Its caller keeps it boxed, as an
/// `Option<Box<FacadeRead>>`: the box is made by a call in a frame that reads, before its first
/// step, even one that then finishes at once; a call outside a frame, or one answered without a
/// read, makes none. Only a return for IO keeps it.
pub(crate) struct FacadeRead {
    owner: crate::storage::atoll::buffer::Owner,
    /// The transaction it reads in, by slot and start; `None` outside one. A read kept from
    /// another transaction starts again.
    txn: Option<(u16, u64)>,
    at: ReadAt,
    /// Dropped last: the end of the frame its `Drop` opens (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

/// Where a facade read stands.
enum ReadAt {
    /// One header field, and its read under way with the header's changes when it started.
    Field {
        field: Field,
        get: Option<(Get, u64)>,
    },
    /// The header's fields in order: `next` the one under way, `words` those read.
    Header {
        next: usize,
        words: [u32; 7],
        get: Option<(Get, u64)>,
    },
    /// The page store's classes in order: `class` the one under way, `bytes` the sum so far.
    Pages { class: usize, bytes: u64 },
    /// A begin's read set: each tree's catalog row in order, `next` the one under way. The set
    /// is the caller's, named by its address and length: a statement's, fixed while the statement
    /// and so its kept read live.
    Since {
        set: (usize, usize),
        next: usize,
        get: Option<Get>,
    },
    /// A tree's catalog row (ASYNC1 D2e (ii)): `get`, its read under way, made at its first step;
    /// `changes`, the catalog's changes when the read started, which its row is tagged with.
    Row {
        id: u64,
        changes: u64,
        get: Option<Get>,
    },
}

impl ReadAt {
    fn field(field: Field) -> Self {
        Self::Field { field, get: None }
    }

    fn header() -> Self {
        Self::Header {
            next: 0,
            words: [0; 7],
            get: None,
        }
    }

    fn pages() -> Self {
        Self::Pages { class: 0, bytes: 0 }
    }

    fn since(reads: &[u64]) -> Self {
        Self::Since {
            set: (reads.as_ptr() as usize, reads.len()),
            next: 0,
            get: None,
        }
    }

    fn row(id: u64, changes: u64) -> Self {
        Self::Row {
            id,
            changes,
            get: None,
        }
    }

    /// Whether a read kept as `self` is the one `fresh` would start: the same kind, of the same
    /// field or read set; a catalog row's of the same tree while the catalog counts no change
    /// since it started.
    fn continues(&self, fresh: &ReadAt) -> bool {
        match (self, fresh) {
            (Self::Field { field: a, .. }, Self::Field { field: b, .. }) => a == b,
            (Self::Header { .. }, Self::Header { .. })
            | (Self::Pages { .. }, Self::Pages { .. }) => true,
            (Self::Since { set: a, .. }, Self::Since { set: b, .. }) => a == b,
            (
                Self::Row {
                    id: a, changes: c, ..
                },
                Self::Row {
                    id: b, changes: d, ..
                },
            ) => a == b && c == d,
            _ => false,
        }
    }
}

impl FacadeRead {
    /// The read kept in `read` if it is the one `fresh` would start, in transaction `txn`; else
    /// `fresh`, started with an owner of connection `conn`, in its place.
    fn kept<'r>(
        read: &'r mut Option<Box<FacadeRead>>,
        conn: &Arc<crate::storage::atoll::buffer::Conn>,
        txn: Option<(u16, u64)>,
        fresh: ReadAt,
    ) -> &'r mut FacadeRead {
        if !read
            .as_ref()
            .is_some_and(|r| r.txn == txn && r.at.continues(&fresh))
        {
            #[cfg(test)]
            if let ReadAt::Row { .. } = fresh {
                RESOLVER_BOXES.with(|n| n.set(n.get() + 1));
            }
            *read = Some(Box::new(FacadeRead {
                owner: crate::storage::atoll::buffer::Owner::of(conn.clone()),
                txn,
                at: fresh,
                _frame: crate::storage::atoll::unwind::FrameEnd::default(),
            }));
        }
        read.as_mut().expect("a read in flight")
    }

    /// The frame and the metadata page the read's owner keeps, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn keeps_for_test(
        &self,
    ) -> (Option<u32>, Option<crate::storage::atoll::meta::MetaId>) {
        (self.owner.kept_frame(), self.owner.kept_meta())
    }

    /// Whether the read's owner waits in the shortage queue or holds its gate, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn queued_for_test(&self) -> bool {
        self.owner.queued()
    }

    /// Lets the read go once it is done or failed: only a return for IO keeps it.
    fn settle<T>(
        read: &mut Option<Box<FacadeRead>>,
        out: Result<IOResult<T>>,
    ) -> Result<IOResult<T>> {
        if !matches!(out, Ok(IOResult::IO(_))) {
            *read = None;
        }
        out
    }
}

/// A connection's Atoll transaction: the `Txn` its cursors read and write in, and the header
/// fields it wrote, by statement.
pub struct ConnTxn {
    db: Arc<AtollDb>,
    pub(in crate::storage::atoll) cell: TxnCell,
    /// Each header field written: the statement that wrote it and its word.
    written: Mutex<Vec<(Statement, Field, u32)>>,
    /// A header write under way, kept across IO and waits.
    setting: Mutex<Option<(Field, WriteOp, Waiting)>>,
    /// A gate take waited on, kept so a take parks only on a completion registered before it.
    taking: Mutex<Option<Completion>>,
    /// The wait of a begin for a free slot or of a gate take, with its edge.
    begin_wait: Mutex<Waiting>,
    /// How its statements wait, and its transaction's group in the wait graph.
    waits: Arc<Waits>,
    /// The named savepoints, oldest first.
    savepoints: Mutex<Vec<Savepoint>>,
    /// Whether a schema reparse holds the transaction, which its statements read in and do not
    /// end (DESIGN.md §17, §6), and whether it began it.
    pinned: AtomicBool,
    pin_began: AtomicBool,
    /// What its statements changed in each tree, which a cursor positioned before reads again.
    changes: Arc<TreeChanges>,
    /// The stamps of the trees the open transaction wrote, raised to its commit timestamp once it
    /// is published (DESIGN.md §17, §3, defect 4).
    stamped: Arc<Mutex<HashMap<PageId, super::trees::Stamp>>>,
    /// What its cursors learned of each tree, for the next to open on it.
    known: Arc<Mutex<HashMap<u64, Known>>>,
    /// Each header field as a transaction read it: the word, the transaction's slot and start,
    /// and the header's changes when it read it. Its own while the header counts no change, as a
    /// catalog row is ([`Known`]).
    fields: Mutex<[Option<FieldRead>; 7]>,
    /// The connection's place for the ending of its transaction, once a statement has begun it
    /// (DESIGN.md §17): whichever statement of the connection meets it steps it, the one that
    /// began it to its end, a sibling still reading until its commit is published.
    ending: Mutex<Option<Ending>>,
    /// Whether the open transaction is a read begun for statements reading on beside an ending
    /// (`serve_sibling`, and the one an ending's end begins for its siblings), which no statement
    /// began for itself: the connection ends it once none can read in it.
    served: AtomicBool,
    /// Dropped last: the end of the frame its `Drop` opens (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

/// What a connection's cursors learned of one tree: its catalog row as a transaction read it, and
/// the leaf the last cursor over it stood at. The row holds for that transaction while the catalog
/// counts no change ([`Catalog::changes`]): its snapshot sees no other transaction's write, and
/// each of its own, and each rollback of its statements, is counted. The leaf is a hint, which a
/// cursor checks before it uses it, so it holds across transactions.
#[derive(Default)]
struct Known {
    row: Option<KnownRow>,
    near: Near,
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The trees whose catalog row a cursor opened on this thread read, for tests to see which
    /// opens the connection's cursors answered.
    pub(crate) static CATALOG_READS: std::cell::RefCell<Vec<u64>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// The header fields read on this thread, for the same.
    pub(crate) static HEADER_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The catalog-row `Get`s a cursor's resolution made on this thread ([`row_get`]).
    pub(crate) static ROW_GETS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The boxes a cursor's resolution was kept in, made on this thread ([`FacadeRead::kept`]).
    pub(crate) static RESOLVER_BOXES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Each step of a cursor's open on this thread: its tree, and whether the step returned for IO
    /// ([`SqlCursor::finish_open`]), for a test to see an open return at its own boundary.
    pub(crate) static OPEN_STEPS: std::cell::RefCell<Vec<(u64, bool)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// The commits told on this thread of a failure their driver saw (`Ending::met`), for a test
    /// to see that its schedule reached the driver.
    pub(crate) static COMMITS_MET: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The statement step entries on this thread that settled a statement rollback owed
    /// (ASYNC1 D2e (iii-b)).
    pub(crate) static SETTLES_AT_STEP: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The parked settle waits on this thread that came back failed and were dropped by their
    /// statement, the failure left to the owed rollback's own copy (ASYNC1 D2e (iii-b)).
    pub(crate) static SETTLE_WAITS_DROPPED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The calls of a commit's ending on this thread into the statement rollback it owed
    /// (`Ending::step_owed`), for a test to see none after its kept wait failed.
    pub(crate) static ENDING_OWED_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The completion the last of those calls returned for IO, before its top saw it, for a test
    /// to tell it from the one the top returned in its place.
    pub(crate) static ENDING_OWED_RAW: std::cell::RefCell<Option<crate::io::Completion>> =
        const { std::cell::RefCell::new(None) };
}

#[derive(Clone, Copy)]
struct KnownRow {
    entry: Entry,
    /// The transaction that read it: its slot and start.
    txn: (u16, u64),
    /// The catalog's changes when it read it.
    changes: u64,
}

impl KnownRow {
    /// Whether it holds for `txn` now.
    fn holds(&self, txn: &Txn, changes: u64) -> bool {
        self.txn == (txn.slot(), txn.start()) && self.changes == changes
    }
}

/// Tree `id`'s catalog row as `txn`'s current statement reads it, the one a cursor of the
/// connection read while it holds, and the leaf the last cursor over the tree stood at.
fn row_in(
    db: &Arc<AtollDb>,
    conn: &Arc<crate::storage::atoll::buffer::Conn>,
    known: &Mutex<HashMap<u64, Known>>,
    id: u64,
    txn: &mut Txn,
) -> Result<(KnownRow, Near)> {
    let changes = db.catalog().changes();
    let (held, near) = match known.lock().get(&id) {
        Some(k) => (k.row.filter(|r| r.holds(txn, changes)), k.near),
        None => (None, Near::NONE),
    };
    if let Some(row) = held {
        return Ok((row, near));
    }
    #[cfg(test)]
    CATALOG_READS.with(|r| r.borrow_mut().push(id));
    let mut get = Get::new(&catalog::key(id));
    let entry = drive_for(conn, db.io(), || {
        db.catalog()
            .get(db.atoll().transactions(), db.versions(), txn, &mut get)
    })?
    .ok_or_else(|| LimboError::InternalError(format!("no tree {id} in the catalog")))?;
    let row = KnownRow {
        entry,
        txn: (txn.slot(), txn.start()),
        changes,
    };
    known.lock().entry(id).or_default().row = Some(row);
    Ok((row, near))
}

/// [`row_in`] for a caller that keeps `read` across its returns (ASYNC1 D2e (ii)): in a
/// cooperative frame the catalog row's read returns the IO it waits for, and goes on where it
/// stopped when entered again, its look resuming at the page its owner keeps. Outside a frame the
/// row is read here as `row_in` reads it, a read kept from a frame driven to its end if it is still
/// this one, else let go. The caller lets `read` go at every end but a return for IO.
fn row_in_kept(
    db: &Arc<AtollDb>,
    conn: &Arc<crate::storage::atoll::buffer::Conn>,
    known: &Mutex<HashMap<u64, Known>>,
    id: u64,
    txn: &mut Txn,
    read: &mut Option<Box<FacadeRead>>,
) -> Result<IOResult<(KnownRow, Near)>> {
    let inside = crate::storage::atoll::cooperative::inside();
    if !inside && read.is_none() {
        return row_in(db, conn, known, id, txn).map(IOResult::Done);
    }
    let changes = db.catalog().changes();
    if let Some(k) = known.lock().get(&id) {
        if let Some(row) = k.row.filter(|r| r.holds(txn, changes)) {
            return Ok(IOResult::Done((row, k.near)));
        }
    }
    let identity = Some((txn.slot(), txn.start()));
    let fresh = ReadAt::row(id, changes);
    let continues = read
        .as_ref()
        .is_some_and(|r| r.txn == identity && r.at.continues(&fresh));
    if !continues && !inside {
        // Kept from a frame for another identity: let go, its owner's keeps with it.
        *read = None;
        return row_in(db, conn, known, id, txn).map(IOResult::Done);
    }
    #[cfg(test)]
    if !continues {
        CATALOG_READS.with(|r| r.borrow_mut().push(id));
    }
    let FacadeRead { owner, at, .. } = FacadeRead::kept(read, conn, identity, fresh);
    let ReadAt::Row {
        changes: started,
        get,
        ..
    } = at
    else {
        unreachable!("a catalog row's read over another read");
    };
    let started = *started;
    let entry = done!(drive_or_io(owner, db.io(), || {
        let get = get.get_or_insert_with(|| row_get(id));
        get.resume_at_kept();
        db.catalog()
            .get(db.atoll().transactions(), db.versions(), txn, get)
    }))
    .ok_or_else(|| LimboError::InternalError(format!("no tree {id} in the catalog")))?;
    // Tagged with the changes the catalog counted when the read started, never later ones.
    let row = KnownRow {
        entry,
        txn: (txn.slot(), txn.start()),
        changes: started,
    };
    let mut known = known.lock();
    let k = known.entry(id).or_default();
    k.row = Some(row);
    Ok(IOResult::Done((row, k.near)))
}

/// The `Get` of a tree's catalog row for a cursor's resolution: the one place it is made.
fn row_get(id: u64) -> Get {
    #[cfg(test)]
    ROW_GETS.with(|n| n.set(n.get() + 1));
    Get::new(&catalog::key(id))
}

/// Whether `txn`'s current statement created the tree `entry` names.
fn created_now(txn: &Txn, entry: Entry) -> bool {
    txn.created()
        .iter()
        .any(|c| c.root == entry.root && c.statement == txn.command())
}

/// Slots the trees a connection changes fall into: a cursor whose tree shares a slot with
/// another the connection changed reads its entry again for nothing, which costs a seek and never
/// a wrong read; 256 keep any two trees apart but for one in 256, at 4 KiB a connection.
const CHANGE_SLOTS: usize = 256;

/// What a connection's statements changed in its trees (DESIGN.md §17, §3). They share the trees
/// they write, as SQLite's statements share the b-tree: a cursor positioned before another
/// statement's change reads its entry again at its next read or move, and one whose tree a Clear
/// replaced is bound to the new tree. Each change takes the next epoch; a slot keeps, per tree
/// falling into it, the epoch of its last row change and of its last Clear. A Destroy changes
/// nothing a cursor reads: it stays on the tree it opened (R22).
pub(in crate::storage::atoll) struct TreeChanges {
    epoch: AtomicU64,
    /// The epoch of the last rollback of statements, which may have changed any tree.
    rolled_back: AtomicU64,
    slots: Box<[[AtomicU64; 2]]>,
    /// The trees a Destroy took in the open transaction, with the statement that ran it: a cursor
    /// that statement opens on one after it reads it as empty, as SQLite's does a freed root.
    /// Changed only through [`Self::destroyed`], which keeps `destroying`, its length, beside it.
    destroyed_list: Mutex<Vec<(u64, Statement)>>,
    /// The length of `destroyed_list`, written under its lock: a cursor's open reads none listed
    /// with one load, as nearly every statement destroys nothing.
    destroying: AtomicUsize,
}

/// What a cursor positioned at an epoch has to catch up with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Behind {
    Nothing,
    Rows,
    Cleared,
}

impl TreeChanges {
    fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            rolled_back: AtomicU64::new(0),
            slots: (0..CHANGE_SLOTS)
                .map(|_| [AtomicU64::new(0), AtomicU64::new(0)])
                .collect(),
            destroyed_list: Mutex::new(Vec::new()),
            destroying: AtomicUsize::new(0),
        }
    }

    /// Whether statement `statement` destroyed tree `id`.
    fn destroyed_in(&self, id: u64, statement: Statement) -> bool {
        self.destroying.load(Ordering::Acquire) != 0
            && self.destroyed_list.lock().contains(&(id, statement))
    }

    /// Changes the trees destroyed with `f`, their count kept with them.
    fn destroyed(&self, f: impl FnOnce(&mut Vec<(u64, Statement)>)) {
        let mut list = self.destroyed_list.lock();
        f(&mut list);
        self.destroying.store(list.len(), Ordering::Release);
    }

    fn next(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn slot(&self, id: u64) -> &[AtomicU64; 2] {
        &self.slots[(id % CHANGE_SLOTS as u64) as usize]
    }

    /// A row of tree `id` written or removed.
    fn wrote(&self, id: u64) {
        let e = self.next();
        self.slot(id)[0].store(e, Ordering::Release);
    }

    /// Tree `id`'s catalog row pointed at a new, empty tree.
    fn cleared(&self, id: u64) {
        let e = self.next();
        self.slot(id)[1].store(e, Ordering::Release);
    }

    /// Statements rolled back, whichever trees they wrote.
    fn rolled_back(&self) {
        let e = self.next();
        self.rolled_back.store(e, Ordering::Release);
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// What changed in tree `id` after `seen`.
    fn since(&self, id: u64, seen: u64) -> Behind {
        let [rows, clears] = self.slot(id);
        if clears.load(Ordering::Acquire) > seen {
            Behind::Cleared
        } else if rows.load(Ordering::Acquire) > seen
            || self.rolled_back.load(Ordering::Acquire) > seen
        {
            Behind::Rows
        } else {
            Behind::Nothing
        }
    }
}

/// The Atoll tree id Turso holds as a root page (DESIGN.md §17): ids are positive, from 1,
/// `sqlite_schema`.
#[turso_macros::atoll_entry]
pub fn tree_id(root: i64) -> Result<u64> {
    u64::try_from(root)
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| LimboError::InternalError(format!("an Atoll tree id of {root}")))
}

/// A named savepoint (DESIGN.md §17, §3): the statement it rolls back to, whether it began the
/// transaction, and the deferred foreign-key count a rollback to it restores.
struct Savepoint {
    name: String,
    at: Statement,
    starts_transaction: bool,
    deferred_fk: isize,
}

impl ConnTxn {
    /// The page arm of `PRAGMA integrity_check`, in the transaction's current statement, which
    /// holds the write gate exclusive (DESIGN.md §17, §12, R36): what it found wrong, at most
    /// `max` of it.
    #[turso_macros::atoll_entry]
    pub fn check_pages(&self, max: usize) -> Result<Vec<String>> {
        let mut guard = self.cell.enter_now("check_pages")?;
        let txn = guard.as_mut().ok_or_else(outside)?;
        self.db.check_pages(txn, max)
    }

    #[turso_macros::atoll_entry]
    pub fn new(db: Arc<AtollDb>) -> Self {
        Self {
            db,
            cell: super::cursor::TxnSlot::new(None),
            written: Mutex::new(Vec::new()),
            setting: Mutex::new(None),
            taking: Mutex::new(None),
            begin_wait: Mutex::new(Waiting::default()),
            waits: Arc::new(Waits::new()),
            savepoints: Mutex::new(Vec::new()),
            pinned: AtomicBool::new(false),
            pin_began: AtomicBool::new(false),
            changes: Arc::new(TreeChanges::new()),
            stamped: Arc::new(Mutex::new(HashMap::default())),
            known: Arc::new(Mutex::new(HashMap::default())),
            fields: Mutex::new([None; 7]),
            ending: Mutex::new(None),
            served: AtomicBool::new(false),
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    /// Holds a read transaction for a schema reparse, beginning one if none is open: the cookie
    /// and `sqlite_schema` are read in one view, and so is every row the reparse reads after them,
    /// its nested statements' among them, which begin no transaction of their own (DESIGN.md §17,
    /// §6). The statements it runs read in it and do not end it; [`unpin`](Self::unpin) does.
    #[turso_macros::atoll_entry]
    pub fn pin(&self) -> Result<()> {
        if self.pinned.swap(true, Ordering::AcqRel) {
            return Err(LimboError::InternalError(
                "a schema reparse inside another on one connection".to_string(),
            ));
        }
        // Its begin runs inside `drive_for`'s top, where a statement rollback owed is refused:
        // finished first, here (ASYNC1 D2e (iii-b)).
        if let Err(e) = self.cell.settle_now("pin") {
            self.pinned.store(false, Ordering::Release);
            return Err(e);
        }
        self.pin_began.store(!self.is_open(), Ordering::Release);
        let io = self.db.io().clone();
        drive_for(self.cell.conn(), &io, || self.begin(Begin::Read)).inspect_err(|_| {
            self.pinned.store(false, Ordering::Release);
        })
    }

    /// Lets go of the reparse's transaction: ended here if the reparse began it; one the caller
    /// had open before goes on.
    #[turso_macros::atoll_entry]
    pub fn unpin(&self) -> Result<()> {
        if !self.pinned.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        if !self.pin_began.load(Ordering::Acquire) {
            return Ok(());
        }
        match self.end(true) {
            Some(mut ending) => ending.finish(),
            None => Ok(()),
        }
    }

    /// SAVEPOINT: records where the transaction stands, at no statement when none has begun, as a
    /// deferred transaction's first read begins it.
    #[turso_macros::atoll_entry]
    pub fn open_savepoint(&self, name: String, starts_transaction: bool, deferred_fk: isize) {
        let at = self.cell.observe().as_ref().map_or(0, Txn::command);
        self.savepoints.lock().push(Savepoint {
            name,
            at,
            starts_transaction,
            deferred_fk,
        });
    }

    /// RELEASE: forgets the newest savepoint of `name` and every one after it; releasing the
    /// outermost, when it began the transaction, commits it.
    #[turso_macros::atoll_entry]
    pub fn release_savepoint(&self, name: &str) -> SavepointResult {
        let mut savepoints = self.savepoints.lock();
        let Some(i) = savepoints.iter().rposition(|s| s.name == name) else {
            return SavepointResult::NotFound;
        };
        let commit = i == 0 && savepoints[i].starts_transaction;
        savepoints.truncate(i);
        if commit {
            SavepointResult::Commit
        } else {
            SavepointResult::Release
        }
    }

    /// ROLLBACK TO: rolls the transaction back to where the newest savepoint of `name` stood,
    /// which stays, and forgets every one after it. Returns the deferred foreign-key count to
    /// restore, `None` with no such savepoint.
    #[turso_macros::atoll_entry]
    pub fn rollback_to_savepoint(&self, name: &str) -> Result<Option<isize>> {
        let (at, deferred_fk) = {
            let mut savepoints = self.savepoints.lock();
            let Some(i) = savepoints.iter().rposition(|s| s.name == name) else {
                return Ok(None);
            };
            savepoints.truncate(i + 1);
            (savepoints[i].at, savepoints[i].deferred_fk)
        };
        self.rollback_statement(at)?;
        Ok(Some(deferred_fk))
    }

    /// From now on waits park until what is in the way ends, for the PostgreSQL frontend, whose
    /// waits are unbounded (R2, R34).
    #[turso_macros::atoll_entry]
    pub fn park_waits(&self) {
        self.waits.bound.store(UNBOUNDED, Ordering::Release);
    }

    /// Bounds each wait by the client's busy timeout: it parks until then, and then comes back
    /// `Busy`; with none it comes back `Busy` at once, its edge still in the wait graph (R34).
    #[turso_macros::atoll_entry]
    pub fn bound_waits(&self, timeout: Duration) {
        let nanos = u64::try_from(timeout.as_nanos()).unwrap_or(UNBOUNDED - 1);
        self.waits
            .bound
            .store(nanos.min(UNBOUNDED - 1), Ordering::Release);
    }

    /// Whether the last wait came back `Busy` having waited its bound: the busy handler then
    /// does not wait again. Taken: a second call reads false.
    #[turso_macros::atoll_entry]
    pub fn take_wait_spent(&self) -> bool {
        // Nearly every statement's wait was not spent: one load, and the swap only when it was.
        self.waits.spent.load(Ordering::Acquire) && self.waits.spent.swap(false, Ordering::AcqRel)
    }

    #[turso_macros::atoll_entry]
    pub fn db(&self) -> &Arc<AtollDb> {
        &self.db
    }

    /// Whether a transaction is open.
    #[turso_macros::atoll_entry]
    pub fn is_open(&self) -> bool {
        self.cell.observe().is_some()
    }

    /// Whether a schema reparse holds the transaction ([`pin`](Self::pin)).
    #[turso_macros::atoll_entry]
    pub fn is_pinned(&self) -> bool {
        self.pinned.load(Ordering::Acquire)
    }

    /// Whether the open transaction has written.
    #[turso_macros::atoll_entry]
    pub fn wrote(&self) -> bool {
        self.cell.observe().as_ref().is_some_and(Txn::wrote)
    }

    /// A cursor over the tree whose id Turso holds as the root page `root`, as OpenRead and
    /// OpenWrite open one: an index's or a WITHOUT ROWID table's with `index`, a rowid table's
    /// without. Its open is under way: the caller steps it with
    /// [`finish_open`](CursorTrait::finish_open) until done, as the instruction does.
    #[turso_macros::atoll_entry]
    pub fn open_cursor(
        &self,
        pager: Arc<Pager>,
        root: i64,
        index: Option<Arc<IndexInfo>>,
    ) -> Result<SqlCursor> {
        Ok(SqlCursor::new(self, pager, tree_id(root)?, index).opening_fresh())
    }

    /// [`open_cursor`](Self::open_cursor) for an index method's backing tree, whose writes fail
    /// at once on another writer's key instead of waiting for it ([`SqlCursor::no_wait`]). Outside
    /// a cooperative frame its open is finished here, so it is bound when made; in one it is left
    /// under way, for the index method's own open to finish before it is done (ASYNC1 D2e (ii)).
    #[turso_macros::atoll_entry]
    pub fn open_cursor_no_wait(
        &self,
        pager: Arc<Pager>,
        root: i64,
        index: Option<Arc<IndexInfo>>,
    ) -> Result<SqlCursor> {
        let mut cursor = SqlCursor::new(self, pager, tree_id(root)?, index)
            .no_wait()
            .opening_fresh();
        if !crate::storage::atoll::cooperative::inside() {
            // Outside a frame the catalog read drives in place: the open ends in this call.
            let opened = cursor.finish_open().map_err(|e| *e)?;
            crate::turso_assert!(
                matches!(opened, IOResult::Done(())),
                "an open outside a cooperative frame returned for IO"
            );
        }
        Ok(cursor)
    }

    /// The snapshot timestamp of the open transaction's current statement, once it has one.
    #[turso_macros::atoll_entry]
    pub fn snapshot(&self) -> Option<u64> {
        // Observed: every transaction here is begun at snapshot isolation, whose snapshot a
        // statement rollback never moves (ASYNC1 D2e (iii-b)).
        self.cell
            .observe()
            .as_ref()
            .filter(|txn| txn.snapped())
            .map(Txn::snapshot)
    }

    /// Begins the transaction a Transaction opcode needs, if none is open, and for a write takes
    /// the write gate: shared, or exclusive for a statement writing the schema (DESIGN.md §17,
    /// §6). Behind another's take it comes back with the completion to wait on; stepping it again
    /// goes on. A take the gate lost to a victim's clearing is `Busy`.
    #[turso_macros::atoll_entry]
    pub fn begin(&self, wants: Begin) -> Result<IOResult<()>> {
        Ok(self.begin_reading(wants, &[], &mut None)?.map(|_| ()))
    }

    /// [`begin`](Self::begin) for a schema writer that reads rows of the trees `reads` names: in a
    /// transaction that already has a snapshot, a commit that raised one's stamp past it fails the
    /// take with `BusySnapshot`, checked before it marks itself pending and again once it holds
    /// the gate (DESIGN.md §17, §3, defect 4). Its build or rewrite would read the tree at that
    /// snapshot and miss the rows committed since, as no write conflict tells. Done, it says
    /// whether this call found no transaction open, so began it: the Transaction opcode's one
    /// look at the connection's transaction. The check's read is kept in `read`, the opcode's,
    /// across the begin's returns.
    #[turso_macros::atoll_entry]
    pub fn begin_reading(
        &self,
        wants: Begin,
        reads: &[u64],
        read: &mut Option<Box<FacadeRead>>,
    ) -> Result<IOResult<bool>> {
        let txns = self.db.atoll().transactions();
        if let Begin::Write { .. } = wants {
            // A writing statement steps the checkpointer first, holding no latch, while a
            // checkpoint is due or under way: commits are not its only trigger, so long
            // transactions are checkpointed too (DESIGN.md §17, §11). Its IO is not waited for:
            // the checkpoint goes on at the next step, whoever takes it.
            self.db.atoll().checkpointer().step_due(txns).map(|_| ())?;
        }
        let mut guard = self.cell.enter()?;
        let began = guard.is_none();
        if began {
            let begun = match wants {
                Begin::Read => txns.begin(Isolation::Snapshot)?,
                // Its snapshot once it holds the gate: an exclusive taker's after the drain.
                Begin::Write { .. } => txns.begin_slot_in(Isolation::Snapshot, 0)?,
            };
            match begun {
                IOResult::Done(txn) => {
                    let slot = txn.slot();
                    *guard = Some(txn);
                    self.begin_wait.lock().over()?;
                    self.db.collect_after_begin(slot)?;
                }
                // No slot is free (R13): no one transaction is in the way, so no edge.
                IOResult::IO(IOCompletions(c)) => {
                    return self.begin_wait.lock().wait(&self.waits, c);
                }
            }
        }
        let txn = guard.as_mut().expect("a transaction");
        // A row writer already holding the gate, as every statement after its transaction's
        // first does, has nothing to take.
        let held = matches!(wants, Begin::Write { schema: false })
            && matches!(txn.hold(), Hold::Shared | Hold::Exclusive { .. });
        if held {
            txns.join(txn, self.waits.group());
        } else if let Begin::Write { schema } = wants {
            // A writer is waited on by its group from before its take: a reader never is.
            txns.join(txn, self.waits.group());
            let checks = schema && !reads.is_empty() && txn.snapped();
            if checks
                && !matches!(txn.hold(), Hold::Pending { .. } | Hold::Exclusive { .. })
                && done!(self.read_since_snapshot(txn, reads, read))
            {
                return Err(LimboError::BusySnapshot);
            }
            let mut taking = self.taking.lock();
            let c = match taking.take() {
                Some(c) if !c.finished() => c,
                _ => crate::storage::atoll::notify::remote_wait(),
            };
            let taken = match if schema {
                txns.take_exclusive(txn, &c)
            } else {
                txns.take_shared(txn, &c)
            } {
                Ok(taken) => taken,
                Err(e) => {
                    txns.give_up(txn);
                    drop(taking);
                    self.end_if_fresh(&mut guard);
                    return Err(e);
                }
            };
            match taken {
                Taken::Done => {
                    self.begin_wait.lock().over()?;
                    if checks && done!(self.read_since_snapshot(txn, reads, read)) {
                        txns.hand_back_exclusive(txn);
                        return Err(LimboError::BusySnapshot);
                    }
                }
                Taken::Lost => {
                    // Cleared for a victim: this one, if its wait was flagged.
                    let flagged = self.begin_wait.lock().over().is_err();
                    drop(taking);
                    self.end_if_fresh(&mut guard);
                    return Err(if flagged {
                        LimboError::Deadlock
                    } else {
                        LimboError::Busy
                    });
                }
                Taken::Behind { take, .. } => {
                    // A pending exclusive take stays pending meanwhile (R15).
                    *taking = Some(c.clone());
                    let db: Arc<dyn super::wait::Holders> = self.db.clone();
                    let mut wait = self.begin_wait.lock();
                    wait.publish(&self.waits, On::Take(take), || Edge::Take { db, take })?;
                    return wait.wait(&self.waits, c);
                }
                Taken::Holders(holders) => {
                    *taking = Some(c.clone());
                    let take = txn.pending_take().ok_or_else(|| {
                        LimboError::InternalError(
                            "a drain of shared holders with no take pending".to_string(),
                        )
                    })?;
                    let db: Arc<dyn super::wait::Holders> = self.db.clone();
                    let holders = holders
                        .into_iter()
                        .map(|(slot, start)| Blocker { slot, start })
                        .collect();
                    let mut wait = self.begin_wait.lock();
                    wait.publish(&self.waits, On::Drain(take), || Edge::Drain {
                        db,
                        holders,
                        take,
                    })?;
                    return wait.wait(&self.waits, c);
                }
            }
        }
        let txn = guard.as_mut().expect("a transaction");
        if !txn.snapped() {
            txns.take_snapshot(txn);
        }
        Ok(IOResult::Done(began))
    }

    /// Whether a commit raised the stamp of a tree `reads` names past `txn`'s snapshot: the tree
    /// as the catalog names it at the transaction's view, one created since unnamed and skipped.
    /// Kept in `read` across its returns: in a cooperative frame, the IO it waits for, and it goes
    /// on where it stopped when the begin comes back (ASYNC1 D2e §2.2).
    fn read_since_snapshot(
        &self,
        txn: &mut Txn,
        reads: &[u64],
        read: &mut Option<Box<FacadeRead>>,
    ) -> Result<IOResult<bool>> {
        let fresh = ReadAt::since(reads);
        // Outside a frame the read ends in this call: its state stays a local.
        if read.is_none() && !crate::storage::atoll::cooperative::inside() {
            let mut at = fresh;
            return drive_for(self.cell.conn(), self.db.io(), || {
                self.since_step(txn, reads, &mut at)
            })
            .map(IOResult::Done);
        }
        let identity = Some((txn.slot(), txn.start()));
        let FacadeRead { owner, at, .. } =
            FacadeRead::kept(read, self.cell.conn(), identity, fresh);
        let out = drive_or_io(owner, self.db.io(), || self.since_step(txn, reads, at));
        FacadeRead::settle(read, out)
    }

    /// One call's worth of a begin's read set: each tree's catalog row in order, until one was
    /// written since the snapshot.
    fn since_step(&self, txn: &mut Txn, reads: &[u64], at: &mut ReadAt) -> Result<IOResult<bool>> {
        let ReadAt::Since { next, get, .. } = at else {
            unreachable!("a read set's step over another read");
        };
        let db = &self.db;
        while let Some(&id) = reads.get(*next) {
            let op = get.get_or_insert_with(|| Get::new(&catalog::key(id)));
            op.resume_at_kept();
            let entry = done!(db
                .catalog()
                .get(db.atoll().transactions(), db.versions(), txn, op));
            *get = None;
            *next += 1;
            if entry.is_some_and(|e| db.stamps().written_since(e.root, txn.snapshot())) {
                return Ok(IOResult::Done(true));
            }
        }
        Ok(IOResult::Done(false))
    }

    /// Ends a transaction a failed begin left with nothing done: no snapshot, or no write.
    fn end_if_fresh(&self, guard: &mut Option<Txn>) {
        if guard
            .as_ref()
            .is_some_and(|t| !t.wrote() && t.command() == 0)
        {
            let txn = guard.take().expect("a transaction");
            self.db.atoll().transactions().abort(txn);
            let _ = self.begin_wait.lock().over();
            self.waits.end_group();
        }
    }

    /// Gives up a pending exclusive take, as a statement that stops waiting for it does (R15).
    #[turso_macros::atoll_entry]
    pub fn give_up(&self) {
        if let Some(txn) = self.cell.observe().as_mut() {
            self.db.atoll().transactions().give_up(txn);
        }
        *self.taking.lock() = None;
        let _ = self.begin_wait.lock().over();
    }

    /// Starts the Atoll statement of a statement that writes: returns the number to roll back
    /// to if it fails (R18).
    #[turso_macros::atoll_entry]
    pub fn start_statement(&self) -> Result<Statement> {
        let mut guard = self.cell.enter_now("start_statement")?;
        let txn = guard.as_mut().ok_or_else(outside)?;
        let n = txn.command();
        self.db.atoll().transactions().statement(txn)?;
        Ok(n)
    }

    /// Rolls the open transaction back to where it stood after statement `n` (R19): recorded in
    /// the transaction as the rollback it owes, which the top of the connection's next call
    /// finishes before anything else ([`TxnSlot::settle`]); outside a cooperative frame finished
    /// here, in place, as a blocking host does (ASYNC1 D2e (iii-b)). What the connection kept of
    /// the statements rolled back (the header fields they wrote, a header write under way, the
    /// trees they destroyed) is forgotten now, and every cache of what they changed is told, once,
    /// when the rollback is first owed: nothing reads the transaction until it is finished.
    #[turso_macros::atoll_entry]
    pub fn rollback_statement(&self, n: Statement) -> Result<()> {
        {
            let mut slot = self.cell.observe();
            let Some(owing) = slot.owe(n, &self.db, self.cell.conn()) else {
                return Ok(());
            };
            if owing != super::cursor::Owing::Failed {
                self.written.lock().retain(|(s, _, _)| *s <= n);
                *self.setting.lock() = None;
                self.changes.destroyed(|list| list.retain(|(_, s)| *s <= n));
            }
            if owing == super::cursor::Owing::Fresh {
                self.changes.rolled_back();
                // It may undo a catalog or header write the transaction read.
                self.db.catalog().note_change();
                self.db.header().note_change();
            }
        }
        if crate::storage::atoll::cooperative::inside() {
            return Ok(());
        }
        match self.cell.settle()? {
            IOResult::Done(()) => Ok(()),
            IOResult::IO(_) => unreachable!("a settle outside a cooperative frame returned IO"),
        }
    }

    /// Finishes the statement rollback the transaction owes, at the top of a call of the
    /// connection ([`TxnSlot::settle`]): a statement step's entry, before its first instruction.
    #[turso_macros::atoll_entry]
    pub fn settle(&self) -> Result<IOResult<()>> {
        self.cell.settle()
    }

    /// Whether the transaction owes a statement rollback: one load, for a statement step's entry.
    #[inline]
    #[turso_macros::atoll_entry]
    pub fn owes(&self) -> bool {
        self.cell.owes()
    }

    /// The open transaction's slot and start, if the statement rollback it owes failed: what a
    /// statement that met the failure marks, so its abort rolls that transaction back whole.
    #[turso_macros::atoll_entry]
    pub fn owed_failure(&self) -> Option<(u16, u64)> {
        self.cell.observe().failed_in()
    }

    /// The ending of the open transaction: its commit, or its rollback. `None` with none open, or
    /// while a schema reparse holds it.
    #[turso_macros::atoll_entry]
    pub fn end(&self, commit: bool) -> Option<Ending> {
        self.end_publishing(commit, None)
    }

    /// [`end`](Self::end), with `publish` run as the commit is published, after its header fields
    /// and its trees' stamps and before the write gate is let go, so whoever takes the gate next
    /// sees what it publishes: the SQL schema a commit changed (DESIGN.md §17). Dropped unrun by a
    /// rollback, or by a commit that fails before it is published.
    #[turso_macros::atoll_entry]
    pub fn end_publishing(
        &self,
        commit: bool,
        publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Option<Ending> {
        if self.pinned.load(Ordering::Acquire) {
            return None;
        }
        self.served.store(false, Ordering::Release);
        let (txn, written, stamped, owed) = self.take_to_end()?;
        let conn = self.cell.conn().clone();
        Some(if commit {
            Ending::commit(self.db.clone(), conn, txn, written, stamped, publish, owed)
        } else {
            // The whole rollback undoes all a statement rollback owed would have, from wherever
            // it stopped: undo is idempotent.
            drop(owed);
            self.changes.rolled_back();
            Ending::rollback(self.db.clone(), conn, txn)
        })
    }

    /// The open transaction, taken out of the connection to be ended, with what it wrote to the
    /// header and the trees it stamped; the connection's part of it let go.
    fn take_to_end(&self) -> Option<EndedTxn> {
        self.savepoints.lock().clear();
        let (txn, owed) = self.cell.take()?;
        self.changes.destroyed(Vec::clear);
        let written = std::mem::take(&mut *self.written.lock());
        let stamped: Vec<_> = std::mem::take(&mut *self.stamped.lock())
            .into_values()
            .collect();
        *self.setting.lock() = None;
        *self.taking.lock() = None;
        let _ = self.begin_wait.lock().over();
        self.waits.end_group();
        // No sibling cursor keeps the gate, `R` or a protected set past the transaction it served
        // (DESIGN.md 9.1.3, *Revoking an inactive tenure*).
        crate::storage::atoll::buffer::cancel_shortage(self.db.atoll().pool(), self.cell.conn());
        Some((txn, written, stamped, owed))
    }

    /// Ends the open read begun for siblings (`served`) by letting its slot and snapshot go: it
    /// wrote and abandoned nothing, so it owes no rollback and has no ending of its own, the drain
    /// and the checkpoint's pacing a transaction's end does left to the endings that write, the one
    /// in the connection's place among them (the auditor's #4459: an ending of its own beside that
    /// one would be a second ending of the connection, with a drain and a full checkpoint's wait to
    /// owe). Whether it ended one: not while a schema reparse holds the transaction, nor one a
    /// later statement wrote in, which that statement ends.
    #[turso_macros::atoll_entry]
    pub fn end_served(&self) -> bool {
        if self.pinned.load(Ordering::Acquire) || !self.served.swap(false, Ordering::AcqRel) {
            return false;
        }
        if !self.cell.observe().as_ref().is_some_and(|txn| !txn.wrote()) {
            return false;
        }
        // Only with no changes, so a statement rollback owed has nothing to undo.
        let Some((txn, _, _, _owed)) = self.take_to_end() else {
            return false;
        };
        self.changes.rolled_back();
        self.db.atoll().transactions().abort(txn);
        true
    }

    /// Puts `ending` in the connection's place, where whichever statement of the connection meets
    /// it steps it (DESIGN.md §17). One at a time: the end word lets one statement end the
    /// connection's transaction (`Connection::atoll_end`).
    #[turso_macros::atoll_entry]
    pub fn place(&self, ending: Ending) {
        let mut place = self.ending.lock();
        crate::turso_assert!(place.is_none(), "two endings in one connection's place");
        *place = Some(ending);
    }

    /// Steps the ending in the place; done with none there.
    #[turso_macros::atoll_entry]
    pub fn step_placed(&self) -> Result<IOResult<()>> {
        match self.ending.lock().as_mut() {
            Some(ending) => ending.step(),
            None => Ok(IOResult::Done(())),
        }
    }

    /// Whether the ending in the place commits and has published its commit.
    #[turso_macros::atoll_entry]
    pub fn placed_published(&self) -> bool {
        self.ending.lock().as_ref().is_some_and(Ending::published)
    }

    /// Hands the ending in the place the failure an IO it waited on met ([`Ending::failed`]).
    #[turso_macros::atoll_entry]
    pub fn fail_placed(&self, failure: LimboError) {
        if let Some(ending) = self.ending.lock().as_mut() {
            ending.failed(failure);
        }
    }

    /// Takes the ending out of the place: done, or to drop, which drives it to its end.
    #[turso_macros::atoll_entry]
    pub fn take_placed(&self) -> Option<Ending> {
        self.ending.lock().take()
    }

    /// A view for a statement that was reading the connection's transaction when another statement
    /// of the connection began to end it, and steps again before that ending is done (the auditor's
    /// #4435): the ending in the place is served ([`Ending::serve`]) until its transaction is over
    /// and the horizon covers its commit, its waits until then the statement's to wait on, and then
    /// a read transaction is begun, which sees what the commit left, as the one the ending begins
    /// for its siblings at its end does (DESIGN.md §17). Done at once with a transaction open.
    #[turso_macros::atoll_entry]
    pub fn serve_sibling(&self) -> Result<IOResult<()>> {
        if self.cell.observe().is_some() {
            return Ok(IOResult::Done(()));
        }
        if let Some(ending) = self.ending.lock().as_mut() {
            if let IOResult::IO(io) = ending.serve()? {
                return Ok(IOResult::IO(io));
            }
        }
        let begun = self.begin(Begin::Read)?;
        if !begun.is_io() {
            self.mark_served();
        }
        Ok(begun)
    }

    /// Holds the database's horizon where it stands, or lets it go, for a test.
    #[cfg(all(test, not(atoll_loom)))]
    #[turso_macros::atoll_entry]
    pub(crate) fn hold_horizon(&self, hold: bool) {
        self.db.atoll().transactions().hold_horizon(hold);
    }

    /// Marks the open transaction a read begun for siblings (`served`).
    #[turso_macros::atoll_entry]
    pub fn mark_served(&self) {
        self.served.store(true, Ordering::Release);
    }

    /// The schema cookie: at the transaction's view, or committed outside one. Driven to its end
    /// here, for a blocking host.
    #[turso_macros::atoll_entry]
    pub fn schema_cookie(&self) -> Result<u32> {
        let mut guard = self.cell.enter_now("schema_cookie")?;
        match guard.as_mut() {
            None => Ok(self.db.header().committed(Field::SchemaCookie)),
            Some(txn) => self.field_in(txn, Field::SchemaCookie),
        }
    }

    /// [`schema_cookie`](Self::schema_cookie) for a caller that keeps `read` across its returns:
    /// in a cooperative frame, the IO it waits for, and it goes on where it stopped when called
    /// again with the same `read` (ASYNC1 D2e §2.2).
    #[turso_macros::atoll_entry]
    pub fn schema_cookie_in(&self, read: &mut Option<Box<FacadeRead>>) -> Result<IOResult<u32>> {
        if read.is_none() {
            // Outside a frame the read ends in this call, as the synchronous form drives it.
            if !crate::storage::atoll::cooperative::inside() {
                return self.schema_cookie().map(IOResult::Done);
            }
            if let Some(word) = self.field_known(Field::SchemaCookie)? {
                return Ok(IOResult::Done(word));
            }
        }
        self.read_in(read, ReadAt::field(Field::SchemaCookie), Self::field_step)
    }

    /// The header as SQLite lays it out: every field's word at the transaction's view, or
    /// committed outside one. Its database size and free-list count are the main pager's, which
    /// holds no pages: what reads them walks the pager's pages. `PRAGMA page_count` reads the page
    /// store ([`pages`](Self::pages)); `freelist_count` stays 0, which is so, Atoll keeping no free
    /// list in its files, a freed slot being erased and taken again. Driven to its end here, for a
    /// blocking host.
    #[turso_macros::atoll_entry]
    pub fn header(&self) -> Result<DatabaseHeader> {
        let mut guard = self.cell.enter_now("header")?;
        let mut words = [0u32; 7];
        for field in Field::ALL {
            words[field as usize - 1] = match guard.as_mut() {
                None => self.db.header().committed(field),
                Some(txn) => self.field_in(txn, field)?,
            };
        }
        Ok(header_of(&words))
    }

    /// [`header`](Self::header) for a caller that keeps `read` across its returns, as
    /// [`schema_cookie_in`](Self::schema_cookie_in).
    #[turso_macros::atoll_entry]
    pub fn header_in(
        &self,
        read: &mut Option<Box<FacadeRead>>,
    ) -> Result<IOResult<DatabaseHeader>> {
        if read.is_none() {
            // Outside a frame the read ends in this call, as the synchronous form drives it.
            if !crate::storage::atoll::cooperative::inside() {
                return self.header().map(IOResult::Done);
            }
            if let Some(header) = self.header_known()? {
                return Ok(IOResult::Done(header));
            }
        }
        self.read_in(read, ReadAt::header(), Self::header_step)
    }

    /// The pages the page store holds in use (DESIGN.md §17, §12), every class's in node-size
    /// units, rounded up, as `PRAGMA page_count` reports them; past `u32::MAX`, the most a header
    /// word holds. Driven to its end here, for a blocking host.
    #[turso_macros::atoll_entry]
    pub fn pages(&self) -> Result<u32> {
        let store = self.db.atoll().store();
        let mut bytes: u64 = 0;
        for class in 0..crate::storage::atoll::page::CLASS_COUNT {
            let class = crate::storage::atoll::page::SizeClass::new(class).expect("a class");
            let n = drive_for(self.cell.conn(), self.db.io(), || store.pages_in_use(class))?;
            bytes = bytes.saturating_add(n.saturating_mul(class.size() as u64));
        }
        Ok(u32::try_from(bytes.div_ceil(PAGE as u64)).unwrap_or(u32::MAX))
    }

    /// [`pages`](Self::pages) for a caller that keeps `read` across its returns, as
    /// [`schema_cookie_in`](Self::schema_cookie_in).
    #[turso_macros::atoll_entry]
    pub fn pages_in(&self, read: &mut Option<Box<FacadeRead>>) -> Result<IOResult<u32>> {
        // Outside a frame the read ends in this call, as the synchronous form drives it.
        if read.is_none() && !crate::storage::atoll::cooperative::inside() {
            return self.pages().map(IOResult::Done);
        }
        self.read_in(read, ReadAt::pages(), Self::pages_step)
    }

    /// Header field `field` at `txn`'s view, driven to its end here, for a blocking host: as the
    /// transaction read it already while the header counts no change, else read and kept.
    fn field_in(&self, txn: &mut Txn, field: Field) -> Result<u32> {
        let (db, io) = (&self.db, self.db.io());
        let (identity, changes) = ((txn.slot(), txn.start()), db.header().changes());
        if let Some(word) = self.known_at(txn, field, changes) {
            return Ok(word);
        }
        #[cfg(test)]
        HEADER_READS.with(|r| r.set(r.get() + 1));
        let mut get = Header::get_op(field);
        let word = drive_for(self.cell.conn(), io, || {
            db.header().get(
                db.atoll().transactions(),
                db.versions(),
                txn,
                field,
                &mut get,
            )
        })?;
        self.fields.lock()[field.index()] = Some((word, identity, changes));
        Ok(word)
    }

    /// Header field `field` known without a read: committed outside a transaction, else as this
    /// transaction read it while the header counts no change.
    fn field_known(&self, field: Field) -> Result<Option<u32>> {
        let guard = self.cell.enter()?;
        let Some(txn) = guard.as_ref() else {
            return Ok(Some(self.db.header().committed(field)));
        };
        Ok(self.known_at(txn, field, self.db.header().changes()))
    }

    /// The header known without a read, as [`field_known`](Self::field_known) knows each field.
    fn header_known(&self) -> Result<Option<DatabaseHeader>> {
        let guard = self.cell.enter()?;
        let mut words = [0u32; 7];
        let changes = self.db.header().changes();
        for field in Field::ALL {
            words[field as usize - 1] = match guard.as_ref() {
                None => self.db.header().committed(field),
                Some(txn) => match self.known_at(txn, field, changes) {
                    Some(word) => word,
                    None => return Ok(None),
                },
            };
        }
        Ok(Some(header_of(&words)))
    }

    /// Header field `field` as `txn` read it, while the header counts `changes`.
    fn known_at(&self, txn: &Txn, field: Field, changes: u64) -> Option<u32> {
        let identity = (txn.slot(), txn.start());
        match self.fields.lock()[field.index()] {
            Some((word, t, c)) if t == identity && c == changes => Some(word),
            _ => None,
        }
    }

    /// A read kept in `read` across its returns: the one there goes on if it is of `fresh`'s kind
    /// and transaction, else `fresh` starts. Done or failed, it is let go. A read kept from a frame
    /// and entered again outside one is driven to its end, its kept state going on.
    fn read_in<T>(
        &self,
        read: &mut Option<Box<FacadeRead>>,
        fresh: ReadAt,
        step: impl Fn(&Self, Option<&mut Txn>, &mut ReadAt) -> Result<IOResult<T>>,
    ) -> Result<IOResult<T>> {
        let txn = self.cell.enter()?.as_ref().map(|t| (t.slot(), t.start()));
        let FacadeRead { owner, at, .. } = FacadeRead::kept(read, self.cell.conn(), txn, fresh);
        let out = drive_or_io(owner, self.db.io(), || {
            let mut guard = self.cell.enter()?;
            step(self, guard.as_mut(), at)
        });
        FacadeRead::settle(read, out)
    }

    /// One call's worth of a header field: from the fields read already, else its read stepped.
    fn field_step(&self, txn: Option<&mut Txn>, at: &mut ReadAt) -> Result<IOResult<u32>> {
        let ReadAt::Field { field, get } = at else {
            unreachable!("a field's step over another read");
        };
        Ok(IOResult::Done(match txn {
            None => self.db.header().committed(*field),
            Some(txn) => done!(self.field_at(txn, *field, get)),
        }))
    }

    /// One call's worth of the header: its fields in order, each as [`field_step`] reads it.
    fn header_step(
        &self,
        mut txn: Option<&mut Txn>,
        at: &mut ReadAt,
    ) -> Result<IOResult<DatabaseHeader>> {
        let ReadAt::Header { next, words, get } = at else {
            unreachable!("the header's step over another read");
        };
        while let Some(&field) = Field::ALL.get(*next) {
            words[field as usize - 1] = match txn.as_deref_mut() {
                None => self.db.header().committed(field),
                Some(txn) => done!(self.field_at(txn, field, get)),
            };
            *next += 1;
        }
        Ok(IOResult::Done(header_of(words)))
    }

    /// Header field `field` at `txn`'s view: the word this transaction read already while the
    /// header counts no change, else `get` stepped, made first for the field, and the word kept.
    fn field_at(
        &self,
        txn: &mut Txn,
        field: Field,
        get: &mut Option<(Get, u64)>,
    ) -> Result<IOResult<u32>> {
        let db = &self.db;
        let identity = (txn.slot(), txn.start());
        if get.is_none() {
            let changes = db.header().changes();
            if let Some(word) = self.known_at(txn, field, changes) {
                return Ok(IOResult::Done(word));
            }
            #[cfg(test)]
            HEADER_READS.with(|r| r.set(r.get() + 1));
            *get = Some((Header::get_op(field), changes));
        }
        let (op, changes) = get.as_mut().expect("a field read under way");
        op.resume_at_kept();
        let word = done!(db
            .header()
            .get(db.atoll().transactions(), db.versions(), txn, field, op));
        self.fields.lock()[field.index()] = Some((word, identity, *changes));
        *get = None;
        Ok(IOResult::Done(word))
    }

    /// One call's worth of the page store's pages in use: its classes in order.
    fn pages_step(&self, _txn: Option<&mut Txn>, at: &mut ReadAt) -> Result<IOResult<u32>> {
        let ReadAt::Pages { class, bytes } = at else {
            unreachable!("the pages' step over another read");
        };
        let store = self.db.atoll().store();
        while *class < crate::storage::atoll::page::CLASS_COUNT {
            let size = crate::storage::atoll::page::SizeClass::new(*class).expect("a class");
            let n = done!(store.pages_in_use(size));
            *bytes = bytes.saturating_add(n.saturating_mul(size.size() as u64));
            *class += 1;
        }
        Ok(IOResult::Done(
            u32::try_from(bytes.div_ceil(PAGE as u64)).unwrap_or(u32::MAX),
        ))
    }

    /// Applies `f` to the header and writes every field it changed, in the open transaction's
    /// current statement. Each field written is recorded for the commit to publish. A write behind
    /// another transaction comes back to wait; applying `f` again goes on, the fields already
    /// written reading as written.
    #[turso_macros::atoll_entry]
    pub fn set_header<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> Result<IOResult<T>> {
        loop {
            let before = self.header()?;
            let mut after = before;
            let out = f(&mut after);
            if word(&after, AUTO_VACUUM) != 0 {
                return Err(LimboError::InvalidArgument(
                    "an Atoll database has no auto-vacuum: its pages are freed as its trees \
                     drain"
                        .to_string(),
                ));
            }
            let (old, new) = (words_of(&before), words_of(&after));
            let mut setting = self.setting.lock();
            let field = match setting.as_ref() {
                Some((field, _, _)) => *field,
                None => match Field::ALL
                    .into_iter()
                    .find(|f| old[*f as usize - 1] != new[*f as usize - 1])
                {
                    Some(field) => field,
                    None => return Ok(IOResult::Done(out)),
                },
            };
            let value = new[field as usize - 1];
            let (_, op, waiting) = setting
                .get_or_insert_with(|| (field, Header::put_op(field, value), Waiting::default()));
            let mut guard = self.cell.enter()?;
            let txn = guard.as_mut().ok_or_else(outside)?;
            let w = done!(self.db.header().write(
                self.db.atoll().transactions(),
                self.db.versions(),
                txn,
                op
            ));
            match waiting.judge(&self.db, &self.waits, w)? {
                Judged::Done => {
                    if field == Field::SchemaCookie {
                        txn.wrote_schema();
                    }
                    self.written.lock().push((txn.command(), field, value));
                    *setting = None;
                }
                Judged::Park(c) => return waiting.wait(&self.waits, c),
                Judged::Again => {}
            }
        }
    }

    /// Destroy, or ClearBtree with `clear` (DESIGN.md §17, §5): tree `id`'s catalog row deleted,
    /// or pointed at a new, empty tree, and its old root listed as dropped, in the current
    /// statement. Step it with the same `op` until done; a Clear returns the rows the tree held at
    /// the view, which its statement counts as changed.
    #[turso_macros::atoll_entry]
    pub fn retire_tree(
        &self,
        op: &mut Option<RetireOp>,
        id: u64,
        clear: bool,
    ) -> Result<IOResult<u64>> {
        let op = op.get_or_insert_with(|| RetireOp {
            destroy: Destroy::new(id),
            waiting: Waiting::default(),
            counted: None,
            count: None,
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        });
        if clear && op.counted.is_none() {
            let inside = crate::storage::atoll::cooperative::inside();
            if op.count.is_none() && !inside {
                // A blocking host with no count kept: the count as before, its cursor on the stack.
                op.counted = Some(self.count_rows(id)?);
            } else {
                let count = op.count.get_or_insert_with(Count::new);
                let out = if inside {
                    self.count_step(id, count)
                } else {
                    // A count kept from a frame, entered outside one: driven to its end here.
                    drive_for(self.cell.conn(), self.db.io(), || {
                        self.count_step(id, count)
                    })
                    .map(IOResult::Done)
                };
                match out {
                    Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                    Ok(IOResult::Done(n)) => {
                        op.count = None;
                        op.counted = Some(n);
                    }
                    Err(e) => {
                        // Failed, the count is let go at this return: its read's owner and its
                        // cursor's.
                        op.count = None;
                        return Err(e);
                    }
                }
            }
        }
        loop {
            let mut guard = self.cell.enter()?;
            let txn = guard.as_mut().ok_or_else(outside)?;
            let w = done!(if clear {
                self.db.clear(txn, &mut op.destroy)
            } else {
                self.db.destroy(txn, &mut op.destroy)
            });
            match op.waiting.judge(&self.db, &self.waits, w)? {
                Judged::Done => {
                    txn.wrote_schema();
                    if clear {
                        self.changes.cleared(id);
                    } else {
                        self.changes
                            .destroyed(|list| list.push((id, txn.command())));
                    }
                    return Ok(IOResult::Done(op.counted.unwrap_or(0)));
                }
                Judged::Park(c) => return op.waiting.wait(&self.waits, c),
                Judged::Again => {}
            }
        }
    }

    /// The rows tree `id` holds as the current statement reads them.
    fn count_rows(&self, id: u64) -> Result<u64> {
        let entry = {
            let mut guard = self.cell.enter_now("count_rows")?;
            let txn = guard.as_mut().ok_or_else(outside)?;
            let mut get = Get::new(&catalog::key(id));
            let db = &self.db;
            drive_for(self.cell.conn(), db.io(), || {
                db.catalog()
                    .get(db.atoll().transactions(), db.versions(), txn, &mut get)
            })?
            .ok_or_else(|| LimboError::InternalError(format!("no tree {id} in the catalog")))?
        };
        let mut cursor = AtollCursor::new(
            self.db.clone(),
            self.cell.clone(),
            id,
            entry,
            None,
            key::ENCODING,
        );
        let io = self.db.io().clone();
        drive_for(self.cell.conn(), &io, || cursor.rewind())?;
        let mut n = 0;
        while !cursor.is_empty() {
            n += 1;
            drive_for(self.cell.conn(), &io, || cursor.next())?;
        }
        Ok(n)
    }

    /// One call's worth of a Clear's count in a cooperative frame (ASYNC1 D2e (iii-a)), its
    /// progress kept in `count` across its returns for IO: the tree's catalog row, read as a
    /// cursor's resolution reads it (D2e (ii)); a cursor over the tree from its first row; and each
    /// row counted as the cursor leaves it, never again when a move past it is entered again.
    fn count_step(&self, id: u64, count: &mut Count) -> Result<IOResult<u64>> {
        loop {
            match count.at {
                CountAt::Resolving => {
                    let entry = {
                        let mut guard = self.cell.enter()?;
                        let txn = guard.as_mut().ok_or_else(outside)?;
                        let out = row_in_kept(
                            &self.db,
                            self.cell.conn(),
                            &self.known,
                            id,
                            txn,
                            &mut count.read,
                        );
                        match FacadeRead::settle(&mut count.read, out)? {
                            IOResult::Done((row, _)) => row.entry,
                            IOResult::IO(io) => return Ok(IOResult::IO(io)),
                        }
                    };
                    count.cursor = Some(Box::new(AtollCursor::new(
                        self.db.clone(),
                        self.cell.clone(),
                        id,
                        entry,
                        None,
                        key::ENCODING,
                    )));
                    count.at = CountAt::Rewinding;
                }
                CountAt::Rewinding => {
                    done!(count.cursor.as_mut().expect("a count's cursor").rewind());
                    count.at = CountAt::At(0);
                }
                CountAt::At(n) => {
                    if count.cursor.as_ref().expect("a count's cursor").is_empty() {
                        return Ok(IOResult::Done(n));
                    }
                    count.at = CountAt::Moving(n + 1);
                }
                CountAt::Moving(n) => {
                    done!(count.cursor.as_mut().expect("a count's cursor").next());
                    count.at = CountAt::At(n);
                }
            }
        }
    }

    /// CreateBtree (DESIGN.md §17, §5): draws a tree id, creates its tree in the current
    /// statement and writes its catalog row, an index's (or a WITHOUT ROWID table's, R29) or a
    /// table's. Step it with the same `op` until done; returns the tree id, which Turso calls the
    /// root page.
    #[turso_macros::atoll_entry]
    pub fn create_btree(&self, op: &mut Option<CreateOp>, index: bool) -> Result<IOResult<u64>> {
        let op = op.get_or_insert_with(|| CreateOp {
            id: self.db.draw_tree_id(),
            root: None,
            write: None,
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        });
        let mut guard = self.cell.enter()?;
        let txn = guard.as_mut().ok_or_else(outside)?;
        let root = match op.root {
            Some(root) => root,
            None => *op
                .root
                .insert(done!(self.db.create_tree(txn, System::class()))),
        };
        let write = op.write.get_or_insert_with(|| {
            let entry = Entry {
                root,
                layout: if index { Layout::Index } else { Layout::Table },
                encoding: if index { key::ENCODING } else { 0 },
                created_by: 0,
            };
            Catalog::put_op(op.id, &entry, Mode::Insert)
        });
        let w = done!(self.db.catalog().write(
            self.db.atoll().transactions(),
            self.db.versions(),
            txn,
            write
        ));
        match w {
            Write::Done(Outcome::Inserted) => {
                txn.wrote_schema();
                Ok(IOResult::Done(op.id))
            }
            other => Err(LimboError::InternalError(format!(
                "the catalog row of the tree {} drawn now came to {other:?}",
                op.id
            ))),
        }
    }
}

/// Destroy's or ClearBtree's state across IO and waits.
pub struct RetireOp {
    destroy: Destroy,
    waiting: Waiting,
    /// For a Clear, the rows the tree held at the view.
    counted: Option<u64>,
    /// For a Clear in a cooperative frame, its count under way (ASYNC1 D2e (iii-a)).
    count: Option<Count>,
    /// Dropped last: the end of the frame its `Drop` opens (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

/// A Clear's count of its tree's rows under way in a cooperative frame (ASYNC1 D2e (iii-a)): what
/// its steps keep across their returns for IO.
struct Count {
    /// The tree's catalog row's read, kept across its returns (D2e (ii)'s).
    read: Option<Box<FacadeRead>>,
    cursor: Option<Box<AtollCursor>>,
    at: CountAt,
}

/// Where a count stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CountAt {
    /// The tree's catalog row is being read.
    Resolving,
    /// The cursor goes to the first row.
    Rewinding,
    /// At a row, `n` counted before it; or past the last, `n` rows in all.
    At(u64),
    /// Moving past a row, which `n` counts already.
    Moving(u64),
}

impl Count {
    fn new() -> Self {
        Self {
            read: None,
            cursor: None,
            at: CountAt::Resolving,
        }
    }
}

impl RetireOp {
    /// Where its count stands, if one is under way, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn count_at_for_test(&self) -> Option<&'static str> {
        self.count.as_ref().map(|c| match c.at {
            CountAt::Resolving => "resolving",
            CountAt::Rewinding => "rewinding",
            CountAt::At(_) => "at",
            CountAt::Moving(_) => "moving",
        })
    }

    /// The frame and metadata page its count's catalog read keeps, and its cursor's owner, for a
    /// test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn count_keeps_for_test(
        &self,
    ) -> (
        Option<(Option<u32>, Option<crate::storage::atoll::meta::MetaId>)>,
        Option<(Option<u32>, Option<crate::storage::atoll::meta::MetaId>)>,
    ) {
        match &self.count {
            Some(c) => (
                c.read.as_ref().map(|r| r.keeps_for_test()),
                c.cursor.as_ref().map(|c| c.keeps_for_test()),
            ),
            None => (None, None),
        }
    }

    /// Whether its Clear has counted, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn counted_for_test(&self) -> Option<u64> {
        self.counted
    }
}

/// A full checkpoint (DESIGN.md §17, §11): one begun now, done, for `PRAGMA wal_checkpoint` and
/// `Connection::checkpoint`. Driven to its end here, as a WAL checkpoint of the whole log is.
#[turso_macros::atoll_entry]
pub fn full_checkpoint(db: &AtollDb) -> Result<()> {
    let checkpointer = db.atoll().checkpointer();
    let mut ticket = checkpointer.request_full();
    let txns = db.atoll().transactions();
    drive(db.io(), || checkpointer.step_full(&mut ticket, txns))
}

/// Steps the checkpointer only while a checkpoint is due, once its share of the log budget has
/// been appended, or one is under way (DESIGN.md 22.9, Due). Its IO is not waited for: the
/// checkpoint goes on at the next step, whoever takes it.
fn nudge_checkpoint(db: &AtollDb) -> Result<()> {
    db.atoll()
        .checkpointer()
        .step_due(db.atoll().transactions())
        .map(|_| ())
}

/// CreateBtree's state across IO.
pub struct CreateOp {
    id: u64,
    root: Option<PageId>,
    write: Option<WriteOp>,
    /// Dropped last: the end of the frame its `Drop` opens (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

/// A connection's transaction state dropped: inside a frame (DESIGN.md 22.11), its fields after
/// it, the last ending it.
impl Drop for ConnTxn {
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {}
}

/// An operation's state the VDBE held, dropped inside a frame (DESIGN.md 22.11).
impl Drop for CreateOp {
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {}
}

/// An operation's state the VDBE held, dropped inside a frame (DESIGN.md 22.11).
impl Drop for RetireOp {
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {}
}

/// A read's state the VDBE held, its owner's tenure and keeps let go inside a frame (DESIGN.md
/// 22.11).
impl Drop for FacadeRead {
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {}
}

fn outside() -> LimboError {
    LimboError::InternalError("an Atoll read or write outside any transaction".to_string())
}

/// An open transaction taken out of its connection to be ended ([`ConnTxn::take_to_end`]), with the
/// header fields it wrote and the stamps of the trees it wrote.
type EndedTxn = (
    Txn,
    Vec<(Statement, Field, u32)>,
    Vec<super::trees::Stamp>,
    Option<Box<OwedRollback>>,
);

/// A transaction's commit or rollback, then the drain of the trees it abandoned. Step it until
/// done. Never abandoned: dropped unfinished, it is driven to its end where it drops (R19).
pub struct Ending {
    db: Arc<AtollDb>,
    step: Step,
    /// Whether its commit was published, once its commit step is behind it.
    published: bool,
    /// The top of the ending, which keeps its memory-shortage tenure across IO (DESIGN.md 9.1.3,
    /// *The top and the owner*).
    owner: crate::storage::atoll::buffer::Owner,
    /// Dropped last: the end of the frame its `Drop` opens (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

// One per transaction's ending, held in place while it steps: boxing the commit would allocate on
// every commit.
#[allow(clippy::large_enum_variant)]
enum Step {
    /// A statement rollback the transaction owed, finished before its commit (R19, ASYNC1 D2e
    /// (iii-b)): the rollback, the completion the ending's last step returned while it was in this
    /// step, until the next step has judged it, and the commit to step once it is done.
    Owed {
        rollback: Box<crate::storage::atoll::txn::rollback::Rollback>,
        waiting: Option<Completion>,
        commit: Commit,
    },
    Commit(Commit),
    /// A rollback, with the failure of the commit it replaces: a commit that failed before it
    /// published rolls its transaction back, and its failure is then the ending's outcome.
    Rollback(Abort, Option<LimboError>),
    Drain,
    /// A full checkpoint, by the wait for it, which holds the end's demand on the checkpoint.
    Checkpoint(crate::storage::atoll::checkpoint::FullWait),
    /// An ending that failed, whoever stepped it when it did: its next step returns the failure,
    /// and it is over (DESIGN.md §17). A commit whose IO failed once its transaction ended,
    /// published or having written nothing; a commit that failed before it published, its rollback
    /// done; a rollback that could not finish (`Transactions::abandon`).
    Failed(LimboError),
    Done,
}

impl Ending {
    /// Commits `txn`: once it is published, its header fields `written` and the stamps of the
    /// trees it wrote are raised to its commit timestamp, and `publish` runs, before it releases
    /// the write gate. A statement rollback `txn` owes is finished first, its top the ending's own
    /// (ASYNC1 D2e (iii-b)); one that failed makes the commit the transaction's rollback, its
    /// failure the outcome.
    fn commit(
        db: Arc<AtollDb>,
        conn: Arc<crate::storage::atoll::buffer::Conn>,
        txn: Txn,
        written: Vec<(Statement, Field, u32)>,
        stamped: Vec<super::trees::Stamp>,
        publish: Option<Box<dyn FnOnce() + Send>>,
        owed: Option<Box<OwedRollback>>,
    ) -> Self {
        #[cfg(test)]
        crate::storage::atoll::testing::ending_made();
        let published = db.clone();
        let mut commit = Commit::new(txn).on_published(move |ts| {
            for (_, field, value) in written {
                published.header().publish(field, ts, value);
            }
            super::trees::TreeStamps::raise(&stamped, ts);
            if let Some(publish) = publish {
                publish();
            }
        });
        let Some(owed) = owed else {
            return Self {
                db,
                step: Step::Commit(commit),
                published: false,
                owner: crate::storage::atoll::buffer::Owner::of(conn),
                _frame: crate::storage::atoll::unwind::FrameEnd::default(),
            };
        };
        let mut owner = crate::storage::atoll::buffer::Owner::of(conn);
        let step = match Some(owed).map(|o| o.into_parts()) {
            None => Step::Commit(commit),
            // Its top goes on as the ending's, its keeps with it; its place in the shortage queue,
            // if it had one, `take_to_end` ended, and its next call starts afresh.
            Some(Ok((rollback, top, waiting))) => {
                owner = top;
                Step::Owed {
                    rollback: Box::new(rollback),
                    waiting,
                    commit,
                }
            }
            Some(Err(failure)) => Step::Rollback(
                Abort::new(
                    commit
                        .take_txn()
                        .expect("a commit not begun holds its transaction"),
                ),
                Some(failure),
            ),
        };
        Self {
            db,
            step,
            published: false,
            owner,
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    fn rollback(
        db: Arc<AtollDb>,
        conn: Arc<crate::storage::atoll::buffer::Conn>,
        txn: Txn,
    ) -> Self {
        #[cfg(test)]
        crate::storage::atoll::testing::ending_made();
        if !txn.wrote() {
            db.atoll().transactions().abort(txn);
            return Self {
                db,
                step: Step::Drain,
                published: false,
                owner: crate::storage::atoll::buffer::Owner::of(conn),
                _frame: crate::storage::atoll::unwind::FrameEnd::default(),
            };
        }
        Self {
            db,
            step: Step::Rollback(Abort::new(txn), None),
            published: false,
            owner: crate::storage::atoll::buffer::Owner::of(conn),
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    #[turso_macros::atoll_entry]
    pub fn step(&mut self) -> Result<IOResult<()>> {
        if matches!(self.step, Step::Owed { .. }) {
            return self.step_while_owed();
        }
        crate::storage::atoll::buffer::Owner::call_in(self, |e| &mut e.owner, Self::step_owned)
    }

    /// [`step`](Self::step) in [`Step::Owed`]: its wait judged before the call, and kept after it.
    #[cold]
    #[inline(never)]
    fn step_while_owed(&mut self) -> Result<IOResult<()>> {
        if let Some(io) = self.judge_owed_wait() {
            return Ok(IOResult::IO(io));
        }
        let out =
            crate::storage::atoll::buffer::Owner::call_in(self, |e| &mut e.owner, Self::step_owned);
        self.keep_owed_wait(&out);
        out
    }

    /// In [`Step::Owed`], the completion the ending's last step returned, judged before its top
    /// admits another call: unfinished, the same wait; failed, the commit becomes the
    /// transaction's rollback with that failure, the statement rollback never stepped again;
    /// finished cleanly, let go.
    fn judge_owed_wait(&mut self) -> Option<IOCompletions> {
        let Step::Owed { waiting, .. } = &mut self.step else {
            return None;
        };
        let failure = match waiting.take() {
            None => return None,
            Some(c) if !c.finished() => {
                *waiting = Some(c.clone());
                return Some(IOCompletions(c));
            }
            Some(c) => c.get_error()?,
        };
        self.owed_failed(LimboError::CompletionError(failure));
        None
    }

    /// The wait an ending in [`Step::Owed`] keeps from its last call, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn owed_wait_for_test(&self) -> Option<Completion> {
        match &self.step {
            Step::Owed { waiting, .. } => waiting.clone(),
            _ => None,
        }
    }

    /// Keeps, in [`Step::Owed`], the completion the step returned through its top: the wait that
    /// stands, which the top may have put in place of the rollback's own.
    fn keep_owed_wait(&mut self, out: &Result<IOResult<()>>) {
        if let (Step::Owed { waiting, .. }, Ok(IOResult::IO(IOCompletions(c)))) =
            (&mut self.step, out)
        {
            *waiting = Some(c.clone());
        }
    }

    /// The statement rollback the transaction owed failed: the commit becomes the transaction's
    /// rollback, the failure its outcome, as a commit that fails before it publishes does.
    fn owed_failed(&mut self, failure: LimboError) {
        let Step::Owed { commit, .. } = &mut self.step else {
            unreachable!("an owed rollback's failure in another step")
        };
        let txn = commit
            .take_txn()
            .expect("a commit not begun holds its transaction");
        self.step = Step::Rollback(Abort::new(txn), Some(failure));
    }

    /// The statement rollback the transaction owed, one call, under the ending's top.
    fn step_owed(&mut self) -> Result<IOResult<()>> {
        #[cfg(test)]
        ENDING_OWED_STEPS.with(|n| n.set(n.get() + 1));
        let Step::Owed {
            rollback, commit, ..
        } = &mut self.step
        else {
            unreachable!("an owed rollback's step in another step")
        };
        let txn = commit
            .txn_mut()
            .expect("a commit not begun holds its transaction");
        let stepped = self
            .db
            .atoll()
            .transactions()
            .rollback_to(self.db.versions(), txn, rollback);
        #[cfg(test)]
        if let Ok(IOResult::IO(IOCompletions(c))) = &stepped {
            ENDING_OWED_RAW.with(|r| *r.borrow_mut() = Some(c.clone()));
        }
        match stepped {
            Ok(IOResult::IO(io)) => Ok(IOResult::IO(io)),
            Ok(IOResult::Done(())) => {
                let Step::Owed { commit, .. } = std::mem::replace(&mut self.step, Step::Done)
                else {
                    unreachable!("the step just matched")
                };
                self.step = Step::Commit(commit);
                Ok(IOResult::Done(()))
            }
            Err(failure) => {
                self.owed_failed(failure);
                Ok(IOResult::Done(()))
            }
        }
    }

    /// An IO the ending waited on failed, its commit published (DESIGN.md §17): the commit stands,
    /// the next step returns the failure, and the ending is over. What was left of it, the wait for
    /// durability, the drain (which the next ending's goes on with) or a full checkpoint's wait, is
    /// not stepped again: stepped again, a fix whose read failed would issue the read anew.
    #[turso_macros::atoll_entry]
    pub fn failed(&mut self, failure: LimboError) {
        crate::turso_assert!(
            self.published(),
            "only a published commit's ending takes its IO's failure"
        );
        self.published = true;
        self.step = Step::Failed(failure);
    }

    /// Whether the ending holds a memory-shortage tenure, for a test.
    #[cfg(test)]
    #[turso_macros::atoll_entry]
    pub(crate) fn shortage_queued(&self) -> bool {
        self.owner.queued()
    }

    /// A sibling's step of the ending (DESIGN.md §17): the transaction's own step, until the
    /// transaction is over (its commit published, or ended having written nothing; its rollback's
    /// changes put back), its waits the sibling's, then the wait for the horizon to cover a
    /// commit, once a snapshot taken sees it. Never past: the wait for durability, the drain and a
    /// full checkpoint's wait are the ending's own, and their IO is for the statement that began it
    /// (the auditor's #4454, #4481). Nor does it return the ending's failure, whoever met it: a
    /// commit failing before it published goes to its rollback, which the sibling steps on, and
    /// the failure is kept in the ending for that statement (`Step::Failed`).
    #[turso_macros::atoll_entry]
    pub fn serve(&mut self) -> Result<IOResult<()>> {
        if matches!(self.step, Step::Owed { .. }) {
            return self.serve_while_owed();
        }
        crate::storage::atoll::buffer::Owner::call_in(self, |e| &mut e.owner, Self::serve_owned)
    }

    /// [`serve`](Self::serve) in [`Step::Owed`]: its wait judged before the call, and kept after it.
    #[cold]
    #[inline(never)]
    fn serve_while_owed(&mut self) -> Result<IOResult<()>> {
        if let Some(io) = self.judge_owed_wait() {
            return Ok(IOResult::IO(io));
        }
        let out = crate::storage::atoll::buffer::Owner::call_in(
            self,
            |e| &mut e.owner,
            Self::serve_owned,
        );
        self.keep_owed_wait(&out);
        out
    }

    fn serve_owned(&mut self) -> Result<IOResult<()>> {
        // The statement rollback owed is the transaction's own step, before it is over.
        if matches!(self.step, Step::Owed { .. }) {
            done!(self.step_owed());
        }
        if matches!(&self.step, Step::Commit(op) if !op.ended()) {
            done!(self.step_commit_end());
        }
        if matches!(self.step, Step::Rollback(..)) {
            return self.step_rollback();
        }
        let Step::Commit(op) = &self.step else {
            return Ok(IOResult::Done(()));
        };
        Ok(
            match op
                .ts()
                .and_then(|ts| self.db.atoll().transactions().wait_covered(ts))
            {
                Some(wait) => IOResult::IO(IOCompletions(wait)),
                None => IOResult::Done(()),
            },
        )
    }

    /// The commit's step to its transaction's end and no further (`Transactions::end_commit`), a
    /// sibling's: what is left is the commit's waits, the ending's own.
    fn step_commit_end(&mut self) -> Result<IOResult<()>> {
        let Step::Commit(op) = &mut self.step else {
            unreachable!("a commit's step of an ending that commits")
        };
        #[cfg(test)]
        if let Some(pause) = crate::storage::atoll::testing::pause_ending() {
            return Ok(IOResult::IO(IOCompletions(pause)));
        }
        let stepped = self.db.atoll().transactions().end_commit(op);
        match stepped {
            Ok(io) => Ok(io),
            Err(failure) => {
                self.commit_failed(failure);
                Ok(IOResult::Done(()))
            }
        }
    }

    /// The commit's step, then the drain once it is done.
    fn step_commit(&mut self) -> Result<IOResult<()>> {
        let Step::Commit(op) = &mut self.step else {
            unreachable!("a commit's step of an ending that commits")
        };
        #[cfg(test)]
        if !op.published() {
            if let Some(pause) = crate::storage::atoll::testing::pause_ending() {
                return Ok(IOResult::IO(IOCompletions(pause)));
            }
        }
        let stepped = self.db.atoll().transactions().commit(op);
        match stepped {
            Ok(IOResult::Done(_)) => {}
            Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
            Err(failure) => {
                self.commit_failed(failure);
                return Ok(IOResult::Done(()));
            }
        }
        self.db.abandoned(op.abandoned());
        self.published = op.published();
        self.step = Step::Drain;
        Ok(IOResult::Done(()))
    }

    /// A commit's step met `failure` (DESIGN.md §17). Before its transaction ended (its log refusing
    /// a part's or the Commit's room, the auditor's #4468) nothing it wrote was published: its
    /// transaction goes to a rollback, the ending's next step, and the failure is the ending's
    /// outcome once that is done. After, its commit published or its transaction having written
    /// nothing, the commit stands and the ending is over with the failure, what was left of it
    /// (the wait for durability, the drain, a full checkpoint's wait) not stepped again.
    fn commit_failed(&mut self, failure: LimboError) {
        let Step::Commit(op) = &mut self.step else {
            unreachable!("a commit's failure in an ending that commits")
        };
        self.step = match op.take_txn() {
            Some(txn) => Step::Rollback(Abort::new(txn), Some(failure)),
            None => {
                self.published = op.published();
                Step::Failed(failure)
            }
        };
    }

    /// The rollback's step, then the drain once it is done, or the failure of the commit it
    /// replaces. A rollback that cannot finish (its undo unable to read back a part the log never
    /// made durable, or to put a key back) leaves its transaction where it stands, its slot and
    /// versions with it, and the database failed until it is reopened (`Transactions::abandon`):
    /// never a slot freed while versions it wrote remain. Its failure is the ending's outcome.
    fn step_rollback(&mut self) -> Result<IOResult<()>> {
        let Step::Rollback(op, _) = &mut self.step else {
            unreachable!("a rollback's step of an ending that rolls back")
        };
        let txns = self.db.atoll().transactions();
        match txns.rollback(self.db.versions(), op) {
            Ok(IOResult::Done(())) => {}
            Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
            Err(failure) => {
                if let Some(txn) = op.take_txn() {
                    txns.abandon(txn, failure.clone());
                }
                self.step = Step::Failed(failure);
                return Ok(IOResult::Done(()));
            }
        }
        self.db.abandoned(op.abandoned());
        let Step::Rollback(_, commit_failure) = std::mem::replace(&mut self.step, Step::Done)
        else {
            unreachable!("the step just matched")
        };
        self.step = match commit_failure {
            Some(failure) => Step::Failed(failure),
            None => Step::Drain,
        };
        Ok(IOResult::Done(()))
    }

    fn step_owned(&mut self) -> Result<IOResult<()>> {
        loop {
            match &mut self.step {
                Step::Owed { .. } => done!(self.step_owed()),
                Step::Commit(_) => done!(self.step_commit()),
                Step::Rollback(..) => done!(self.step_rollback()),
                Step::Drain => {
                    done!(self.db.drain());
                    // A transaction's end steps the checkpointer, holding no latch. Past twice its
                    // budget behind the log's end, the budget the design keeps the redo point to
                    // and once more for what is appended while a pass runs (DESIGN.md §10, §17,
                    // §11), the end waits for a full checkpoint: a pass is due per budget share
                    // appended, so one bulk statement would otherwise leave the redo point behind
                    // until as much again is written. Not when the first part of a transaction
                    // still running holds the redo point there (DESIGN.md §11.4.1): no checkpoint
                    // could bring it nearer.
                    let checkpointer = self.db.atoll().checkpointer();
                    let over = 2 * checkpointer.budget();
                    self.step = if checkpointer.lag() > over
                        && checkpointer.lag_to(self.db.atoll().transactions().held_from()) > over
                    {
                        Step::Checkpoint(checkpointer.request_full())
                    } else {
                        nudge_checkpoint(&self.db)?;
                        Step::Done
                    };
                }
                Step::Checkpoint(wait) => {
                    let txns = self.db.atoll().transactions();
                    done!(self.db.atoll().checkpointer().step_full(wait, txns));
                    self.step = Step::Done;
                }
                Step::Failed(_) => {
                    let Step::Failed(failure) = std::mem::replace(&mut self.step, Step::Done)
                    else {
                        unreachable!("the step just matched")
                    };
                    return Err(failure);
                }
                Step::Done => return Ok(IOResult::Done(())),
            }
        }
    }

    /// Whether the ending commits and its commit has been published: what the transaction wrote
    /// is visible and stays so, whatever a later step of the ending, its wait for durability
    /// among them, meets.
    #[turso_macros::atoll_entry]
    pub fn published(&self) -> bool {
        match &self.step {
            Step::Commit(op) => op.published(),
            _ => self.published,
        }
    }

    /// Drives the ending to its end, for the synchronous sites that end a transaction.
    #[turso_macros::atoll_entry]
    pub fn finish(&mut self) -> Result<()> {
        self.drive_to_end()
    }

    /// Drives the ending to its end. An IO it waits on that fails is seen by the driver, which
    /// returns the completion's error without stepping the ending again (`drive`): the ending takes
    /// that failure as the step that waited on it would have (`Ending::met`), and is driven on, so
    /// a rollback whose read failed leaves its transaction to `Transactions::abandon` rather than
    /// dropped with its slot and gate held (the auditor's #4485, #4486).
    fn drive_to_end(&mut self) -> Result<()> {
        let io = self.db.io().clone();
        loop {
            match drive(&io, || self.step()) {
                Ok(()) => return Ok(()),
                Err(failure) if matches!(self.step, Step::Done) => return Err(failure),
                Err(failure) => self.met(failure),
            }
        }
    }

    /// An IO the ending waited on failed, the failure seen by its driver rather than returned by
    /// its step: the step that waited takes it. A commit whose transaction has not ended goes to
    /// its rollback, the failure its outcome (`commit_failed`); a rollback leaves its transaction
    /// where it stands, the database failed (`Transactions::abandon`); the waits after a
    /// transaction's end leave the ending failed. Each moves the ending on, so a driver taking
    /// failures meets each step at most once.
    fn met(&mut self, failure: LimboError) {
        if matches!(self.step, Step::Commit(_)) {
            #[cfg(test)]
            COMMITS_MET.with(|n| n.set(n.get() + 1));
            return self.commit_failed(failure);
        }
        match &mut self.step {
            Step::Rollback(op, _) => {
                if let Some(txn) = op.take_txn() {
                    self.db.atoll().transactions().abandon(txn, failure.clone());
                }
                self.step = Step::Failed(failure);
            }
            Step::Drain | Step::Checkpoint(_) => self.step = Step::Failed(failure),
            // The owed statement rollback's wait failed: it is never stepped again.
            Step::Owed { .. } => self.owed_failed(failure),
            Step::Commit(_) | Step::Failed(_) | Step::Done => {}
        }
    }
}

impl std::fmt::Debug for Ending {
    #[turso_macros::atoll_entry]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let step = match self.step {
            Step::Owed { .. } => "rolling a statement back",
            Step::Commit(_) => "committing",
            Step::Rollback(..) => "rolling back",
            Step::Drain => "draining",
            Step::Checkpoint(_) => "checkpointing",
            Step::Failed(_) => "failed",
            Step::Done => "done",
        };
        f.debug_tuple("Ending").field(&step).finish()
    }
}

impl Drop for Ending {
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {
        if matches!(self.step, Step::Done) {
            return;
        }
        if let Err(e) = self.drive_to_end() {
            tracing::error!("an Atoll transaction's ending, dropped unfinished, failed: {e}");
        }
    }
}

/// A Turso cursor over one SQL tree, which resolves its catalog row when it opens, a read driven
/// to its end, and again after a Clear replaced its tree (R22).
pub struct SqlCursor {
    db: Arc<AtollDb>,
    cell: TxnCell,
    pager: Arc<Pager>,
    id: u64,
    index: Option<Arc<IndexInfo>>,
    /// Boxed: a statement's cursor is opened, moved and dropped whole, and the cursor is large.
    open: Option<Box<AtollCursor>>,
    /// The null flag set before the cursor resolved.
    null_row: bool,
    has_record: bool,
    waiting: Waiting,
    /// How its connection's statements wait (R34).
    waits: Arc<Waits>,
    /// Whether a write meeting another transaction's open version fails at once, as MVCC's do.
    no_wait: bool,
    /// The rowid counter's seed under way.
    seeding: Option<Seed>,
    /// A count under way.
    counting: Option<Counting>,
    /// Its connection's changes, and the epoch its position was taken at.
    changes: Arc<TreeChanges>,
    seen: u64,
    /// Bound again to the tree a Clear made, at the key it stood at, under way.
    rebinding: Option<Option<Vec<u8>>>,
    /// Opened on a tree its statement had destroyed: it reads as empty, and refuses writes.
    vacant: bool,
    /// Its connection's transaction's written-tree stamps.
    stamped: Arc<Mutex<HashMap<PageId, super::trees::Stamp>>>,
    /// Whether it walks the versions its statement does not see (R10, FkUnseen).
    unseen: bool,
    /// What its connection's cursors learned of the trees.
    known: Arc<Mutex<HashMap<u64, Known>>>,
    /// The catalog row of its tree it last went by, which holds as the connection's [`Known`]
    /// row does: its statement's next execution takes it up with no lookup.
    row: Option<KnownRow>,
    /// Its resolution's catalog read, kept across its returns for IO in a cooperative frame
    /// (ASYNC1 D2e (ii)): let go at every other end of the call that reads.
    resolving: Option<Box<FacadeRead>>,
    /// An open begun and not finished ([`SqlCursor::finish_open`]).
    opening: Option<Opening>,
    /// Dropped last: the end of the frame its `Drop` opens (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

/// What an open under way finishes: a new cursor's resolution, or a parked one's taking up again
/// ([`SqlCursor::reopened`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Opening {
    Fresh,
    Reopen,
}

/// A cursor leaves the leaf it stood at to the next over its tree.
impl Drop for SqlCursor {
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {
        if let Some(cursor) = &self.open {
            self.known.lock().entry(self.id).or_default().near = cursor.near();
        }
    }
}

/// A count's progress: at a row, rows counted before it; or moving past one.
#[derive(Debug, Clone, Copy)]
enum Counting {
    At(usize),
    Moving(usize),
}

impl SqlCursor {
    /// A cursor over tree `id` in `ct`'s transaction. `index` describes an index's or WITHOUT
    /// ROWID table's keys.
    #[turso_macros::atoll_entry]
    pub fn new(ct: &ConnTxn, pager: Arc<Pager>, id: u64, index: Option<Arc<IndexInfo>>) -> Self {
        Self {
            db: ct.db.clone(),
            cell: ct.cell.clone(),
            pager,
            id,
            index,
            open: None,
            null_row: false,
            has_record: false,
            waiting: Waiting::default(),
            waits: ct.waits.clone(),
            no_wait: false,
            seeding: None,
            counting: None,
            changes: ct.changes.clone(),
            seen: ct.changes.epoch(),
            rebinding: None,
            vacant: false,
            stamped: ct.stamped.clone(),
            unseen: false,
            known: ct.known.clone(),
            row: None,
            resolving: None,
            opening: None,
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    /// A write path's probe of `key` before the read that answers it (NoConflict on a UNIQUE
    /// index, KeyProbe before an insert's rowid check; A3.12, A4.9): its newest version decides. Another
    /// transaction's open version waits for it as a write does, on the same edge; one committed
    /// after the snapshot is `WriteWriteConflict`, which an autocommit statement restarts on. Done,
    /// the newest version is one the snapshot sees, or there is none, so the read that follows
    /// answers as it would have.
    #[turso_macros::atoll_entry]
    pub fn probe(&mut self, key: GuardKey<'_>) -> IOResultOr<()> {
        use crate::storage::atoll::txn::versioned::Probe;
        self.usable();
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        loop {
            let cursor = done!(self.resolve());
            let prefix = cursor.guard_prefix(&key)?;
            let w = match cursor.probe(&prefix)? {
                IOResult::Done(Probe::Present | Probe::Absent) => {
                    Write::Done(crate::storage::atoll::btree::Outcome::Unchanged)
                }
                IOResult::Done(Probe::Wait(blocker)) => Write::Wait(blocker),
                IOResult::Done(Probe::Conflict) => Write::Conflict,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            match self.waiting.judge(&self.db, &self.waits, w)? {
                Judged::Done => return Ok(IOResult::Done(())),
                Judged::Park(c) => return Ok(self.waiting.wait(&self.waits, c)?),
                Judged::Again => {}
            }
        }
    }

    /// From now on the cursor rewinds and moves on over the versions its statement does not see
    /// that hold their key, as rows (FkUnseen): the scan of R10's guard of a child table with no
    /// index on its foreign key, which compares each as the probe compares a row.
    #[turso_macros::atoll_entry]
    pub fn walk_unseen(&mut self) {
        self.unseen = true;
        if let Some(open) = self.open.as_mut() {
            open.walk_unseen();
        }
    }

    /// The guard's judgment of the unseen version the cursor rests on, which its scan matched
    /// (FkGuardRow): a wait on its writer as a write's, on the same edge, while that is open, and
    /// `WriteWriteConflict` once it has committed.
    #[turso_macros::atoll_entry]
    pub fn unseen_hit(&mut self) -> IOResultOr<()> {
        self.usable();
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        loop {
            let w = match done!(self.resolve()).unseen_hit()? {
                Guard::Clear => Write::Done(crate::storage::atoll::btree::Outcome::Unchanged),
                Guard::Wait(blocker) => Write::Wait(blocker),
                Guard::Conflict => Write::Conflict,
            };
            match self.waiting.judge(&self.db, &self.waits, w)? {
                Judged::Done => return Ok(IOResult::Done(())),
                Judged::Park(c) => return Ok(self.waiting.wait(&self.waits, c)?),
                Judged::Again => {}
            }
        }
    }

    /// Brings the cursor up to what other statements of its connection changed in its tree since
    /// it took its position (DESIGN.md §17, §3): its entry read again at its key, or, the tree
    /// replaced by a Clear, the cursor bound to the new tree at that key. Step it until done.
    fn caught_up(&mut self) -> Result<IOResult<()>> {
        let epoch = self.changes.epoch();
        if epoch == self.seen {
            return Ok(IOResult::Done(()));
        }
        let behind = self.changes.since(self.id, self.seen);
        if behind == Behind::Cleared && self.rebinding.is_none() {
            if let Some(open) = self.open.take() {
                self.rebinding = Some(open.position());
            }
        }
        if let Some(key) = self.rebinding.clone() {
            if let Some(key) = key {
                // Resolved without letting go: the rebinding is this catch-up's, under way.
                done!(done!(self.resolved()).refresh(Some(key)));
            }
            self.rebinding = None;
        } else if behind != Behind::Nothing {
            if let Some(open) = self.open.as_mut() {
                done!(open.refresh(None));
            }
        }
        self.seen = epoch;
        self.has_record = !self.is_empty();
        Ok(IOResult::Done(()))
    }

    /// Lets go of the cursor's tree if a Clear replaced it since the cursor took its position, so
    /// it resolves the catalog row again and reaches the new tree (DESIGN.md §17, §3). Every step
    /// that reaches the tree resolves it: one that positions the cursor itself, or writes at a key,
    /// needs nothing more, and one that moves from its position caught up first, rebinding it at
    /// its key.
    fn bound(&mut self) {
        if self.changes.since(self.id, self.seen) != Behind::Cleared {
            return;
        }
        if let Some(open) = self.open.take() {
            self.null_row = open.get_null_flag();
        }
        self.rebinding = None;
        self.has_record = false;
        self.seen = self.changes.epoch();
    }

    /// The cursor's position is current with its connection's changes.
    fn positioned(&mut self) {
        self.seen = self.changes.epoch();
    }

    /// The cursor, whose writes meeting another transaction's open version fail at once with
    /// `WriteWriteConflict`, changing nothing, as MVCC's do: an index method's, written against
    /// that contract (DESIGN.md §17, §3.1).
    pub(in crate::storage::atoll) fn no_wait(mut self) -> Self {
        self.no_wait = true;
        self
    }

    /// The tree's rowid counter, seeded from the tree's largest key once it is (DESIGN.md §17,
    /// §8). Step it until done.
    fn counter(&mut self) -> Result<IOResult<&Counter>> {
        done!(self.resolve());
        let cursor = self.open.as_mut().expect("resolved");
        cursor.hold_counter(self.db.rowids());
        let seed = self.seeding.get_or_insert_with(Seed::default);
        done!(cursor.counter_held().seed(cursor.plain_tree(), seed));
        self.seeding = None;
        Ok(IOResult::Done(
            self.open.as_ref().expect("resolved").counter_held(),
        ))
    }

    /// NewRowid (DESIGN.md §17, §8, R30): one above the largest rowid the tree's counter drew,
    /// seeded or raised past; `None` once `i64::MAX` is taken, when the caller probes random
    /// rowids.
    #[turso_macros::atoll_entry]
    pub fn new_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        self.usable();
        Ok(IOResult::Done(done!(self.counter()).draw()))
    }

    /// Takes the cursor up again for its statement's next open of its tree (DESIGN.md §17), in
    /// `ct`'s transaction, an open under way until [`finish_open`](CursorTrait::finish_open) is
    /// done: its tree cursor is kept, at no row, its buffers and its last look with it, where the
    /// catalog row the statement reads is the one it was over; else it resolves afresh. `false`
    /// for a cursor of another connection, which the caller drops.
    #[turso_macros::atoll_entry]
    pub fn reopened(
        &mut self,
        ct: &ConnTxn,
        pager: &Arc<Pager>,
        index: Option<Arc<IndexInfo>>,
    ) -> bool {
        if !Arc::ptr_eq(&self.cell, &ct.cell) || !Arc::ptr_eq(&self.db, &ct.db) {
            return false;
        }
        // The connection's pager, nearly always the one it kept: no count taken then.
        if !Arc::ptr_eq(&self.pager, pager) {
            self.pager = pager.clone();
        }
        self.index = index;
        self.null_row = false;
        self.has_record = false;
        self.no_wait = false;
        self.seen = self.changes.epoch();
        self.opening = Some(Opening::Reopen);
        true
    }

    /// A new cursor's open under way, which [`finish_open`](CursorTrait::finish_open) finishes.
    pub(in crate::storage::atoll) fn opening_fresh(mut self) -> Self {
        self.opening = Some(Opening::Fresh);
        self
    }

    /// Steps the open under way, its catalog read returning the IO it waits for in a cooperative
    /// frame (ASYNC1 D2e (ii)). The cursor is bound to the tree its id names at the call that
    /// finishes the open, as a b-tree cursor is to its root, so a Destroy later in its statement
    /// leaves it on the tree it opened (R22); one opened on a tree its statement had destroyed is
    /// vacant: it reads as empty, as SQLite's cursor on a freed root does, and a write through it
    /// fails. Done or failed, the open and its read are let go; only a return for IO keeps them.
    fn open_step(&mut self, opening: Opening) -> Result<IOResult<()>> {
        let out = self.open_inner(opening);
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.opening = None;
            self.resolving = None;
        }
        out
    }

    fn open_inner(&mut self, opening: Opening) -> Result<IOResult<()>> {
        let taken_up = {
            let mut guard = self.cell.enter()?;
            let txn = guard.as_mut().ok_or_else(outside)?;
            if self.changes.destroyed_in(self.id, txn.command()) {
                drop(guard);
                self.open = None;
                self.vacant = true;
                return Ok(IOResult::Done(()));
            }
            match opening {
                Opening::Fresh => None,
                Opening::Reopen => {
                    let changes = self.db.catalog().changes();
                    let row = match self.row.filter(|r| r.holds(txn, changes)) {
                        Some(row) => row,
                        None => {
                            done!(row_in_kept(
                                &self.db,
                                self.cell.conn(),
                                &self.known,
                                self.id,
                                txn,
                                &mut self.resolving
                            ))
                            .0
                        }
                    };
                    Some((row, created_now(txn, row.entry)))
                }
            }
        };
        let Some((row, plain)) = taken_up else {
            done!(self.resolve());
            return Ok(IOResult::Done(()));
        };
        self.row = Some(row);
        self.vacant = false;
        match self.open.as_mut() {
            Some(cursor) if cursor.entry() == row.entry => {
                cursor.reuse(self.index.clone(), plain);
            }
            _ => {
                self.open = None;
                done!(self.resolved());
            }
        }
        Ok(IOResult::Done(()))
    }

    /// The tree's catalog row as the transaction's current statement reads it, and a cursor over
    /// it, writing plainly where the current statement created the tree: the tree a Clear made, if
    /// one replaced the tree since the cursor took its position.
    fn resolve(&mut self) -> Result<IOResult<&mut AtollCursor>> {
        self.bound();
        self.resolved()
    }

    /// The tree's catalog row as the transaction's current statement reads it, whether that
    /// statement created the tree, and the leaf a cursor of the connection last stood at in it.
    /// Done or failed, its read is let go; only a return for IO keeps it.
    fn entry_now(&mut self) -> Result<IOResult<(Entry, bool, Near)>> {
        let out = self.entry_inner();
        FacadeRead::settle(&mut self.resolving, out)
    }

    fn entry_inner(&mut self) -> Result<IOResult<(Entry, bool, Near)>> {
        let mut guard = self.cell.enter()?;
        let txn = guard.as_mut().ok_or_else(outside)?;
        let (row, near) = done!(row_in_kept(
            &self.db,
            self.cell.conn(),
            &self.known,
            self.id,
            txn,
            &mut self.resolving
        ));
        Ok(IOResult::Done((
            row.entry,
            created_now(txn, row.entry),
            near,
        )))
    }

    /// The cursor over the tree it holds, or over the tree its catalog row names if it holds none.
    fn resolved(&mut self) -> Result<IOResult<&mut AtollCursor>> {
        if self.vacant {
            return Err(LimboError::InternalError(format!(
                "a write into tree {}, which its statement destroyed",
                self.id
            )));
        }
        if self.open.is_none() {
            let (entry, plain, near) = done!(self.entry_now());
            let mut cursor = Box::new(AtollCursor::new(
                self.db.clone(),
                self.cell.clone(),
                self.id,
                entry,
                self.index.clone(),
                key::ENCODING,
            ));
            if plain {
                cursor.write_plainly();
            }
            cursor.start_near(near);
            if self.unseen {
                cursor.walk_unseen();
            }
            cursor.set_null_flag(self.null_row);
            self.open = Some(cursor);
        }
        // A read is kept beside the tree cursor only while a Reopen's open is under way, its old
        // tree cursor held for the reuse test alone.
        debug_assert!(
            self.resolving.is_none() || self.opening.is_some(),
            "a cursor's resolution kept beside its tree cursor"
        );
        Ok(IOResult::Done(self.open.as_deref_mut().expect("resolved")))
    }

    /// Whether its resolution's read is kept, and the frame and metadata page its owner keeps,
    /// for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn resolving_for_test(
        &self,
    ) -> Option<(Option<u32>, Option<crate::storage::atoll::meta::MetaId>)> {
        self.resolving.as_ref().map(|r| r.keeps_for_test())
    }

    /// Whether its tree cursor's operation holds a memory-shortage tenure, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn shortage_queued_for_test(&self) -> bool {
        self.open
            .as_deref()
            .is_some_and(AtollCursor::shortage_queued)
    }

    /// Its tree cursor's address, if it holds one, for a test to see it reused.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn tree_cursor_for_test(&self) -> Option<usize> {
        self.open
            .as_deref()
            .map(|c| c as *const AtollCursor as usize)
    }

    /// No open under way: a cursor's tree is reached only once its open is done.
    fn usable(&self) {
        debug_assert!(
            self.opening.is_none(),
            "tree {}'s cursor used while its open is under way",
            self.id
        );
    }

    /// A write's step: done, a park on its blocker, or the write judged again after registering.
    fn wrote(&mut self, delete: bool, key: Option<&BTreeKey<'_>>) -> IOResultOr<()> {
        loop {
            let cursor = done!(self.resolve());
            let w = match (delete, key) {
                (true, _) => cursor.delete()?,
                (false, Some(key)) => cursor.insert(key)?,
                (false, None) => unreachable!("an insert of no key"),
            };
            let w = match w {
                IOResult::Done(w) => w,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            if let (true, Write::Wait(_)) = (self.no_wait, w) {
                self.open.as_mut().expect("resolved").abandon_write();
                return Err(Box::new(LimboError::WriteWriteConflict));
            }
            match self.waiting.judge(&self.db, &self.waits, w)? {
                Judged::Done => {
                    if self.id == SCHEMA_ID {
                        if let Some(txn) = self.cell.enter()?.as_mut() {
                            txn.wrote_schema();
                        }
                    }
                    // The cursor's own change: it stands where its write left it.
                    self.changes.wrote(self.id);
                    self.positioned();
                    if let Some(open) = self.open.as_ref() {
                        let root = open.root();
                        let db = &self.db;
                        self.stamped
                            .lock()
                            .entry(root)
                            .or_insert_with(|| db.stamps().stamp(root));
                    }
                    return Ok(IOResult::Done(()));
                }
                Judged::Park(c) => return Ok(self.waiting.wait(&self.waits, c)?),
                Judged::Again => {}
            }
        }
    }

    /// R10's foreign-key guard, after its statement wrote a child row or removed a parent key
    /// (FkGuard): every key of the tree that `key` names is judged at each of its versions the
    /// snapshot cannot see, newest first, and the first that `guarded` names waits for its
    /// writer as a write does, on the same edge, while that writer is open, and is
    /// `WriteWriteConflict` once it has committed. A tree its statement destroyed has no keys.
    #[turso_macros::atoll_entry]
    pub fn guard(&mut self, key: GuardKey<'_>, guarded: Guarded) -> IOResultOr<()> {
        self.usable();
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        loop {
            let cursor = done!(self.resolve());
            let prefix = cursor.guard_prefix(&key)?;
            let w = match cursor.guard(&prefix, guarded)? {
                IOResult::Done(Guard::Clear) => {
                    Write::Done(crate::storage::atoll::btree::Outcome::Unchanged)
                }
                IOResult::Done(Guard::Wait(blocker)) => Write::Wait(blocker),
                IOResult::Done(Guard::Conflict) => Write::Conflict,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            match self.waiting.judge(&self.db, &self.waits, w)? {
                Judged::Done => return Ok(IOResult::Done(())),
                Judged::Park(c) => return Ok(self.waiting.wait(&self.waits, c)?),
                Judged::Again => {}
            }
        }
    }

    fn moved(&mut self, out: Result<IOResult<()>>) -> IOResultOr<()> {
        let out = out?;
        if let IOResult::Done(()) = out {
            self.has_record = !self.is_empty();
            self.positioned();
        }
        Ok(out)
    }

    fn refused(&self, what: &str) -> Box<LimboError> {
        Box::new(LimboError::InternalError(format!(
            "{what} on the Atoll tree {} goes through its opcode's Atoll arm, not the cursor",
            self.id
        )))
    }
}

impl CursorTrait for SqlCursor {
    /// Parked at its statement's reset for the next execution's open of its tree
    /// ([`SqlCursor::reopened`]): what it waited on let go, the leaf it stood at left to the
    /// connection's other cursors over the tree, as a drop leaves it.
    #[turso_macros::atoll_entry]
    fn park(&mut self) -> bool {
        self.waiting = Waiting::default();
        self.seeding = None;
        self.counting = None;
        self.rebinding = None;
        // An open or a resolution under way ends with its statement, its read's owner letting go
        // of its tenure and its keeps, whether or not a tree cursor was made.
        self.opening = None;
        self.resolving = None;
        if let Some(cursor) = self.open.as_mut() {
            // Its operation ends with the statement: its memory-shortage tenure is let go.
            cursor.end_operation();
        }
        if self.unseen {
            // A walk over unseen versions is its statement's alone: the next opens afresh.
            return false;
        }
        if let Some(cursor) = &self.open {
            self.known.lock().entry(self.id).or_default().near = cursor.near();
        }
        true
    }

    #[turso_macros::atoll_entry]
    fn opening(&self) -> bool {
        self.opening.is_some()
    }

    #[turso_macros::atoll_entry]
    fn finish_open(&mut self) -> IOResultOr<()> {
        let Some(opening) = self.opening else {
            return Ok(IOResult::Done(()));
        };
        let out = self.open_step(opening);
        #[cfg(test)]
        if let Ok(stepped) = &out {
            OPEN_STEPS.with(|s| {
                s.borrow_mut()
                    .push((self.id, matches!(stepped, IOResult::IO(_))))
            });
        }
        Ok(out?)
    }

    #[turso_macros::atoll_entry]
    fn last(&mut self) -> IOResultOr<()> {
        self.usable();
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        let out = done!(self.resolve()).last();
        self.moved(out)
    }

    /// A forward step with nothing of its connection's changed since the cursor took its
    /// position goes straight to its open cursor: nothing to catch up with or resolve again.
    #[inline]
    #[turso_macros::atoll_entry]
    fn next_row(&mut self) -> crate::storage::btree::CursorStep {
        self.usable();
        use crate::storage::btree::CursorStep;
        if !self.vacant && self.rebinding.is_none() && self.changes.epoch() == self.seen {
            if let Some(open) = self.open.as_mut() {
                if open.get_null_flag() {
                    open.set_null_flag(false);
                    return CursorStep::Empty;
                }
                return match open.next() {
                    Ok(IOResult::Done(())) => {
                        self.has_record = !open.is_empty();
                        if self.has_record {
                            CursorStep::Row
                        } else {
                            CursorStep::Empty
                        }
                    }
                    Ok(IOResult::IO(io)) => CursorStep::IO(io),
                    Err(e) => CursorStep::Error(Box::new(e)),
                };
            }
        }
        let was_null_row = self.get_null_flag();
        self.set_null_flag(false);
        if was_null_row {
            return CursorStep::Empty;
        }
        match self.next() {
            Ok(IOResult::IO(io)) => CursorStep::IO(io),
            Err(err) => CursorStep::Error(err),
            Ok(IOResult::Done(())) if self.is_empty() => CursorStep::Empty,
            Ok(IOResult::Done(())) => CursorStep::Row,
        }
    }

    #[turso_macros::atoll_entry]
    fn next(&mut self) -> IOResultOr<()> {
        self.usable();
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        crate::return_if_io!(self.caught_up());
        let out = done!(self.resolve()).next();
        self.moved(out)
    }

    #[turso_macros::atoll_entry]
    fn prev(&mut self) -> IOResultOr<()> {
        self.usable();
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        crate::return_if_io!(self.caught_up());
        let out = done!(self.resolve()).prev();
        self.moved(out)
    }

    #[inline]
    #[turso_macros::atoll_entry]
    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
        self.usable();
        if self.changes.epoch() != self.seen {
            crate::return_if_io!(self.caught_up());
        }
        match self.open.as_mut() {
            Some(c) => Ok(c.rowid()?),
            None => Ok(IOResult::Done(None)),
        }
    }

    /// The row's bytes as its cursor holds them while nothing of its connection changed since it
    /// took its position: a Column read builds no record.
    #[inline]
    #[turso_macros::atoll_entry]
    fn record_payload(&mut self) -> IOResultOr<Option<&[u8]>> {
        self.usable();
        if self.changes.epoch() == self.seen
            && self.open.as_ref().is_some_and(|o| o.lends_payload())
        {
            return Ok(IOResult::Done(self.open.as_ref().and_then(|o| o.payload())));
        }
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        Ok(match self.record()? {
            IOResult::Done(record) => IOResult::Done(record.map(ImmutableRecord::get_payload)),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    #[inline]
    #[turso_macros::atoll_entry]
    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
        self.usable();
        if self.changes.epoch() != self.seen {
            crate::return_if_io!(self.caught_up());
        }
        match self.open.as_mut() {
            Some(c) => Ok(c.record()?),
            None => Ok(IOResult::Done(None)),
        }
    }

    #[turso_macros::atoll_entry]
    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        self.usable();
        if self.vacant {
            return Ok(IOResult::Done(SeekResult::NotFound));
        }
        let out = done!(self.resolve()).seek(key, op)?;
        if let IOResult::Done(_) = out {
            self.has_record = !self.is_empty();
            self.positioned();
        }
        Ok(out)
    }

    #[turso_macros::atoll_entry]
    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        let record = ImmutableRecord::from_registers(registers, registers.len())?;
        self.seek(SeekKey::IndexKey(record.as_record_ref()), op)
    }

    #[turso_macros::atoll_entry]
    fn insert(&mut self, key: &BTreeKey) -> IOResultOr<()> {
        self.usable();
        // A row's rowid, drawn or explicit, raises its table's counter past it: seeded first, so
        // it never hides the tree's larger keys from the seed (R30).
        if key.maybe_rowid().is_some() {
            crate::return_if_io!(self.counter());
        }
        crate::return_if_io!(self.wrote(false, Some(key)));
        if let Some(rowid) = key.maybe_rowid() {
            self.open
                .as_ref()
                .expect("written through its cursor")
                .counter_held()
                .inserted(rowid);
        }
        Ok(IOResult::Done(()))
    }

    #[turso_macros::atoll_entry]
    fn delete(&mut self) -> IOResultOr<()> {
        self.usable();
        crate::return_if_io!(self.caught_up());
        self.wrote(true, None)
    }

    #[turso_macros::atoll_entry]
    fn set_null_flag(&mut self, flag: bool) {
        self.usable();
        match self.open.as_mut() {
            Some(c) => c.set_null_flag(flag),
            None => self.null_row = flag,
        }
    }

    #[turso_macros::atoll_entry]
    fn get_null_flag(&self) -> bool {
        match self.open.as_ref() {
            Some(c) => c.get_null_flag(),
            None => self.null_row,
        }
    }

    #[turso_macros::atoll_entry]
    fn exists(&mut self, key: &Value) -> IOResultOr<bool> {
        let Value::Numeric(crate::numeric::Numeric::Integer(rowid)) = key else {
            return Err(Box::new(LimboError::InternalError(format!(
                "a rowid lookup of {key:?} in the Atoll tree {}",
                self.id
            ))));
        };
        let found = crate::return_if_io!(
            self.seek(SeekKey::TableRowId(*rowid), SeekOp::GE { eq_only: true })
        );
        Ok(IOResult::Done(matches!(found, SeekResult::Found)))
    }

    #[turso_macros::atoll_entry]
    fn clear_btree(&mut self) -> IOResultOr<u64> {
        Err(self.refused("clearing a tree"))
    }

    #[turso_macros::atoll_entry]
    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        Err(self.refused("destroying a tree"))
    }

    #[turso_macros::atoll_entry]
    fn count(&mut self) -> IOResultOr<usize> {
        self.usable();
        // A move IO cut short is stepped again before any other: the cursor resumes it, so a
        // rewind after it would land where it went, not at the first row.
        loop {
            match self.counting {
                None => {
                    crate::return_if_io!(self.rewind());
                    self.counting = Some(Counting::At(0));
                }
                Some(Counting::At(n)) => {
                    if self.is_empty() {
                        self.counting = None;
                        return Ok(IOResult::Done(n));
                    }
                    self.counting = Some(Counting::Moving(n + 1));
                }
                Some(Counting::Moving(n)) => {
                    crate::return_if_io!(self.next());
                    self.counting = Some(Counting::At(n));
                }
            }
        }
    }

    #[turso_macros::atoll_entry]
    fn is_empty(&self) -> bool {
        self.open.as_deref().is_none_or(AtollCursor::is_empty)
    }

    #[turso_macros::atoll_entry]
    fn root_page(&self) -> i64 {
        self.id as i64
    }

    #[turso_macros::atoll_entry]
    fn rewind(&mut self) -> IOResultOr<()> {
        self.usable();
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        let out = done!(self.resolve()).rewind();
        self.moved(out)
    }

    #[turso_macros::atoll_entry]
    fn has_record(&self) -> bool {
        self.has_record
    }

    #[turso_macros::atoll_entry]
    fn set_has_record(&mut self, has_record: bool) {
        self.has_record = has_record;
    }

    #[turso_macros::atoll_entry]
    fn index_info(&self) -> Option<&Arc<IndexInfo>> {
        self.index.as_ref()
    }

    #[turso_macros::atoll_entry]
    fn seek_end(&mut self) -> IOResultOr<()> {
        Ok(IOResult::Done(()))
    }

    #[turso_macros::atoll_entry]
    fn seek_to_last(&mut self) -> IOResultOr<()> {
        self.last()
    }

    #[turso_macros::atoll_entry]
    fn invalidate_record(&mut self) {
        self.usable();
        if let Some(c) = self.open.as_mut() {
            c.invalidate_record();
        }
    }

    #[turso_macros::atoll_entry]
    fn has_rowid(&self) -> bool {
        self.index.as_ref().is_none_or(|i| i.has_rowid)
    }

    #[turso_macros::atoll_entry]
    fn get_pager(&self) -> Arc<Pager> {
        self.pager.clone()
    }

    #[turso_macros::atoll_entry]
    fn get_skip_advance(&self) -> bool {
        false
    }

    /// The tree was cleared under the cursor: it resolves its catalog row again at its next step,
    /// reading the new tree.
    #[turso_macros::atoll_entry]
    fn invalidate_btree_cache(&mut self) {
        self.open = None;
        self.has_record = false;
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
