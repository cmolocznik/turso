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
//! Reads (the header, the catalog) are driven to their end here: a read waits only on IO, never
//! on another transaction. Writes wait on other transactions, so they come back to the VDBE.

use super::catalog::{self, Catalog, Entry, Layout, System};
use super::cursor::{AtollCursor, TxnCell};
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
use crate::storage::atoll::{drive, drive_for};
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
        let mut guard = self.cell.lock();
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
        let at = self.cell.lock().as_ref().map_or(0, Txn::command);
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
        self.cell.lock().is_some()
    }

    /// Whether the open transaction has written.
    #[turso_macros::atoll_entry]
    pub fn wrote(&self) -> bool {
        self.cell.lock().as_ref().is_some_and(Txn::wrote)
    }

    /// A cursor over the tree whose id Turso holds as the root page `root`, opened as OpenRead and
    /// OpenWrite open one: an index's or a WITHOUT ROWID table's with `index`, a rowid table's
    /// without.
    #[turso_macros::atoll_entry]
    pub fn open_cursor(
        &self,
        pager: Arc<Pager>,
        root: i64,
        index: Option<Arc<IndexInfo>>,
    ) -> Result<SqlCursor> {
        SqlCursor::new(self, pager, tree_id(root)?, index).opened()
    }

    /// [`open_cursor`](Self::open_cursor) for an index method's backing tree, whose writes fail
    /// at once on another writer's key instead of waiting for it ([`SqlCursor::no_wait`]).
    #[turso_macros::atoll_entry]
    pub fn open_cursor_no_wait(
        &self,
        pager: Arc<Pager>,
        root: i64,
        index: Option<Arc<IndexInfo>>,
    ) -> Result<SqlCursor> {
        Ok(SqlCursor::new(self, pager, tree_id(root)?, index)
            .opened()?
            .no_wait())
    }

    /// The snapshot timestamp of the open transaction's current statement, once it has one.
    #[turso_macros::atoll_entry]
    pub fn snapshot(&self) -> Option<u64> {
        self.cell
            .lock()
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
        Ok(self.begin_reading(wants, &[])?.map(|_| ()))
    }

    /// [`begin`](Self::begin) for a schema writer that reads rows of the trees `reads` names: in a
    /// transaction that already has a snapshot, a commit that raised one's stamp past it fails the
    /// take with `BusySnapshot`, checked before it marks itself pending and again once it holds
    /// the gate (DESIGN.md §17, §3, defect 4). Its build or rewrite would read the tree at that
    /// snapshot and miss the rows committed since, as no write conflict tells. Done, it says
    /// whether this call found no transaction open, so began it: the Transaction opcode's one
    /// look at the connection's transaction.
    #[turso_macros::atoll_entry]
    pub fn begin_reading(&self, wants: Begin, reads: &[u64]) -> Result<IOResult<bool>> {
        let txns = self.db.atoll().transactions();
        if let Begin::Write { .. } = wants {
            // A writing statement steps the checkpointer first, holding no latch, while a
            // checkpoint is due or under way: commits are not its only trigger, so long
            // transactions are checkpointed too (DESIGN.md §17, §11). Its IO is not waited for:
            // the checkpoint goes on at the next step, whoever takes it.
            self.db.atoll().checkpointer().step_due(txns).map(|_| ())?;
        }
        let mut guard = self.cell.lock();
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
                && self.read_since_snapshot(txn, reads)?
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
                    if checks && self.read_since_snapshot(txn, reads)? {
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
    fn read_since_snapshot(&self, txn: &mut Txn, reads: &[u64]) -> Result<bool> {
        let db = &self.db;
        for id in reads {
            let mut get = Get::new(&catalog::key(*id));
            let entry = drive_for(self.cell.conn(), db.io(), || {
                db.catalog()
                    .get(db.atoll().transactions(), db.versions(), txn, &mut get)
            })?;
            if entry.is_some_and(|e| db.stamps().written_since(e.root, txn.snapshot())) {
                return Ok(true);
            }
        }
        Ok(false)
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
        if let Some(txn) = self.cell.lock().as_mut() {
            self.db.atoll().transactions().give_up(txn);
        }
        *self.taking.lock() = None;
        let _ = self.begin_wait.lock().over();
    }

    /// Starts the Atoll statement of a statement that writes: returns the number to roll back
    /// to if it fails (R18).
    #[turso_macros::atoll_entry]
    pub fn start_statement(&self) -> Result<Statement> {
        let mut guard = self.cell.lock();
        let txn = guard.as_mut().ok_or_else(outside)?;
        let n = txn.command();
        self.db.atoll().transactions().statement(txn)?;
        Ok(n)
    }

    /// Rolls the open transaction back to where it stood after statement `n`, driven to its end
    /// here, at the synchronous sites that roll a statement back (R19), and forgets the header
    /// fields written since.
    #[turso_macros::atoll_entry]
    pub fn rollback_statement(&self, n: Statement) -> Result<()> {
        let mut guard = self.cell.lock();
        let Some(txn) = guard.as_mut() else {
            return Ok(());
        };
        let mut op = crate::storage::atoll::txn::rollback::Rollback::after(n);
        let (db, io) = (&self.db, self.db.io());
        drive_for(self.cell.conn(), io, || {
            db.atoll()
                .transactions()
                .rollback_to(db.versions(), txn, &mut op)
        })?;
        self.written.lock().retain(|(s, _, _)| *s <= n);
        *self.setting.lock() = None;
        self.changes.destroyed(|list| list.retain(|(_, s)| *s <= n));
        self.changes.rolled_back();
        // It may have undone a catalog or header write the transaction read.
        self.db.catalog().note_change();
        self.db.header().note_change();
        Ok(())
    }

    /// The ending of the open transaction: its commit, or its rollback. `None` with none open, or
    /// while a schema reparse holds it.
    #[turso_macros::atoll_entry]
    pub fn end(&self, commit: bool) -> Option<Ending> {
        if self.pinned.load(Ordering::Acquire) {
            return None;
        }
        self.savepoints.lock().clear();
        let txn = self.cell.lock().take()?;
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
        let conn = self.cell.conn().clone();
        crate::storage::atoll::buffer::cancel_shortage(self.db.atoll().pool(), &conn);
        Some(if commit {
            Ending::commit(self.db.clone(), conn, txn, written, stamped)
        } else {
            self.changes.rolled_back();
            Ending::rollback(self.db.clone(), conn, txn)
        })
    }

    /// The schema cookie: at the transaction's view, or committed outside one.
    #[turso_macros::atoll_entry]
    pub fn schema_cookie(&self) -> Result<u32> {
        self.field(Field::SchemaCookie)
    }

    fn field(&self, field: Field) -> Result<u32> {
        let mut guard = self.cell.lock();
        let Some(txn) = guard.as_mut() else {
            return Ok(self.db.header().committed(field));
        };
        let (db, io) = (&self.db, self.db.io());
        let (identity, changes) = ((txn.slot(), txn.start()), db.header().changes());
        if let Some((word, t, c)) = self.fields.lock()[field.index()] {
            if t == identity && c == changes {
                return Ok(word);
            }
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

    /// The header as SQLite lays it out: every field's word at the transaction's view, or
    /// committed outside one. Its database size and free-list count are the main pager's, which
    /// holds no pages: what reads them walks the pager's pages. `PRAGMA page_count` reads the page
    /// store ([`pages`](Self::pages)); `freelist_count` stays 0, which is so, Atoll keeping no free
    /// list in its files, a freed slot being erased and taken again.
    #[turso_macros::atoll_entry]
    pub fn header(&self) -> Result<DatabaseHeader> {
        let mut words = [0u32; 7];
        for field in Field::ALL {
            words[field as usize - 1] = self.field(field)?;
        }
        Ok(header_of(&words))
    }

    /// The pages the page store holds in use (DESIGN.md §17, §12), every class's in node-size
    /// units, rounded up, as `PRAGMA page_count` reports them; past `u32::MAX`, the most a header
    /// word holds.
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
            let mut guard = self.cell.lock();
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
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        });
        if clear && op.counted.is_none() {
            op.counted = Some(self.count_rows(id)?);
        }
        loop {
            let mut guard = self.cell.lock();
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
            let mut guard = self.cell.lock();
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
        let mut guard = self.cell.lock();
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
    /// Dropped last: the end of the frame its `Drop` opens (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

/// A full checkpoint (DESIGN.md §17, §11): one begun now, done, for `PRAGMA wal_checkpoint` and
/// `Connection::checkpoint`. Driven to its end here, as a WAL checkpoint of the whole log is.
#[turso_macros::atoll_entry]
pub fn full_checkpoint(db: &AtollDb) -> Result<()> {
    let checkpointer = db.atoll().checkpointer();
    let ticket = checkpointer.request_full();
    let txns = db.atoll().transactions();
    drive(db.io(), || checkpointer.step_full(ticket, txns))
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

fn outside() -> LimboError {
    LimboError::InternalError("an Atoll read or write outside any transaction".to_string())
}

/// A transaction's commit or rollback, then the drain of the trees it abandoned. Step it until
/// done. Never abandoned: dropped unfinished, it is driven to its end where it drops (R19).
pub struct Ending {
    db: Arc<AtollDb>,
    step: Step,
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
    Commit(Commit),
    Rollback(Abort),
    Drain,
    /// A full checkpoint, by its ticket.
    Checkpoint(u64),
    Done,
}

impl Ending {
    /// Commits `txn`: once it is published, its header fields `written` and the stamps of the
    /// trees it wrote are raised to its commit timestamp, before it releases the write gate.
    fn commit(
        db: Arc<AtollDb>,
        conn: Arc<crate::storage::atoll::buffer::Conn>,
        txn: Txn,
        written: Vec<(Statement, Field, u32)>,
        stamped: Vec<super::trees::Stamp>,
    ) -> Self {
        let published = db.clone();
        let commit = Commit::new(txn).on_published(move |ts| {
            for (_, field, value) in written {
                published.header().publish(field, ts, value);
            }
            super::trees::TreeStamps::raise(&stamped, ts);
        });
        Self {
            db,
            step: Step::Commit(commit),
            owner: crate::storage::atoll::buffer::Owner::of(conn),
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    fn rollback(
        db: Arc<AtollDb>,
        conn: Arc<crate::storage::atoll::buffer::Conn>,
        txn: Txn,
    ) -> Self {
        if !txn.wrote() {
            db.atoll().transactions().abort(txn);
            return Self {
                db,
                step: Step::Drain,
                owner: crate::storage::atoll::buffer::Owner::of(conn),
                _frame: crate::storage::atoll::unwind::FrameEnd::default(),
            };
        }
        Self {
            db,
            step: Step::Rollback(Abort::new(txn)),
            owner: crate::storage::atoll::buffer::Owner::of(conn),
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    #[turso_macros::atoll_entry]
    pub fn step(&mut self) -> Result<IOResult<()>> {
        crate::storage::atoll::buffer::Owner::call_in(self, |e| &mut e.owner, Self::step_owned)
    }

    /// Whether the ending holds a memory-shortage tenure, for a test.
    #[cfg(test)]
    #[turso_macros::atoll_entry]
    pub(crate) fn shortage_queued(&self) -> bool {
        self.owner.queued()
    }

    fn step_owned(&mut self) -> Result<IOResult<()>> {
        let txns = self.db.atoll().transactions();
        loop {
            match &mut self.step {
                Step::Commit(op) => {
                    done!(txns.commit(op));
                    self.db.abandoned(op.abandoned());
                    self.step = Step::Drain;
                }
                Step::Rollback(op) => {
                    done!(txns.rollback(self.db.versions(), op));
                    self.db.abandoned(op.abandoned());
                    self.step = Step::Drain;
                }
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
                        && checkpointer.lag_to(txns.held_from()) > over
                    {
                        Step::Checkpoint(checkpointer.request_full())
                    } else {
                        nudge_checkpoint(&self.db)?;
                        Step::Done
                    };
                }
                Step::Checkpoint(ticket) => {
                    let txns = self.db.atoll().transactions();
                    done!(self.db.atoll().checkpointer().step_full(*ticket, txns));
                    self.step = Step::Done;
                }
                Step::Done => return Ok(IOResult::Done(())),
            }
        }
    }

    /// Drives the ending to its end, for the synchronous sites that end a transaction.
    #[turso_macros::atoll_entry]
    pub fn finish(&mut self) -> Result<()> {
        let io = self.db.io().clone();
        drive(&io, || self.step())
    }
}

impl std::fmt::Debug for Ending {
    #[turso_macros::atoll_entry]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let step = match self.step {
            Step::Commit(_) => "committing",
            Step::Rollback(_) => "rolling back",
            Step::Drain => "draining",
            Step::Checkpoint(_) => "checkpointing",
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
        let io = self.db.io().clone();
        if let Err(e) = drive(&io, || self.step()) {
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
    /// Dropped last: the end of the frame its `Drop` opens (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
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
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        loop {
            let cursor = self.resolve()?;
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
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        loop {
            let w = match self.resolve()?.unseen_hit() {
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
                done!(self.resolved()?.refresh(Some(key)));
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
        self.resolve()?;
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
        Ok(IOResult::Done(done!(self.counter()).draw()))
    }

    /// Takes the cursor up again for its statement's next open of its tree (DESIGN.md §17), as
    /// [`opened`](Self::opened) opens one, in `ct`'s transaction: its tree cursor is kept, at no
    /// row, its buffers and its last look with it, where the catalog row the statement reads is
    /// the one it was over; else it resolves afresh. `false` for a cursor of another connection,
    /// which the caller drops.
    #[turso_macros::atoll_entry]
    pub fn reopened(
        &mut self,
        ct: &ConnTxn,
        pager: &Arc<Pager>,
        index: Option<Arc<IndexInfo>>,
    ) -> Result<bool> {
        if !Arc::ptr_eq(&self.cell, &ct.cell) || !Arc::ptr_eq(&self.db, &ct.db) {
            return Ok(false);
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
        let row = {
            let mut guard = self.cell.lock();
            let txn = guard.as_mut().ok_or_else(outside)?;
            if self.changes.destroyed_in(self.id, txn.command()) {
                None
            } else {
                let changes = self.db.catalog().changes();
                let row = match self.row.filter(|r| r.holds(txn, changes)) {
                    Some(row) => row,
                    None => row_in(&self.db, self.cell.conn(), &self.known, self.id, txn)?.0,
                };
                Some((row, created_now(txn, row.entry)))
            }
        };
        let Some((row, plain)) = row else {
            self.open = None;
            self.vacant = true;
            return Ok(true);
        };
        self.row = Some(row);
        self.vacant = false;
        let entry = row.entry;
        match self.open.as_mut() {
            Some(cursor) if cursor.entry() == entry => {
                cursor.reuse(self.index.clone(), plain);
            }
            _ => {
                self.open = None;
                self.resolved()?;
            }
        }
        Ok(true)
    }

    /// The cursor with its tree resolved, as OpenRead and OpenWrite open it: bound to the tree its
    /// id names when it opens, as a b-tree cursor is to its root, so a Destroy later in its
    /// statement leaves it on the tree it opened (R22). One opened on a tree its statement had
    /// destroyed is vacant: it reads as empty, as SQLite's cursor on a freed root does, and a
    /// write through it fails.
    pub(in crate::storage::atoll) fn opened(mut self) -> Result<Self> {
        let destroyed = {
            let guard = self.cell.lock();
            let txn = guard.as_ref().ok_or_else(outside)?;
            self.changes.destroyed_in(self.id, txn.command())
        };
        if destroyed {
            self.vacant = true;
            return Ok(self);
        }
        self.resolve()?;
        Ok(self)
    }

    /// The tree's catalog row as the transaction's current statement reads it, and a cursor over
    /// it, writing plainly where the current statement created the tree: the tree a Clear made, if
    /// one replaced the tree since the cursor took its position.
    fn resolve(&mut self) -> Result<&mut AtollCursor> {
        self.bound();
        self.resolved()
    }

    /// The tree's catalog row as the transaction's current statement reads it, whether that
    /// statement created the tree, and the leaf a cursor of the connection last stood at in it.
    fn entry_now(&self) -> Result<(Entry, bool, Near)> {
        let mut guard = self.cell.lock();
        let txn = guard.as_mut().ok_or_else(outside)?;
        let (row, near) = row_in(&self.db, self.cell.conn(), &self.known, self.id, txn)?;
        Ok((row.entry, created_now(txn, row.entry), near))
    }

    /// The cursor over the tree it holds, or over the tree its catalog row names if it holds none.
    fn resolved(&mut self) -> Result<&mut AtollCursor> {
        if self.vacant {
            return Err(LimboError::InternalError(format!(
                "a write into tree {}, which its statement destroyed",
                self.id
            )));
        }
        if self.open.is_none() {
            let (entry, plain, near) = self.entry_now()?;
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
        Ok(self.open.as_deref_mut().expect("resolved"))
    }

    /// A write's step: done, a park on its blocker, or the write judged again after registering.
    fn wrote(&mut self, delete: bool, key: Option<&BTreeKey<'_>>) -> IOResultOr<()> {
        loop {
            let cursor = self.resolve()?;
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
                        if let Some(txn) = self.cell.lock().as_mut() {
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
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        loop {
            let cursor = self.resolve()?;
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
    fn last(&mut self) -> IOResultOr<()> {
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        let out = self.resolve()?.last();
        self.moved(out)
    }

    /// A forward step with nothing of its connection's changed since the cursor took its
    /// position goes straight to its open cursor: nothing to catch up with or resolve again.
    #[inline]
    #[turso_macros::atoll_entry]
    fn next_row(&mut self) -> crate::storage::btree::CursorStep {
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
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        crate::return_if_io!(self.caught_up());
        let out = self.resolve()?.next();
        self.moved(out)
    }

    #[turso_macros::atoll_entry]
    fn prev(&mut self) -> IOResultOr<()> {
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        crate::return_if_io!(self.caught_up());
        let out = self.resolve()?.prev();
        self.moved(out)
    }

    #[inline]
    #[turso_macros::atoll_entry]
    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
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
        if self.vacant {
            return Ok(IOResult::Done(SeekResult::NotFound));
        }
        let out = self.resolve()?.seek(key, op)?;
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
        crate::return_if_io!(self.caught_up());
        self.wrote(true, None)
    }

    #[turso_macros::atoll_entry]
    fn set_null_flag(&mut self, flag: bool) {
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
        if self.vacant {
            return Ok(IOResult::Done(()));
        }
        let out = self.resolve()?.rewind();
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
