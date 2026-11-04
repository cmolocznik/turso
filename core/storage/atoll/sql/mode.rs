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

use super::catalog::{self, Catalog, Entry, Layout, System, SCHEMA_ID};
use super::cursor::{AtollCursor, TxnCell};
use super::header::{Field, Header};
use super::key;
use super::lifecycle::Destroy;
use super::rowid::{Counter, Seed};
use super::AtollDb;
use crate::io::FileSyncType;
use crate::io::{Completion, OpenFlags, IO};
use crate::storage::atoll::btree::{Mode, Outcome};
use crate::storage::atoll::buffer::MIN_BUDGET;
use crate::storage::atoll::control::{self, Found};
use crate::storage::atoll::db::AtollConfig;
use crate::storage::atoll::drive;
use crate::storage::atoll::page::PageId;
use crate::storage::atoll::txn::versioned::{Get, Write, WriteOp};
use crate::storage::atoll::txn::{Abort, Blocker, Commit, Isolation, Statement, Taken, Txn};
use crate::storage::btree::{BTreeKey, CursorTrait};
use crate::storage::database::DatabaseStorage;
use crate::storage::database::IOContext;
use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{
    IOCompletions, IOResult, IOResultOr, ImmutableRecord, IndexInfo, SeekKey, SeekOp, SeekResult,
};
use crate::vdbe::Register;
use crate::{Buffer, CompletionError, LimboError, Result, Value};
use std::io::ErrorKind;

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
/// option, Turso's default page cache, 2000 KiB (`cache_size` −2000), raised to the smallest pool
/// Atoll takes (R26).
pub fn config_for(option: Option<&AtollConfig>) -> AtollConfig {
    option
        .cloned()
        .unwrap_or_else(|| AtollConfig::with_budget(MIN_BUDGET.max(2_048_000)))
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
    fn read_header(&self, _c: Completion) -> Result<Completion> {
        Err(refused())
    }

    fn read_page(
        &self,
        _page_idx: usize,
        _io_ctx: &IOContext,
        _c: Completion,
    ) -> Result<Completion> {
        Err(refused())
    }

    fn write_page(
        &self,
        _page_idx: usize,
        _buffer: Arc<Buffer>,
        _io_ctx: &IOContext,
        _c: Completion,
    ) -> Result<Completion> {
        Err(refused())
    }

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

    fn sync(&self, _c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        Err(refused())
    }

    fn size(&self) -> Result<u64> {
        Ok(PAGE as u64)
    }

    fn truncate(&self, _len: usize, _c: Completion) -> Result<Completion> {
        Err(refused())
    }
}

/// The page size Turso sees: SQL trees' node size (R7).
pub const PAGE: usize = 4096;

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

/// A write behind another transaction: the completion it waits on, registered on the blocker's
/// end before the write was judged again. Reused while unfinished (DESIGN.md §17, §3.1).
#[derive(Default)]
struct Waiting {
    on: Option<(Blocker, Completion)>,
}

impl Waiting {
    /// What a write that came back `w` comes to: done, a park on its blocker's end, or judged
    /// again after registering on it.
    fn judge(&mut self, db: &AtollDb, w: Write) -> Result<Judged> {
        match w {
            Write::Done(_) => {
                self.on = None;
                Ok(Judged::Done)
            }
            Write::Conflict => {
                self.on = None;
                Err(LimboError::WriteWriteConflict)
            }
            Write::Wait(blocker) => {
                if let Some((on, c)) = &self.on {
                    if *on == blocker && !c.finished() {
                        return Ok(Judged::Park(c.clone()));
                    }
                }
                let c = match self.on.take() {
                    Some((_, c)) if !c.finished() => c,
                    _ => Completion::new_wait(),
                };
                db.atoll().transactions().wait_end(blocker, &c);
                self.on = Some((blocker, c));
                Ok(Judged::Again)
            }
        }
    }
}

/// Where a write, a begin or a gate take behind another transaction goes (R34): with `parks`,
/// onto `c`, which the other's end completes, as the PostgreSQL frontend's unbounded waits do; else
/// back as `Busy`, which Turso's busy handler retries in place, at the same instruction, with
/// SQLite's backoff until the busy timeout, as SQLite waits on a lock.
fn park<T>(parks: &AtomicBool, c: Completion) -> Result<IOResult<T>> {
    if parks.load(Ordering::Relaxed) {
        Ok(IOResult::IO(IOCompletions(c)))
    } else {
        Err(LimboError::Busy)
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

/// A connection's Atoll transaction: the `Txn` its cursors read and write in, and the header
/// fields it wrote, by statement.
pub struct ConnTxn {
    db: Arc<AtollDb>,
    pub cell: TxnCell,
    /// Each header field written: the statement that wrote it and its word.
    written: Mutex<Vec<(Statement, Field, u32)>>,
    /// A header write under way, kept across IO and waits.
    setting: Mutex<Option<(Field, WriteOp, Waiting)>>,
    /// A gate take waited on, kept so a take parks only on a completion registered before it.
    taking: Mutex<Option<Completion>>,
    /// Whether waits park, or come back `Busy` to be retried in place (R34).
    parks: Arc<AtomicBool>,
}

impl ConnTxn {
    pub fn new(db: Arc<AtollDb>) -> Self {
        Self {
            db,
            cell: Arc::new(Mutex::new(None)),
            written: Mutex::new(Vec::new()),
            setting: Mutex::new(None),
            taking: Mutex::new(None),
            parks: Arc::new(AtomicBool::new(false)),
        }
    }

    /// From now on waits park until what is in the way ends, for the PostgreSQL frontend, whose
    /// waits are unbounded (R2, R34).
    pub fn park_waits(&self) {
        self.parks.store(true, Ordering::Relaxed);
    }

    pub fn db(&self) -> &Arc<AtollDb> {
        &self.db
    }

    /// Whether a transaction is open.
    pub fn is_open(&self) -> bool {
        self.cell.lock().is_some()
    }

    /// Whether the open transaction has written.
    pub fn wrote(&self) -> bool {
        self.cell.lock().as_ref().is_some_and(Txn::wrote)
    }

    /// Begins the transaction a Transaction opcode needs, if none is open, and for a write takes
    /// the write gate: shared, or exclusive for a statement writing the schema (DESIGN.md §17,
    /// §6). Behind another's take it comes back with the completion to wait on; stepping it again
    /// goes on. A take the gate lost to a victim's clearing is `Busy`.
    pub fn begin(&self, wants: Begin) -> Result<IOResult<()>> {
        let txns = self.db.atoll().transactions();
        if let Begin::Write { .. } = wants {
            // A writing statement nudges the checkpointer first, holding no latch: commits are
            // not its only trigger, so long transactions are checkpointed too (DESIGN.md §17,
            // §11).
            nudge_checkpoint(&self.db)?;
        }
        let mut guard = self.cell.lock();
        if guard.is_none() {
            let begun = match wants {
                Begin::Read => txns.begin(Isolation::Snapshot)?,
                // Its snapshot once it holds the gate: an exclusive taker's after the drain.
                Begin::Write { .. } => txns.begin_slot_in(Isolation::Snapshot, 0)?,
            };
            match begun {
                IOResult::Done(txn) => *guard = Some(txn),
                // No slot is free (R13).
                IOResult::IO(IOCompletions(c)) => return park(&self.parks, c),
            }
        }
        let txn = guard.as_mut().expect("a transaction");
        if let Begin::Write { schema } = wants {
            let mut taking = self.taking.lock();
            let c = match taking.take() {
                Some(c) if !c.finished() => c,
                _ => Completion::new_wait(),
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
                Taken::Done => {}
                Taken::Lost => {
                    drop(taking);
                    self.end_if_fresh(&mut guard);
                    return Err(LimboError::Busy);
                }
                Taken::Behind { .. } | Taken::Holders(_) => {
                    // A pending exclusive take stays pending meanwhile (R15).
                    *taking = Some(c.clone());
                    return park(&self.parks, c);
                }
            }
        }
        let txn = guard.as_mut().expect("a transaction");
        if !txn.snapped() {
            txns.take_snapshot(txn);
        }
        Ok(IOResult::Done(()))
    }

    /// Ends a transaction a failed begin left with nothing done: no snapshot, or no write.
    fn end_if_fresh(&self, guard: &mut Option<Txn>) {
        if guard
            .as_ref()
            .is_some_and(|t| !t.wrote() && t.command() == 0)
        {
            let txn = guard.take().expect("a transaction");
            self.db.atoll().transactions().abort(txn);
        }
    }

    /// Gives up a pending exclusive take, as a statement that stops waiting for it does (R15).
    pub fn give_up(&self) {
        if let Some(txn) = self.cell.lock().as_mut() {
            self.db.atoll().transactions().give_up(txn);
        }
        *self.taking.lock() = None;
    }

    /// Starts the Atoll statement of a statement that writes: returns the number to roll back
    /// to if it fails (R18).
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
    pub fn rollback_statement(&self, n: Statement) -> Result<()> {
        let mut guard = self.cell.lock();
        let Some(txn) = guard.as_mut() else {
            return Ok(());
        };
        let mut op = crate::storage::atoll::txn::rollback::Rollback::after(n);
        let (db, io) = (&self.db, self.db.io());
        drive(io, || {
            db.atoll()
                .transactions()
                .rollback_to(db.versions(), txn, &mut op)
        })?;
        self.written.lock().retain(|(s, _, _)| *s <= n);
        *self.setting.lock() = None;
        Ok(())
    }

    /// The ending of the open transaction: its commit, or its rollback. `None` with none open.
    pub fn end(&self, commit: bool) -> Option<Ending> {
        let txn = self.cell.lock().take()?;
        let written = std::mem::take(&mut *self.written.lock());
        *self.setting.lock() = None;
        *self.taking.lock() = None;
        Some(if commit {
            Ending::commit(self.db.clone(), txn, written)
        } else {
            Ending::rollback(self.db.clone(), txn)
        })
    }

    /// The schema cookie: at the transaction's view, or committed outside one.
    pub fn schema_cookie(&self) -> Result<u32> {
        self.field(Field::SchemaCookie)
    }

    fn field(&self, field: Field) -> Result<u32> {
        let mut guard = self.cell.lock();
        let Some(txn) = guard.as_mut() else {
            return Ok(self.db.header().committed(field));
        };
        let (db, io) = (&self.db, self.db.io());
        let mut get = Header::get_op(field);
        drive(io, || {
            db.header().get(
                db.atoll().transactions(),
                db.versions(),
                txn,
                field,
                &mut get,
            )
        })
    }

    /// The header as SQLite lays it out: every field's word at the transaction's view, or
    /// committed outside one.
    pub fn header(&self) -> Result<DatabaseHeader> {
        let mut words = [0u32; 7];
        for field in Field::ALL {
            words[field as usize - 1] = self.field(field)?;
        }
        Ok(header_of(&words))
    }

    /// Applies `f` to the header and writes every field it changed, in the open transaction's
    /// current statement. Each field written is recorded for the commit to publish. A write behind
    /// another transaction comes back to wait; applying `f` again goes on, the fields already
    /// written reading as written.
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
            match waiting.judge(&self.db, w)? {
                Judged::Done => {
                    if field == Field::SchemaCookie {
                        txn.wrote_schema();
                    }
                    self.written.lock().push((txn.command(), field, value));
                    *setting = None;
                }
                Judged::Park(c) => return park(&self.parks, c),
                Judged::Again => {}
            }
        }
    }

    /// Destroy, or ClearBtree with `clear` (DESIGN.md §17, §5): tree `id`'s catalog row deleted,
    /// or pointed at a new, empty tree, and its old root listed as dropped, in the current
    /// statement. Step it with the same `op` until done; a Clear returns the rows the tree held at
    /// the view, which its statement counts as changed.
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
            match op.waiting.judge(&self.db, w)? {
                Judged::Done => {
                    txn.wrote_schema();
                    return Ok(IOResult::Done(op.counted.unwrap_or(0)));
                }
                Judged::Park(c) => return park(&self.parks, c),
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
            drive(db.io(), || {
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
        drive(&io, || cursor.rewind())?;
        let mut n = 0;
        while !cursor.is_empty() {
            n += 1;
            drive(&io, || cursor.next())?;
        }
        Ok(n)
    }

    /// CreateBtree (DESIGN.md §17, §5): draws a tree id, creates its tree in the current
    /// statement and writes its catalog row, an index's (or a WITHOUT ROWID table's, R29) or a
    /// table's. Step it with the same `op` until done; returns the tree id, which Turso calls the
    /// root page.
    pub fn create_btree(&self, op: &mut Option<CreateOp>, index: bool) -> Result<IOResult<u64>> {
        let op = op.get_or_insert_with(|| CreateOp {
            id: self.db.draw_tree_id(),
            root: None,
            write: None,
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
}

/// Steps the checkpointer, which does whatever is due: a pass once its share of the log budget has
/// been appended, and moving the redo point after one. Its IO is not waited for: the pass goes on
/// at the next step, whoever takes it.
fn nudge_checkpoint(db: &AtollDb) -> Result<()> {
    db.atoll().checkpointer().step().map(|_| ())
}

/// CreateBtree's state across IO.
pub struct CreateOp {
    id: u64,
    root: Option<PageId>,
    write: Option<WriteOp>,
}

fn outside() -> LimboError {
    LimboError::InternalError("an Atoll read or write outside any transaction".to_string())
}

/// A transaction's commit or rollback, then the drain of the trees it abandoned. Step it until
/// done. Never abandoned: dropped unfinished, it is driven to its end where it drops (R19).
pub struct Ending {
    db: Arc<AtollDb>,
    step: Step,
}

enum Step {
    Commit(Commit),
    Rollback(Abort),
    Drain,
    /// A full checkpoint, by its ticket.
    Checkpoint(u64),
    Done,
}

impl Ending {
    fn commit(db: Arc<AtollDb>, txn: Txn, written: Vec<(Statement, Field, u32)>) -> Self {
        let published = db.clone();
        let commit = Commit::new(txn).on_published(move |ts| {
            for (_, field, value) in written {
                published.header().publish(field, ts, value);
            }
        });
        Self {
            db,
            step: Step::Commit(commit),
        }
    }

    fn rollback(db: Arc<AtollDb>, txn: Txn) -> Self {
        if !txn.wrote() {
            db.atoll().transactions().abort(txn);
            return Self {
                db,
                step: Step::Drain,
            };
        }
        Self {
            db,
            step: Step::Rollback(Abort::new(txn)),
        }
    }

    pub fn step(&mut self) -> Result<IOResult<()>> {
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
                    // until as much again is written.
                    let checkpointer = self.db.atoll().checkpointer();
                    self.step = if checkpointer.lag() > 2 * checkpointer.budget() {
                        Step::Checkpoint(checkpointer.request_full())
                    } else {
                        nudge_checkpoint(&self.db)?;
                        Step::Done
                    };
                }
                Step::Checkpoint(ticket) => {
                    done!(self.db.atoll().checkpointer().step_full(*ticket));
                    self.step = Step::Done;
                }
                Step::Done => return Ok(IOResult::Done(())),
            }
        }
    }

    /// Drives the ending to its end, for the synchronous sites that end a transaction.
    pub fn finish(mut self) -> Result<()> {
        let io = self.db.io().clone();
        drive(&io, || self.step())
    }
}

impl std::fmt::Debug for Ending {
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

/// A Turso cursor over one SQL tree, which resolves its catalog row at its first step (R22):
/// OpenRead and OpenWrite build cursors with no IO to step.
pub struct SqlCursor {
    db: Arc<AtollDb>,
    cell: TxnCell,
    pager: Arc<Pager>,
    id: u64,
    index: Option<Arc<IndexInfo>>,
    open: Option<AtollCursor>,
    /// The null flag set before the cursor resolved.
    null_row: bool,
    has_record: bool,
    waiting: Waiting,
    /// Whether its writes' waits park (R34).
    parks: Arc<AtomicBool>,
    /// The rowid counter's seed under way.
    seeding: Option<Seed>,
    /// A count under way.
    counting: Option<Counting>,
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
            parks: ct.parks.clone(),
            seeding: None,
            counting: None,
        }
    }

    /// The tree's rowid counter, seeded from the tree's largest key once it is (DESIGN.md §17,
    /// §8). Step it until done.
    fn counter(&mut self) -> Result<IOResult<Arc<Counter>>> {
        self.resolve()?;
        let cursor = self.open.as_ref().expect("resolved");
        let counter = self.db.rowids().counter(cursor.root());
        let seed = self.seeding.get_or_insert_with(Seed::default);
        done!(counter.seed(cursor.plain_tree(), seed));
        self.seeding = None;
        Ok(IOResult::Done(counter))
    }

    /// NewRowid (DESIGN.md §17, §8, R30): one above the largest rowid the tree's counter drew,
    /// seeded or raised past; `None` once `i64::MAX` is taken, when the caller probes random
    /// rowids.
    pub fn new_rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        Ok(IOResult::Done(done!(self.counter()).draw()))
    }

    /// The tree's catalog row as the transaction's current statement reads it, and a cursor over
    /// it, writing plainly where the current statement created the tree.
    fn resolve(&mut self) -> Result<&mut AtollCursor> {
        if self.open.is_none() {
            let (entry, plain) = {
                let mut guard = self.cell.lock();
                let txn = guard.as_mut().ok_or_else(outside)?;
                let mut get = Get::new(&catalog::key(self.id));
                let db = &self.db;
                let entry = drive(db.io(), || {
                    db.catalog()
                        .get(db.atoll().transactions(), db.versions(), txn, &mut get)
                })?
                .ok_or_else(|| {
                    LimboError::InternalError(format!("no tree {} in the catalog", self.id))
                })?;
                let plain = txn
                    .created()
                    .iter()
                    .any(|c| c.root == entry.root && c.statement == txn.command());
                (entry, plain)
            };
            let mut cursor = AtollCursor::new(
                self.db.clone(),
                self.cell.clone(),
                self.id,
                entry,
                self.index.clone(),
                key::ENCODING,
            );
            if plain {
                cursor.write_plainly();
            }
            cursor.set_null_flag(self.null_row);
            self.open = Some(cursor);
        }
        Ok(self.open.as_mut().expect("resolved"))
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
            match self.waiting.judge(&self.db, w)? {
                Judged::Done => {
                    if self.id == SCHEMA_ID {
                        if let Some(txn) = self.cell.lock().as_mut() {
                            txn.wrote_schema();
                        }
                    }
                    return Ok(IOResult::Done(()));
                }
                Judged::Park(c) => return Ok(park(&self.parks, c)?),
                Judged::Again => {}
            }
        }
    }

    fn moved(&mut self, out: Result<IOResult<()>>) -> IOResultOr<()> {
        let out = out?;
        if let IOResult::Done(()) = out {
            self.has_record = !self.is_empty();
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
    fn last(&mut self) -> IOResultOr<()> {
        let out = self.resolve()?.last();
        self.moved(out)
    }

    fn next(&mut self) -> IOResultOr<()> {
        let out = self.resolve()?.next();
        self.moved(out)
    }

    fn prev(&mut self) -> IOResultOr<()> {
        let out = self.resolve()?.prev();
        self.moved(out)
    }

    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
        match self.open.as_mut() {
            Some(c) => Ok(c.rowid()?),
            None => Ok(IOResult::Done(None)),
        }
    }

    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
        match self.open.as_mut() {
            Some(c) => Ok(c.record()?),
            None => Ok(IOResult::Done(None)),
        }
    }

    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        let out = self.resolve()?.seek(key, op)?;
        if let IOResult::Done(_) = out {
            self.has_record = !self.is_empty();
        }
        Ok(out)
    }

    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        let record = ImmutableRecord::from_registers(registers, registers.len())?;
        self.seek(SeekKey::IndexKey(record.as_record_ref()), op)
    }

    fn insert(&mut self, key: &BTreeKey) -> IOResultOr<()> {
        // A row's rowid, drawn or explicit, raises its table's counter past it: seeded first, so
        // it never hides the tree's larger keys from the seed (R30).
        let counter = match key.maybe_rowid() {
            Some(_) => Some(crate::return_if_io!(self.counter())),
            None => None,
        };
        crate::return_if_io!(self.wrote(false, Some(key)));
        if let (Some(counter), Some(rowid)) = (counter, key.maybe_rowid()) {
            counter.inserted(rowid);
        }
        Ok(IOResult::Done(()))
    }

    fn delete(&mut self) -> IOResultOr<()> {
        self.wrote(true, None)
    }

    fn set_null_flag(&mut self, flag: bool) {
        match self.open.as_mut() {
            Some(c) => c.set_null_flag(flag),
            None => self.null_row = flag,
        }
    }

    fn get_null_flag(&self) -> bool {
        match self.open.as_ref() {
            Some(c) => c.get_null_flag(),
            None => self.null_row,
        }
    }

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

    fn clear_btree(&mut self) -> IOResultOr<u64> {
        Err(self.refused("clearing a tree"))
    }

    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        Err(self.refused("destroying a tree"))
    }

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

    fn is_empty(&self) -> bool {
        self.open.as_ref().is_none_or(AtollCursor::is_empty)
    }

    fn root_page(&self) -> i64 {
        self.id as i64
    }

    fn rewind(&mut self) -> IOResultOr<()> {
        let out = self.resolve()?.rewind();
        self.moved(out)
    }

    fn has_record(&self) -> bool {
        self.has_record
    }

    fn set_has_record(&mut self, has_record: bool) {
        self.has_record = has_record;
    }

    fn index_info(&self) -> Option<&Arc<IndexInfo>> {
        self.index.as_ref()
    }

    fn seek_end(&mut self) -> IOResultOr<()> {
        Ok(IOResult::Done(()))
    }

    fn seek_to_last(&mut self) -> IOResultOr<()> {
        self.last()
    }

    fn invalidate_record(&mut self) {
        if let Some(c) = self.open.as_mut() {
            c.invalidate_record();
        }
    }

    fn has_rowid(&self) -> bool {
        self.index.as_ref().is_none_or(|i| i.has_rowid)
    }

    fn get_pager(&self) -> Arc<Pager> {
        self.pager.clone()
    }

    fn get_skip_advance(&self) -> bool {
        false
    }

    /// The tree was cleared under the cursor: it resolves its catalog row again at its next step,
    /// reading the new tree.
    fn invalidate_btree_cache(&mut self) {
        self.open = None;
        self.has_record = false;
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
