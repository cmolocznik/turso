//! An index over slash-separated paths that answers one question fast: which rows have a path
//! matching any of a set of glob patterns, and, optionally, a sequence number above a given one.
//!
//! > CREATE INDEX files_by_path ON files USING path_trie (path, seq)
//! > SELECT * FROM files WHERE path_sync(path, seq, '["src/**", "*.toml"]', 41)
//!
//! It is a prefix tree keyed by path component. A directory's entry holds the highest sequence
//! number of anything beneath it, so a query with a minimum sequence number never reads a
//! directory in which nothing is newer. CephFS's `rctime` and the birth transaction ids of btrfs
//! and ZFS serve incremental sends the same way.
//!
//! The tree is kept in two key-only B-trees:
//!
//! - `names`: (directory, name, is a directory, sequence number, child). It gives a directory's
//!   entries by name.
//! - `changes`: (directory, sequence number newest first, name, is a directory, child). It gives a
//!   directory's entries by how recently they changed.
//!
//! A file's child is the rowid of its table row. A directory's child is the directory's id; the
//! root's is 0.

use std::collections::VecDeque;

use globset::{GlobBuilder, GlobMatcher, GlobSet, GlobSetBuilder};
use turso_parser::ast::{self, SortOrder};

use crate::json::jsonb::ElementType;
use crate::json::{convert_dbtype_to_jsonb, Conv};
use crate::numeric::Numeric;
use crate::types::IOResultOr;
use crate::{
    index_method::{
        parse_patterns, BackingColumn, BackingIndex, BackingSchema, BackingStoreOp, BackingTable,
        IndexMethod, IndexMethodAttachment, IndexMethodConfiguration, IndexMethodContext,
        IndexMethodCursor, IndexMethodDefinition, IndexMethodMvccSupport,
        PATH_TRIE_INDEX_METHOD_NAME,
    },
    return_if_io,
    schema::Type,
    storage::btree::{BTreeKey, CursorTrait},
    sync::Arc,
    translate::collate::CollationSeq,
    types::{IOResult, ImmutableRecord, KeyInfo, SeekKey, SeekOp, SeekResult},
    vdbe::Register,
    LimboError, Result, Value, ValueRef,
};

/// The SQL function the index serves.
pub const PATH_SYNC_FUNCTION_NAME: &str = "path_sync";

/// `path_sync(path, seq, patterns, since)` for one row, without the index: whether `path` matches
/// any glob in the JSON array `patterns` and, unless `since` is NULL, `seq` is an integer above
/// `since`. The index returns exactly the rows for which this is true.
pub fn path_sync(path: &Value, seq: &Value, patterns: &Value, since: &Value) -> Result<Value> {
    let since = since_from_value(since)?;
    let Some(patterns) = PathPatterns::from_value(patterns)? else {
        return Ok(Value::Null);
    };
    let Some(path) = path.to_text() else {
        return Ok(Value::Null);
    };
    let new_enough = match since {
        None => true,
        Some(since) => sequence_number(seq) > since,
    };
    Ok(Value::from_i64(i64::from(
        new_enough && patterns.matches(path),
    )))
}

#[derive(Debug)]
pub struct PathTrieIndexMethod;

impl IndexMethod for PathTrieIndexMethod {
    fn attach(
        &self,
        configuration: &IndexMethodConfiguration,
    ) -> Result<Arc<dyn IndexMethodAttachment>> {
        let [path, seq] = configuration.columns.as_slice() else {
            return Err(LimboError::ParseError(format!(
                "{PATH_TRIE_INDEX_METHOD_NAME} index needs two columns, a path and a sequence \
                 number, but {} has {}",
                configuration.index_name,
                configuration.columns.len()
            )));
        };
        let pattern = format!(
            "SELECT * FROM {} WHERE {PATH_SYNC_FUNCTION_NAME}({}, {}, ?, ?)",
            configuration.table_name, path.name, seq.name
        );
        Ok(Arc::new(PathTrieAttachment {
            configuration: configuration.clone(),
            patterns: parse_patterns(&[&pattern])?,
        }))
    }
}

#[derive(Debug)]
pub struct PathTrieAttachment {
    configuration: IndexMethodConfiguration,
    patterns: Vec<ast::Select>,
}

impl IndexMethodAttachment for PathTrieAttachment {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a> {
        IndexMethodDefinition {
            method_name: PATH_TRIE_INDEX_METHOD_NAME,
            table_name: &self.configuration.table_name,
            index_name: &self.configuration.index_name,
            patterns: self.patterns.as_slice(),
            backing_btree: false,
            results_materialized: false,
            filtered_queries: false,
            mvcc_support: IndexMethodMvccSupport::TransactionalBackingStore,
            optimize_in_steps: false,
        }
    }

    fn init(&self) -> Result<Box<dyn IndexMethodCursor>> {
        Ok(Box::new(PathTrieCursor::new(self.configuration.clone())))
    }

    fn backing_schema(&self) -> BackingSchema {
        backing_schema(&self.configuration)
    }
}

/// One table of the method's own, which stays empty, and the two key-only B-trees on it. A
/// B-tree on the indexed table itself would be filled from that table's rows when the index is
/// made.
fn backing_schema(configuration: &IndexMethodConfiguration) -> BackingSchema {
    let table = BackingTable::new(
        format!("path_trie_{}", configuration.index_name),
        vec![
            BackingColumn::new("dir", Type::Integer),
            BackingColumn::new("name", Type::Text),
            BackingColumn::new("is_dir", Type::Integer),
            BackingColumn::new("seq", Type::Integer),
            BackingColumn::new("child", Type::Integer),
        ],
    );
    let five_keys = || vec![key_info(), key_info(), key_info(), key_info(), key_info()];
    let columns = |names: [&str; 5]| names.iter().map(|name| (*name).to_string()).collect();
    let names = BackingIndex::on_backing_table(
        &table,
        format!("{}_names", table.table_name()),
        columns(["dir", "name", "is_dir", "seq", "child"]),
        five_keys(),
    );
    let changes = BackingIndex::on_backing_table(
        &table,
        format!("{}_changes", table.table_name()),
        columns(["dir", "seq", "name", "is_dir", "child"]),
        five_keys(),
    );
    BackingSchema::new(vec![table], vec![names, changes])
}

fn key_info() -> KeyInfo {
    KeyInfo {
        collation: CollationSeq::BINARY,
        sort_order: SortOrder::Asc,
        nulls_order: None,
    }
}

pub struct PathTrieCursor {
    schema: BackingSchema,
    names: Option<Box<dyn CursorTrait>>,
    changes: Option<Box<dyn CursorTrait>>,
    /// An open of its cursors is under way: entered again, it goes on with the cursors it made.
    opening: bool,
    pending_store_ops: VecDeque<BackingStoreOp>,
    insert: InsertState,
    delete: DeleteState,
    writes: Writes,
    walk: Option<Walk>,
    /// `query_start` made `walk` and has not yet given its first answer.
    walk_starting: bool,
}

impl PathTrieCursor {
    pub fn new(configuration: IndexMethodConfiguration) -> Self {
        Self {
            schema: backing_schema(&configuration),
            names: None,
            changes: None,
            opening: false,
            pending_store_ops: VecDeque::new(),
            insert: InsertState::Idle,
            delete: DeleteState::Idle,
            writes: Writes::default(),
            walk: None,
            walk_starting: false,
        }
    }

    fn drive_pending_store_ops(&mut self) -> IOResultOr<()> {
        while let Some(op) = self.pending_store_ops.front_mut() {
            return_if_io!(op.step());
            self.pending_store_ops.pop_front();
        }
        Ok(IOResult::Done(()))
    }

    fn open_cursors(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let out = self.open_cursors_inner(context);
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.opening = false;
        }
        out
    }

    fn open_cursors_inner(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if !self.opening {
            self.names = Some(open_store_cursor(context, &self.schema.indexes[0])?);
            self.changes = Some(open_store_cursor(context, &self.schema.indexes[1])?);
            self.opening = true;
        }
        for cursor in [&mut self.names, &mut self.changes].into_iter().flatten() {
            return_if_io!(cursor.finish_open());
        }
        Ok(IOResult::Done(()))
    }

    fn advance_walk(&mut self) -> IOResultOr<bool> {
        let (Some(names), Some(changes), Some(walk)) =
            (&mut self.names, &mut self.changes, &mut self.walk)
        else {
            return Err(not_open());
        };
        walk.advance(names.as_mut(), changes.as_mut())
    }
}

fn open_store_cursor(
    context: &IndexMethodContext,
    index: &BackingIndex,
) -> Result<Box<dyn CursorTrait>> {
    context
        .backing_store(index)?
        .ok_or_else(|| {
            LimboError::InternalError(format!("backing store {} not found", index.name))
        })?
        .open_cursor()
}

fn not_open() -> Box<LimboError> {
    Box::new(LimboError::InternalError(
        "path_trie cursors must be opened".to_string(),
    ))
}

impl IndexMethodCursor for PathTrieCursor {
    fn create(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.pending_store_ops.is_empty() {
            self.pending_store_ops
                .push_back(context.create_backing_schema(&self.schema)?);
        }
        self.drive_pending_store_ops()
    }

    fn destroy(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.pending_store_ops.is_empty() {
            self.pending_store_ops
                .push_back(context.drop_backing_schema(&self.schema)?);
        }
        self.drive_pending_store_ops()
    }

    fn open_read(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.open_cursors(context)
    }

    fn open_write(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.open_cursors(context)
    }

    fn close(&mut self, _context: &IndexMethodContext) {
        self.names = None;
        self.changes = None;
        self.opening = false;
    }

    /// Adds the row's file entry, makes the directories on its path that do not exist yet, and
    /// raises the sequence number of every directory above it that holds a lower one.
    ///
    /// It reads first and writes after: each state below reads, and queues what to write in
    /// `writes`, and the last state writes the queue. Its first read is of the counter.
    fn insert(&mut self, values: &[Register]) -> IOResultOr<()> {
        let (Some(names), Some(changes)) = (&mut self.names, &mut self.changes) else {
            return Err(not_open());
        };
        loop {
            match &mut self.insert {
                InsertState::Idle => {
                    self.writes.clear();
                    let Some(row) = IndexedRow::from_registers(values)? else {
                        return Ok(IOResult::Done(()));
                    };
                    self.insert = InsertState::Counter {
                        finder: Finder::at(directory_prefix(COUNTER)?),
                        row,
                    };
                }
                InsertState::Counter { finder, .. } => {
                    let found = return_if_io!(finder.step(names.as_mut(), Tree::Names));
                    let InsertState::Counter { row, .. } =
                        std::mem::replace(&mut self.insert, InsertState::Idle)
                    else {
                        unreachable!("the state was matched above");
                    };
                    let counter = self.writes.take_counter(found)?;
                    self.insert = InsertState::below(row, 0, ROOT, counter)?;
                }
                InsertState::Descend { finder, .. } => {
                    let found = return_if_io!(finder.step(names.as_mut(), Tree::Names));
                    let InsertState::Descend {
                        row,
                        depth,
                        dir,
                        mut counter,
                        ..
                    } = std::mem::replace(&mut self.insert, InsertState::Idle)
                    else {
                        unreachable!("the state was matched above");
                    };
                    let name = &row.components[depth];
                    self.insert = match found.filter(|e| e.is_directory_named(dir, name)) {
                        Some(entry) => {
                            if entry.seq < row.seq {
                                self.writes.replace(&entry, &entry.with_seq(row.seq))?;
                            }
                            InsertState::below(row, depth + 1, entry.child, counter)?
                        }
                        // This directory and those below it do not exist: none of them can
                        // hold the file yet.
                        None => {
                            let mut parent = dir;
                            for name in &row.components[depth..row.components.len() - 1] {
                                self.writes.add(&Entry {
                                    dir: parent,
                                    name: name.clone(),
                                    is_dir: true,
                                    seq: row.seq,
                                    child: counter.next_id,
                                })?;
                                parent = counter.next_id;
                                counter.next_id += 1;
                            }
                            self.writes.add(&row.file_entry(parent))?;
                            self.writes.put_counter(counter)?;
                            InsertState::Write
                        }
                    };
                }
                InsertState::Leaf { finder, .. } => {
                    let found = return_if_io!(finder.step(names.as_mut(), Tree::Names));
                    let InsertState::Leaf {
                        row, dir, counter, ..
                    } = std::mem::replace(&mut self.insert, InsertState::Idle)
                    else {
                        unreachable!("the state was matched above");
                    };
                    if found.is_some_and(|e| e.is_file_named(dir, row.file_name())) {
                        return Err(Box::new(LimboError::Constraint(format!(
                            "{PATH_TRIE_INDEX_METHOD_NAME}: two rows have the path {}",
                            row.components.join("/")
                        ))));
                    }
                    self.writes.add(&row.file_entry(dir))?;
                    self.writes.put_counter(counter)?;
                    self.insert = InsertState::Write;
                }
                InsertState::Write => {
                    return_if_io!(self.writes.drain(names.as_mut(), changes.as_mut()));
                    self.insert = InsertState::Idle;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Removes the row's file entry, and every directory above it that is left empty. Like
    /// `insert`, it reads first and writes after.
    ///
    /// It leaves the sequence numbers of the directories above as they are: a number that is too
    /// high costs a query a visit, and never an answer.
    fn delete(&mut self, values: &[Register]) -> IOResultOr<()> {
        let (Some(names), Some(changes)) = (&mut self.names, &mut self.changes) else {
            return Err(not_open());
        };
        loop {
            match &mut self.delete {
                DeleteState::Idle => {
                    self.writes.clear();
                    let Some(row) = IndexedRow::from_registers(values)? else {
                        return Ok(IOResult::Done(()));
                    };
                    self.delete = DeleteState::Counter {
                        finder: Finder::at(directory_prefix(COUNTER)?),
                        row,
                    };
                }
                DeleteState::Counter { finder, .. } => {
                    let found = return_if_io!(finder.step(names.as_mut(), Tree::Names));
                    let DeleteState::Counter { row, .. } =
                        std::mem::replace(&mut self.delete, DeleteState::Idle)
                    else {
                        unreachable!("the state was matched above");
                    };
                    let counter = self.writes.take_counter(found)?;
                    self.writes.put_counter(counter)?;
                    self.delete = DeleteState::below(row, Vec::new(), ROOT)?;
                }
                DeleteState::Descend { finder, .. } => {
                    let found = return_if_io!(finder.step(names.as_mut(), Tree::Names));
                    let DeleteState::Descend {
                        row,
                        mut above,
                        dir,
                        ..
                    } = std::mem::replace(&mut self.delete, DeleteState::Idle)
                    else {
                        unreachable!("the state was matched above");
                    };
                    let name = &row.components[above.len()];
                    let Some(entry) = found.filter(|e| e.is_directory_named(dir, name)) else {
                        return Err(corrupt("a directory of an indexed path is missing"));
                    };
                    let child = entry.child;
                    above.push(entry);
                    self.delete = DeleteState::below(row, above, child)?;
                }
                DeleteState::Leaf { finder, .. } => {
                    let found = return_if_io!(finder.step(names.as_mut(), Tree::Names));
                    let DeleteState::Leaf {
                        row, above, dir, ..
                    } = std::mem::replace(&mut self.delete, DeleteState::Idle)
                    else {
                        unreachable!("the state was matched above");
                    };
                    let Some(entry) = found
                        .filter(|e| e.is_file_named(dir, row.file_name()) && e.child == row.rowid)
                    else {
                        return Err(corrupt("an indexed row has no file entry"));
                    };
                    self.writes.remove(&entry)?;
                    self.delete = DeleteState::Emptied {
                        finder: Finder::at(directory_prefix(dir)?),
                        above,
                        removed: entry,
                        saw_removed: false,
                    };
                }
                DeleteState::Emptied { finder, .. } => {
                    let found = return_if_io!(finder.step(names.as_mut(), Tree::Names));
                    let DeleteState::Emptied {
                        mut finder,
                        mut above,
                        removed,
                        saw_removed,
                    } = std::mem::replace(&mut self.delete, DeleteState::Idle)
                    else {
                        unreachable!("the state was matched above");
                    };
                    let other = found.filter(|e| e.dir == removed.dir);
                    self.delete = match other {
                        // This is the removed entry itself: look at the one after it.
                        Some(entry) if entry == removed && !saw_removed => {
                            finder.next();
                            DeleteState::Emptied {
                                finder,
                                above,
                                removed,
                                saw_removed: true,
                            }
                        }
                        Some(_) => DeleteState::Write,
                        None => match above.pop() {
                            // The directory is empty now: remove it from the one above.
                            Some(directory) => {
                                self.writes.remove(&directory)?;
                                DeleteState::Emptied {
                                    finder: Finder::at(directory_prefix(directory.dir)?),
                                    above,
                                    removed: directory,
                                    saw_removed: false,
                                }
                            }
                            // The root has no entry to remove.
                            None => DeleteState::Write,
                        },
                    };
                }
                DeleteState::Write => {
                    return_if_io!(self.writes.drain(names.as_mut(), changes.as_mut()));
                    self.delete = DeleteState::Idle;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// `values` holds the pattern's number (there is one pattern), then the JSON array of globs,
    /// then the minimum sequence number or NULL.
    fn query_start(&mut self, values: &[Register]) -> IOResultOr<bool> {
        if !self.walk_starting {
            let since = since_from_value(values[2].get_value())?;
            let patterns = PathPatterns::from_value(values[1].get_value())?;
            self.walk = Some(Walk::new(patterns, since));
            self.walk_starting = true;
        }
        let out = self.advance_walk();
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.walk_starting = false;
        }
        out
    }

    fn query_next(&mut self) -> IOResultOr<bool> {
        self.advance_walk()
    }

    fn query_rowid(&mut self) -> IOResultOr<Option<i64>> {
        Ok(IOResult::Done(self.walk.as_ref().and_then(|walk| walk.row)))
    }

    fn query_column(&mut self, _: usize) -> IOResultOr<Value> {
        Err(Box::new(LimboError::InternalError(
            "path_trie serves no column of its own".to_string(),
        )))
    }
}

/// The root directory's id.
const ROOT: i64 = 0;

/// The directory id under which `names` keeps one entry that is not a file: the counter. Every
/// insert and delete replaces it, so two transactions that write the index at once write the
/// same entry, and the engine refuses the second.
const COUNTER: i64 = -1;

/// The counter's entry holds `next_id` as its sequence number and `writes` as its child.
#[derive(Debug, Clone, Copy)]
struct Counter {
    /// The next unused directory id.
    next_id: i64,
    /// How many inserts and deletes the index has taken.
    writes: i64,
}

/// What a row gives the index: its path, split at each `/`, its sequence number and its rowid.
#[derive(Debug)]
struct IndexedRow {
    components: Vec<String>,
    seq: i64,
    rowid: i64,
}

impl IndexedRow {
    /// `values` holds the path, the sequence number and the rowid. A row whose path is not text
    /// is not indexed: no pattern matches it.
    fn from_registers(values: &[Register]) -> Result<Option<Self>> {
        let Some(path) = values[0].get_value().to_text() else {
            return Ok(None);
        };
        let Some(rowid) = values[2].get_value().as_int() else {
            return Err(LimboError::InternalError(
                "path_trie: the third value must be the rowid".to_string(),
            ));
        };
        Ok(Some(Self {
            components: path.split('/').map(str::to_owned).collect(),
            seq: sequence_number(values[1].get_value()),
            rowid,
        }))
    }

    fn file_name(&self) -> &str {
        self.components
            .last()
            .expect("splitting a path gives at least one component")
    }

    fn file_entry(&self, dir: i64) -> Entry {
        Entry {
            dir,
            name: self.file_name().to_owned(),
            is_dir: false,
            seq: self.seq,
            child: self.rowid,
        }
    }
}

/// A row's sequence number. One that is not an integer is below every `since`.
fn sequence_number(value: &Value) -> i64 {
    value.as_int().unwrap_or(i64::MIN)
}

fn since_from_value(value: &Value) -> Result<Option<i64>> {
    match value {
        Value::Null => Ok(None),
        _ => value.as_int().map(Some).ok_or_else(|| {
            LimboError::InvalidArgument(format!(
                "{PATH_SYNC_FUNCTION_NAME}: since must be an integer or NULL"
            ))
        }),
    }
}

#[derive(Debug)]
enum InsertState {
    Idle,
    /// Reading the counter, which every write replaces.
    Counter {
        row: IndexedRow,
        finder: Finder,
    },
    /// Looking for the directory `row.components[depth]` in `dir`.
    Descend {
        row: IndexedRow,
        depth: usize,
        dir: i64,
        counter: Counter,
        finder: Finder,
    },
    /// Every directory exists: looking for a file of the same name in `dir`.
    Leaf {
        row: IndexedRow,
        dir: i64,
        counter: Counter,
        finder: Finder,
    },
    Write,
}

impl InsertState {
    /// The state that goes on from directory `dir`, which holds `row.components[depth]`.
    fn below(row: IndexedRow, depth: usize, dir: i64, counter: Counter) -> Result<Self> {
        let name = &row.components[depth];
        if depth + 1 == row.components.len() {
            Ok(Self::Leaf {
                finder: Finder::at(entry_prefix(dir, name, false)?),
                row,
                dir,
                counter,
            })
        } else {
            Ok(Self::Descend {
                finder: Finder::at(entry_prefix(dir, name, true)?),
                row,
                depth,
                dir,
                counter,
            })
        }
    }
}

#[derive(Debug)]
enum DeleteState {
    Idle,
    /// Reading the counter, which every write replaces.
    Counter {
        row: IndexedRow,
        finder: Finder,
    },
    /// Looking for the directory `row.components[above.len()]` in `dir`. `above` holds the
    /// entries of the directories passed so far, the root's child first.
    Descend {
        row: IndexedRow,
        above: Vec<Entry>,
        dir: i64,
        finder: Finder,
    },
    /// Looking for the row's file entry in `dir`.
    Leaf {
        row: IndexedRow,
        above: Vec<Entry>,
        dir: i64,
        finder: Finder,
    },
    /// `removed` is queued for removal from its directory: looking for any other entry there.
    /// With none, the directory's own entry, the last of `above`, is removed too.
    Emptied {
        above: Vec<Entry>,
        removed: Entry,
        saw_removed: bool,
        finder: Finder,
    },
    Write,
}

impl DeleteState {
    /// The state that goes on from directory `dir`, which holds `row.components[above.len()]`.
    fn below(row: IndexedRow, above: Vec<Entry>, dir: i64) -> Result<Self> {
        let name = &row.components[above.len()];
        if above.len() + 1 == row.components.len() {
            Ok(Self::Leaf {
                finder: Finder::at(entry_prefix(dir, name, false)?),
                row,
                above,
                dir,
            })
        } else {
            Ok(Self::Descend {
                finder: Finder::at(entry_prefix(dir, name, true)?),
                row,
                above,
                dir,
            })
        }
    }
}

fn corrupt(what: &str) -> Box<LimboError> {
    Box::new(LimboError::Corrupt(format!(
        "{PATH_TRIE_INDEX_METHOD_NAME} index corrupted: {what}"
    )))
}

/// A query in progress: the directories still to read, and the one being read.
struct Walk {
    /// `None` when the patterns are NULL: no row matches.
    patterns: Option<PathPatterns>,
    since: Option<i64>,
    pending: Vec<Directory>,
    scan: Option<Scan>,
    /// The rowid of the row the query stands at.
    row: Option<i64>,
}

/// A directory a query reads, with its path (ending in `/`, empty for the root) and what the
/// patterns still need below it.
struct Directory {
    id: i64,
    path: String,
    states: PatternStates,
}

struct Scan {
    directory: Directory,
    kind: ScanKind,
    finder: Finder,
}

enum ScanKind {
    /// Every entry, by name.
    Names,
    /// Entries newer than `since`, newest first.
    Changes,
    /// Only these names: every pattern still alive needs one of them here.
    Literals { names: Vec<String>, next: usize },
}

impl Walk {
    fn new(patterns: Option<PathPatterns>, since: Option<i64>) -> Self {
        let pending = match &patterns {
            Some(patterns) => vec![Directory {
                id: ROOT,
                path: String::new(),
                states: patterns.start(),
            }],
            None => Vec::new(),
        };
        Self {
            patterns,
            since,
            pending,
            scan: None,
            row: None,
        }
    }

    /// Moves to the next matching row. Returns whether there is one.
    fn advance(
        &mut self,
        names: &mut dyn CursorTrait,
        changes: &mut dyn CursorTrait,
    ) -> IOResultOr<bool> {
        let Some(patterns) = &self.patterns else {
            return Ok(IOResult::Done(false));
        };
        loop {
            let Some(scan) = &mut self.scan else {
                let Some(directory) = self.pending.pop() else {
                    self.row = None;
                    return Ok(IOResult::Done(false));
                };
                self.scan = Some(Scan::of(directory, patterns, self.since)?);
                continue;
            };
            let entry = match scan.kind {
                ScanKind::Changes => return_if_io!(scan.finder.step(changes, Tree::Changes)),
                ScanKind::Names | ScanKind::Literals { .. } => {
                    return_if_io!(scan.finder.step(names, Tree::Names))
                }
            };
            // The read is done. Nothing below waits for IO, so each entry is handled once.
            let entry = entry.filter(|e| e.dir == scan.directory.id);
            let entry = match &mut scan.kind {
                ScanKind::Names => entry,
                // `changes` holds the newest first: the first entry that is not newer than
                // `since` ends the directory.
                ScanKind::Changes => entry.filter(|e| self.since.is_none_or(|s| e.seq > s)),
                ScanKind::Literals { names, next } => {
                    match entry.filter(|e| e.name == names[*next]) {
                        Some(entry) => Some(entry),
                        None if *next + 1 < names.len() => {
                            *next += 1;
                            scan.finder =
                                Finder::at(name_prefix(scan.directory.id, &names[*next])?);
                            continue;
                        }
                        None => None,
                    }
                }
            };
            let Some(entry) = entry else {
                self.scan = None;
                continue;
            };
            scan.finder.next();
            if self.since.is_some_and(|since| entry.seq <= since) {
                continue;
            }
            let next = patterns.advance(&scan.directory.states, &entry.name);
            if entry.is_dir {
                let states = patterns.unfinished(next);
                if !states.is_empty() {
                    self.pending.push(Directory {
                        id: entry.child,
                        path: format!("{}{}/", scan.directory.path, entry.name),
                        states,
                    });
                }
            } else if patterns.may_finish(&next) {
                let path = format!("{}{}", scan.directory.path, entry.name);
                if patterns.matches(&path) {
                    self.row = Some(entry.child);
                    return Ok(IOResult::Done(true));
                }
            }
        }
    }
}

impl Scan {
    fn of(directory: Directory, patterns: &PathPatterns, since: Option<i64>) -> Result<Self> {
        let (kind, key) = if since.is_some() {
            (ScanKind::Changes, directory_prefix(directory.id)?)
        } else if let Some(names) = patterns.only_literals(&directory.states) {
            let key = name_prefix(directory.id, &names[0])?;
            (ScanKind::Literals { names, next: 0 }, key)
        } else {
            (ScanKind::Names, directory_prefix(directory.id)?)
        };
        Ok(Self {
            directory,
            kind,
            finder: Finder::at(key),
        })
    }
}

/// One entry of a directory: a file or a directory in it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    dir: i64,
    name: String,
    is_dir: bool,
    /// A file's sequence number, or the highest one beneath a directory.
    seq: i64,
    /// A file's rowid, or a directory's id.
    child: i64,
}

impl Entry {
    fn is_directory_named(&self, dir: i64, name: &str) -> bool {
        self.dir == dir && self.is_dir && self.name == name
    }

    fn is_file_named(&self, dir: i64, name: &str) -> bool {
        self.dir == dir && !self.is_dir && self.name == name
    }

    fn with_seq(&self, seq: i64) -> Self {
        Self {
            seq,
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tree {
    Names,
    Changes,
}

fn names_key(entry: &Entry) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[
            Value::from_i64(entry.dir),
            Value::build_text(entry.name.clone()),
            Value::from_i64(i64::from(entry.is_dir)),
            Value::from_i64(entry.seq),
            Value::from_i64(entry.child),
        ],
        5,
    )
}

/// `changes` sorts a directory's entries newest first: it keys them by the bitwise complement of
/// the sequence number, which reverses the order of every `i64`.
fn changes_key(entry: &Entry) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[
            Value::from_i64(entry.dir),
            Value::from_i64(!entry.seq),
            Value::build_text(entry.name.clone()),
            Value::from_i64(i64::from(entry.is_dir)),
            Value::from_i64(entry.child),
        ],
        5,
    )
}

/// The key every entry of directory `dir` starts with, in either tree.
fn directory_prefix(dir: i64) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(&[Value::from_i64(dir)], 1)
}

/// The key the entries called `name` in `dir` start with, in `names`.
fn name_prefix(dir: i64, name: &str) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[Value::from_i64(dir), Value::build_text(name.to_owned())],
        2,
    )
}

/// The key the file or the directory called `name` in `dir` starts with, in `names`.
fn entry_prefix(dir: i64, name: &str, is_dir: bool) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[
            Value::from_i64(dir),
            Value::build_text(name.to_owned()),
            Value::from_i64(i64::from(is_dir)),
        ],
        3,
    )
}

fn parse_entry(tree: Tree, record: &ImmutableRecord) -> Result<Entry> {
    let integer = |column: usize| match record.get_value(column)? {
        ValueRef::Numeric(Numeric::Integer(value)) => Ok(value),
        _ => Err(LimboError::Corrupt(format!(
            "{PATH_TRIE_INDEX_METHOD_NAME} index corrupted: column {column} is not an integer"
        ))),
    };
    let text = |column: usize| match record.get_value(column)? {
        ValueRef::Text(value) => Ok(value.as_str().to_owned()),
        _ => Err(LimboError::Corrupt(format!(
            "{PATH_TRIE_INDEX_METHOD_NAME} index corrupted: column {column} is not text"
        ))),
    };
    match tree {
        Tree::Names => Ok(Entry {
            dir: integer(0)?,
            name: text(1)?,
            is_dir: integer(2)? != 0,
            seq: integer(3)?,
            child: integer(4)?,
        }),
        Tree::Changes => Ok(Entry {
            dir: integer(0)?,
            seq: !integer(1)?,
            name: text(2)?,
            is_dir: integer(3)? != 0,
            child: integer(4)?,
        }),
    }
}

/// Reads entries in key order, starting at the first one at or after a key.
#[derive(Debug)]
struct Finder {
    key: ImmutableRecord,
    phase: FinderPhase,
}

#[derive(Debug)]
enum FinderPhase {
    Seek,
    Next,
    Read,
}

impl Finder {
    fn at(key: ImmutableRecord) -> Self {
        Self {
            key,
            phase: FinderPhase::Seek,
        }
    }

    /// Gives the entry it stands at, or `None` past the last one. Called again without `next`,
    /// it gives the same entry.
    fn step(&mut self, cursor: &mut dyn CursorTrait, tree: Tree) -> IOResultOr<Option<Entry>> {
        loop {
            match self.phase {
                FinderPhase::Seek => {
                    let result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(self.key.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    ));
                    self.phase = match result {
                        SeekResult::Found => FinderPhase::Read,
                        SeekResult::TryAdvance => FinderPhase::Next,
                        SeekResult::NotFound => return Ok(IOResult::Done(None)),
                    };
                }
                FinderPhase::Next => {
                    return_if_io!(cursor.next());
                    self.phase = FinderPhase::Read;
                }
                FinderPhase::Read => {
                    let record = return_if_io!(cursor.record());
                    return Ok(IOResult::Done(match record {
                        Some(record) => Some(parse_entry(tree, record)?),
                        None => None,
                    }));
                }
            }
        }
    }

    /// Makes the next `step` give the entry after the one it stands at.
    fn next(&mut self) {
        self.phase = FinderPhase::Next;
    }
}

#[derive(Debug)]
enum WriteOp {
    Insert(Tree, ImmutableRecord),
    Delete(Tree, ImmutableRecord),
}

/// The writes an insert or a delete has decided on, done in order.
#[derive(Debug, Default)]
struct Writes {
    ops: VecDeque<WriteOp>,
    /// How far the first op has come.
    phase: WritePhase,
}

#[derive(Debug, Default, Clone, Copy)]
enum WritePhase {
    /// Moving the cursor to where the op's key belongs.
    #[default]
    Seek,
    /// A delete's seek stopped at the end of a leaf: the entry, when its key is the divider in
    /// the page above, is the next one.
    Advance,
    /// A delete checks that the cursor stands at the entry to remove.
    Check,
    Apply,
}

impl Writes {
    fn clear(&mut self) {
        self.ops.clear();
        self.phase = WritePhase::Seek;
    }

    fn push(&mut self, op: WriteOp) {
        self.ops.push_back(op);
    }

    fn add(&mut self, entry: &Entry) -> Result<()> {
        self.push(WriteOp::Insert(Tree::Names, names_key(entry)?));
        self.push(WriteOp::Insert(Tree::Changes, changes_key(entry)?));
        Ok(())
    }

    fn remove(&mut self, entry: &Entry) -> Result<()> {
        self.push(WriteOp::Delete(Tree::Names, names_key(entry)?));
        self.push(WriteOp::Delete(Tree::Changes, changes_key(entry)?));
        Ok(())
    }

    fn replace(&mut self, old: &Entry, new: &Entry) -> Result<()> {
        self.remove(old)?;
        self.add(new)
    }

    /// Queues the removal of the counter's entry, which `found` is unless the index has taken
    /// no write yet, and gives the counter it held.
    fn take_counter(&mut self, found: Option<Entry>) -> Result<Counter> {
        match found.filter(|entry| entry.dir == COUNTER) {
            Some(entry) => {
                self.push(WriteOp::Delete(Tree::Names, names_key(&entry)?));
                Ok(Counter {
                    next_id: entry.seq,
                    writes: entry.child,
                })
            }
            None => Ok(Counter {
                next_id: ROOT + 1,
                writes: 0,
            }),
        }
    }

    /// Queues the counter's entry as it is after this write.
    fn put_counter(&mut self, counter: Counter) -> Result<()> {
        let entry = Entry {
            dir: COUNTER,
            name: String::new(),
            is_dir: false,
            seq: counter.next_id,
            child: counter.writes + 1,
        };
        self.push(WriteOp::Insert(Tree::Names, names_key(&entry)?));
        Ok(())
    }

    fn drain(
        &mut self,
        names: &mut dyn CursorTrait,
        changes: &mut dyn CursorTrait,
    ) -> IOResultOr<()> {
        while let Some(op) = self.ops.front() {
            let (WriteOp::Insert(tree, key) | WriteOp::Delete(tree, key)) = op;
            let is_delete = matches!(op, WriteOp::Delete(..));
            let cursor: &mut dyn CursorTrait = match tree {
                Tree::Names => &mut *names,
                Tree::Changes => &mut *changes,
            };
            match self.phase {
                WritePhase::Seek => {
                    let result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(key.as_record_ref()),
                        SeekOp::GE { eq_only: true },
                    ));
                    self.phase = match (is_delete, result) {
                        // An insert goes where the seek stopped.
                        (false, _) => WritePhase::Apply,
                        (true, SeekResult::Found) => WritePhase::Check,
                        (true, SeekResult::TryAdvance) => WritePhase::Advance,
                        (true, SeekResult::NotFound) => {
                            return Err(corrupt("an entry to remove is missing"));
                        }
                    };
                }
                WritePhase::Advance => {
                    return_if_io!(cursor.next());
                    self.phase = WritePhase::Check;
                }
                WritePhase::Check => {
                    let record = return_if_io!(cursor.record());
                    if record.is_none_or(|record| record.get_payload() != key.get_payload()) {
                        return Err(corrupt("an entry to remove is missing"));
                    }
                    self.phase = WritePhase::Apply;
                }
                WritePhase::Apply => {
                    if is_delete {
                        return_if_io!(cursor.delete());
                    } else {
                        return_if_io!(cursor.insert(&BTreeKey::IndexKey(key.as_record_ref())));
                    }
                    self.phase = WritePhase::Seek;
                    self.ops.pop_front();
                }
            }
        }
        Ok(IOResult::Done(()))
    }
}

/// A set of glob patterns over slash-separated paths, with `globset`'s rules and a literal
/// separator: `*` and `?` do not match `/`, and `**` as a whole component matches any number
/// of directories.
pub struct PathPatterns {
    /// Decides whether a whole path matches.
    whole: GlobSet,
    /// Each pattern split at `/`, for deciding which directories are worth reading.
    components: Vec<Vec<Component>>,
}

/// What a pattern needs of one path component.
enum Component {
    Literal(String),
    Glob(GlobMatcher),
    /// `**`: any number of directories, none included.
    AnyDirectories,
    /// The pattern from here on is not split: anything below might match, and `whole` decides.
    Anything,
}

/// For each pattern still alive, how many of its components the path so far has matched: pairs
/// of pattern and component number, sorted, each once.
type PatternStates = Vec<(usize, usize)>;

impl PathPatterns {
    /// Reads a JSON array of glob strings. NULL gives `None`.
    fn from_value(value: &Value) -> Result<Option<Self>> {
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        let invalid = |what: &str| {
            LimboError::InvalidArgument(format!(
                "{PATH_SYNC_FUNCTION_NAME}: patterns must be a JSON array of strings: {what}"
            ))
        };
        let json =
            convert_dbtype_to_jsonb(value, Conv::Strict).map_err(|_| invalid("it is not JSON"))?;
        let mut position = json
            .array_iterator()
            .map_err(|_| invalid("it is not an array"))?;
        let mut globs = Vec::new();
        while let Some(((_, element), next)) = json
            .array_iterator_next(&position)
            .map_err(|_| invalid("out of memory"))?
        {
            match element.element_type()? {
                ElementType::TEXT
                | ElementType::TEXTJ
                | ElementType::TEXT5
                | ElementType::TEXTRAW => globs.push(element.scalar_string_value()?),
                _ => return Err(invalid("an element is not a string")),
            }
            position = next;
        }
        Self::new(&globs).map(Some)
    }

    pub fn new(globs: &[String]) -> Result<Self> {
        let mut whole = GlobSetBuilder::new();
        for glob in globs {
            whole.add(path_glob(glob).map_err(|err| {
                LimboError::InvalidArgument(format!("{PATH_SYNC_FUNCTION_NAME}: {err}"))
            })?);
        }
        let whole = whole.build().map_err(|err| {
            LimboError::InvalidArgument(format!("{PATH_SYNC_FUNCTION_NAME}: {err}"))
        })?;
        Ok(Self {
            whole,
            components: globs.iter().map(|glob| split_glob(glob)).collect(),
        })
    }

    pub fn matches(&self, path: &str) -> bool {
        self.whole.is_match(path)
    }

    /// The states before any component is matched.
    fn start(&self) -> PatternStates {
        let mut states = Vec::new();
        for pattern in 0..self.components.len() {
            self.reach(pattern, 0, &mut states);
        }
        normalized(states)
    }

    /// The states after one more component, `name`, is matched.
    fn advance(&self, states: &PatternStates, name: &str) -> PatternStates {
        let mut next = Vec::new();
        for &(pattern, matched) in states {
            match self.components[pattern].get(matched) {
                None => {}
                Some(Component::Literal(literal)) => {
                    if literal == name {
                        self.reach(pattern, matched + 1, &mut next);
                    }
                }
                Some(Component::Glob(glob)) => {
                    if glob.is_match(name) {
                        self.reach(pattern, matched + 1, &mut next);
                    }
                }
                Some(Component::AnyDirectories | Component::Anything) => {
                    self.reach(pattern, matched, &mut next);
                }
            }
        }
        normalized(next)
    }

    /// Adds the state, and those it reaches by letting `**` match no directory.
    fn reach(&self, pattern: usize, mut matched: usize, states: &mut PatternStates) {
        states.push((pattern, matched));
        while let Some(Component::AnyDirectories) = self.components[pattern].get(matched) {
            matched += 1;
            states.push((pattern, matched));
        }
    }

    /// Whether a file reached with these states might match. It may say yes for a file that does
    /// not match, and never no for one that does: `matches` decides.
    fn may_finish(&self, states: &PatternStates) -> bool {
        states.iter().any(|&(pattern, matched)| {
            matches!(
                self.components[pattern].get(matched),
                None | Some(Component::AnyDirectories | Component::Anything)
            )
        })
    }

    /// The states that still need a component: those a directory is worth reading for.
    fn unfinished(&self, mut states: PatternStates) -> PatternStates {
        states.retain(|&(pattern, matched)| matched < self.components[pattern].len());
        states
    }

    /// The names the states need next, if each of them needs one exact name.
    fn only_literals(&self, states: &PatternStates) -> Option<Vec<String>> {
        let mut names = Vec::with_capacity(states.len());
        for &(pattern, matched) in states {
            match self.components[pattern].get(matched) {
                Some(Component::Literal(name)) => names.push(name.clone()),
                _ => return None,
            }
        }
        names.sort_unstable();
        names.dedup();
        (!names.is_empty()).then_some(names)
    }
}

fn normalized(mut states: PatternStates) -> PatternStates {
    states.sort_unstable();
    states.dedup();
    states
}

fn path_glob(glob: &str) -> std::result::Result<globset::Glob, globset::Error> {
    GlobBuilder::new(glob)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
}

/// Splits a glob at each `/`. Where a component cannot be taken on its own, the split stops
/// with `Anything`.
fn split_glob(glob: &str) -> Vec<Component> {
    let mut components = Vec::new();
    for part in glob.split('/') {
        match component(part) {
            Some(component) => components.push(component),
            None => {
                components.push(Component::Anything);
                break;
            }
        }
    }
    components
}

/// What one component of a glob needs, or `None` if it cannot be taken on its own: it is empty,
/// holds an alternation or a `**` beside other characters, ends in an escape, or does not parse.
///
/// A component of plain characters, escaped characters (`\*`) and classes of one character
/// (`[*]`, as `globset::escape` writes them) is one exact name.
fn component(part: &str) -> Option<Component> {
    if part == "**" {
        return Some(Component::AnyDirectories);
    }
    if part.is_empty() {
        return None;
    }
    let mut name = String::with_capacity(part.len());
    let mut exact = true;
    let mut after_star = false;
    let mut chars = part.chars();
    while let Some(c) = chars.next() {
        let star = c == '*';
        match c {
            '\\' => name.push(chars.next()?),
            '{' | '}' => return None,
            '*' if after_star => return None,
            '*' | '?' | ']' => exact = false,
            '[' => {
                let mut class = chars.clone();
                match (class.next(), class.next()) {
                    (Some(only), Some(']')) if only != '!' && only != '^' => {
                        name.push(only);
                        chars = class;
                    }
                    _ => exact = false,
                }
            }
            c => name.push(c),
        }
        after_star = star;
    }
    if exact {
        Some(Component::Literal(name))
    } else {
        path_glob(part)
            .ok()
            .map(|glob| Component::Glob(glob.compile_matcher()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patterns(globs: &[&str]) -> PathPatterns {
        PathPatterns::new(&globs.iter().map(|g| (*g).to_owned()).collect::<Vec<_>>()).unwrap()
    }

    /// Whether the split patterns would let a walk reach `path` as a file.
    fn reachable(patterns: &PathPatterns, path: &str) -> bool {
        let components: Vec<&str> = path.split('/').collect();
        let (file, directories) = components.split_last().unwrap();
        let mut states = patterns.start();
        for directory in directories {
            states = patterns.unfinished(patterns.advance(&states, directory));
            if states.is_empty() {
                return false;
            }
        }
        patterns.may_finish(&patterns.advance(&states, file))
    }

    #[test]
    fn the_split_patterns_reach_every_path_the_whole_patterns_match() {
        let globs = [
            "crates/foo/**",
            "**/*.toml",
            "crates/*/Cargo.toml",
            "Cargo.lock",
            "a/**/b",
            "**",
            "docs/{guide,api/v1}/*.md",
            "src/[a-c]?.rs",
            "we\\*ird/*",
            "x**y/z",
            "",
            "a//b",
            "app/[[]id[]]/page.tsx",
            "q[?]/[*].rs",
            "[{]lit[}]",
            "back\\\\slash/\\*",
            "[!a]/x",
        ];
        let paths = [
            "Cargo.lock",
            "Cargo.toml",
            "crates/foo/src/lib.rs",
            "crates/foo/Cargo.toml",
            "crates/bar/Cargo.toml",
            "crates/bar/src/Cargo.toml",
            "a/b",
            "a/x/y/b",
            "a/b/c",
            "docs/guide/intro.md",
            "docs/api/v1/index.md",
            "src/ab.rs",
            "src/zz.rs",
            "we*ird/file",
            "xy/z",
            "x/q/y/z",
            "",
            "a//b",
            "deep/er/than/any/pattern.txt",
            "app/[id]/page.tsx",
            "q?/*.rs",
            "{lit}",
            "back\\slash/*",
            "b/x",
        ];
        for glob in globs {
            let one = patterns(&[glob]);
            for path in paths {
                if one.matches(path) {
                    assert!(reachable(&one, path), "{glob:?} matches {path:?}");
                }
            }
        }
        let all = patterns(&globs);
        for path in paths {
            if all.matches(path) {
                assert!(reachable(&all, path), "the set matches {path:?}");
            }
        }
    }

    #[test]
    fn a_pattern_of_exact_names_prunes_everything_else() {
        let one = patterns(&["crates/foo/**", "Cargo.lock"]);
        let root = one.start();
        assert_eq!(
            one.only_literals(&root),
            Some(vec!["Cargo.lock".to_owned(), "crates".to_owned()])
        );
        assert!(one.unfinished(one.advance(&root, "docs")).is_empty());
        let crates = one.unfinished(one.advance(&root, "crates"));
        assert!(one.unfinished(one.advance(&crates, "bar")).is_empty());
        assert!(!one.unfinished(one.advance(&crates, "foo")).is_empty());
    }

    #[test]
    fn a_name_written_with_escapes_is_still_one_exact_name() {
        let one = patterns(&["app/[[]id[]]/page.tsx", "we\\*ird/[?]"]);
        assert!(one.matches("app/[id]/page.tsx"));
        assert!(one.matches("we*ird/?"));
        let root = one.start();
        assert_eq!(
            one.only_literals(&root),
            Some(vec!["app".to_owned(), "we*ird".to_owned()])
        );
        let app = one.unfinished(one.advance(&root, "app"));
        assert_eq!(one.only_literals(&app), Some(vec!["[id]".to_owned()]));
        assert!(one.unfinished(one.advance(&app, "id")).is_empty());
    }

    #[test]
    fn a_star_does_not_cross_a_separator() {
        let one = patterns(&["crates/*/Cargo.toml"]);
        assert!(one.matches("crates/a/Cargo.toml"));
        assert!(!one.matches("crates/a/b/Cargo.toml"));
        assert!(!reachable(&one, "crates/a/b/Cargo.toml"));
    }
}
