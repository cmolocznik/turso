//! An index over slash-separated paths that answers one question fast: which rows have a path
//! matching any of a set of glob patterns, and, optionally, a sequence number above a given one.
//!
//! > CREATE INDEX files_by_path ON files USING path_trie (path, seq) WITH (depth = 2)
//! > SELECT * FROM files WHERE path_sync(path, seq, '["src/**", "*.toml"]', 41)
//!
//! Each row has entries of its own and shares none with another row, so writers of different
//! rows never write the same entry: under MVCC they commit side by side. There is no directory
//! entry, counter or running maximum to keep up to date.
//!
//! The entries are in two key-only B-trees:
//!
//! - `by_path`: (path, sequence number, rowid), one per row. A query with no minimum sequence
//!   number reads the range of a pattern's literal directory, or the one path a pattern names.
//! - `by_directory`: (directory, direct child, sequence number, rest of the path, rowid), one for
//!   every directory above the row down to `depth` levels below the root: the root, then each
//!   directory in turn. The entry for the row's own directory is marked a direct child. A query
//!   with a minimum sequence number reads, for each pattern, the entries of its literal directory
//!   above that number, or those of the directory `depth` levels down that holds it; a pattern
//!   that can match only a direct child reads only those.
//!
//! The rows `by_directory` holds under one directory are a closure table's: every pair of a
//! directory and a row below it, here cut off `depth` levels down. A pattern's literal part is
//! as many of its first components as hold no glob character; every candidate is then checked
//! against the whole pattern, so a class or an alternation that can match a `/` is never cut
//! short. Two rows may have one path: keeping paths unique is a UNIQUE index's work.

use std::collections::{HashSet, VecDeque};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
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

/// How many levels below the root `by_directory` keeps entries for, unless the index says.
const DEFAULT_DEPTH: usize = 2;
/// The most levels an index may ask for.
const MOST_DEPTH: usize = 64;

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
        let depth = depth_of(configuration)?;
        let pattern = format!(
            "SELECT * FROM {} WHERE {PATH_SYNC_FUNCTION_NAME}({}, {}, ?, ?)",
            configuration.table_name, path.name, seq.name
        );
        Ok(Arc::new(PathTrieAttachment {
            configuration: configuration.clone(),
            patterns: parse_patterns(&[&pattern])?,
            depth,
        }))
    }
}

/// The index's `depth` option: the default when it is not given.
fn depth_of(configuration: &IndexMethodConfiguration) -> Result<usize> {
    if let Some(other) = configuration
        .parameters
        .keys()
        .find(|name| *name != "depth")
    {
        return Err(LimboError::ParseError(format!(
            "{PATH_TRIE_INDEX_METHOD_NAME} index has no option {other}: it takes only depth"
        )));
    }
    match configuration.parameters.get("depth") {
        None => Ok(DEFAULT_DEPTH),
        Some(Value::Numeric(Numeric::Integer(depth)))
            if usize::try_from(*depth).is_ok_and(|depth| depth <= MOST_DEPTH) =>
        {
            Ok(*depth as usize)
        }
        Some(other) => Err(LimboError::ParseError(format!(
            "{PATH_TRIE_INDEX_METHOD_NAME} index: depth must be an integer from 0 to \
             {MOST_DEPTH}, not {other:?}"
        ))),
    }
}

#[derive(Debug)]
pub struct PathTrieAttachment {
    configuration: IndexMethodConfiguration,
    patterns: Vec<ast::Select>,
    depth: usize,
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
        Ok(Box::new(PathTrieCursor::new(
            self.configuration.clone(),
            self.depth,
        )))
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
            BackingColumn::new("path", Type::Text),
            BackingColumn::new("seq", Type::Integer),
            BackingColumn::new("row", Type::Integer),
            BackingColumn::new("dir", Type::Text),
            BackingColumn::new("direct", Type::Integer),
            BackingColumn::new("rest", Type::Text),
        ],
    );
    let index = |suffix: &str, names: &[&str]| {
        BackingIndex::on_backing_table(
            &table,
            format!("{}_{suffix}", table.table_name()),
            names.iter().map(|name| (*name).to_string()).collect(),
            names.iter().map(|_| key_info()).collect(),
        )
    };
    let by_path = index("by_path", &["path", "seq", "row"]);
    let by_directory = index("by_directory", &["dir", "direct", "seq", "rest", "row"]);
    BackingSchema::new(vec![table], vec![by_path, by_directory])
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
    depth: usize,
    by_path: Option<Box<dyn CursorTrait>>,
    by_directory: Option<Box<dyn CursorTrait>>,
    /// An open of its cursors is under way: entered again, it goes on with the cursors it made.
    opening: bool,
    pending_store_ops: VecDeque<BackingStoreOp>,
    /// The entries an insert or a delete has decided to write, until they are written.
    writes: Option<Writes>,
    query: Option<Query>,
    /// `query_start` made `query` and has not yet given its first answer.
    query_starting: bool,
}

impl PathTrieCursor {
    pub fn new(configuration: IndexMethodConfiguration, depth: usize) -> Self {
        Self {
            schema: backing_schema(&configuration),
            depth,
            by_path: None,
            by_directory: None,
            opening: false,
            pending_store_ops: VecDeque::new(),
            writes: None,
            query: None,
            query_starting: false,
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
            self.by_path = Some(open_store_cursor(context, &self.schema.indexes[0])?);
            self.by_directory = Some(open_store_cursor(context, &self.schema.indexes[1])?);
            self.opening = true;
        }
        for cursor in [&mut self.by_path, &mut self.by_directory]
            .into_iter()
            .flatten()
        {
            return_if_io!(cursor.finish_open());
        }
        Ok(IOResult::Done(()))
    }

    /// Writes the entries `decide` gives for the row in `values`, deciding once: entered again
    /// after a wait for IO, it goes on with what it decided.
    fn write(
        &mut self,
        values: &[Register],
        decide: fn(&IndexedRow, usize) -> Result<Writes>,
    ) -> IOResultOr<()> {
        let (Some(by_path), Some(by_directory)) = (&mut self.by_path, &mut self.by_directory)
        else {
            return Err(not_open());
        };
        if self.writes.is_none() {
            let Some(row) = IndexedRow::from_registers(values)? else {
                return Ok(IOResult::Done(()));
            };
            self.writes = Some(decide(&row, self.depth)?);
        }
        let writes = self.writes.as_mut().expect("decided above");
        let out = writes.drain(by_path.as_mut(), by_directory.as_mut());
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.writes = None;
        }
        out
    }

    fn advance_query(&mut self) -> IOResultOr<bool> {
        let (Some(by_path), Some(by_directory), Some(query)) =
            (&mut self.by_path, &mut self.by_directory, &mut self.query)
        else {
            return Err(not_open());
        };
        query.advance(by_path.as_mut(), by_directory.as_mut())
    }
}

fn open_store_cursor(
    context: &IndexMethodContext,
    index: &BackingIndex,
) -> Result<Box<dyn CursorTrait>> {
    context
        .backing_store(index)?
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "backing store {} not found: an index made by an earlier {PATH_TRIE_INDEX_METHOD_NAME} \
                 must be dropped and made again",
                index.name
            ))
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
        self.by_path = None;
        self.by_directory = None;
        self.opening = false;
    }

    /// Adds the row's entries. It reads nothing: no other row's entry is touched.
    fn insert(&mut self, values: &[Register]) -> IOResultOr<()> {
        self.write(values, Writes::inserting)
    }

    /// Removes the row's entries, as `insert` made them from the same values.
    fn delete(&mut self, values: &[Register]) -> IOResultOr<()> {
        self.write(values, Writes::deleting)
    }

    /// `values` holds the pattern's number (there is one pattern), then the JSON array of globs,
    /// then the minimum sequence number or NULL.
    fn query_start(&mut self, values: &[Register]) -> IOResultOr<bool> {
        if !self.query_starting {
            let since = since_from_value(values[2].get_value())?;
            let patterns = PathPatterns::from_value(values[1].get_value())?;
            self.query = Some(Query::new(patterns, since, self.depth));
            self.query_starting = true;
        }
        let out = self.advance_query();
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.query_starting = false;
        }
        out
    }

    fn query_next(&mut self) -> IOResultOr<bool> {
        self.advance_query()
    }

    fn query_rowid(&mut self) -> IOResultOr<Option<i64>> {
        Ok(IOResult::Done(
            self.query.as_ref().and_then(|query| query.row),
        ))
    }

    fn query_column(&mut self, _: usize) -> IOResultOr<Value> {
        Err(Box::new(LimboError::InternalError(
            "path_trie serves no column of its own".to_string(),
        )))
    }
}

/// What a row gives the index: its path, split at each `/`, its sequence number and its rowid.
#[derive(Debug)]
struct IndexedRow {
    path: String,
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
            path: path.to_owned(),
            components: path.split('/').map(str::to_owned).collect(),
            seq: sequence_number(values[1].get_value()),
            rowid,
        }))
    }

    /// The row's entries in `by_directory`: one for each directory above it down to `depth`
    /// levels below the root, the root first.
    fn directory_entries(&self, depth: usize) -> Vec<Entry> {
        let directories = &self.components[..self.components.len() - 1];
        let levels = directories.len().min(depth);
        let mut dir = String::new();
        let mut entries = Vec::with_capacity(levels + 1);
        for level in 0..=levels {
            if level > 0 {
                dir.push_str(&directories[level - 1]);
                dir.push('/');
            }
            entries.push(Entry {
                path: format!("{dir}{}", self.components[level..].join("/")),
                seq: self.seq,
                rowid: self.rowid,
                dir: dir.clone(),
                direct: level == directories.len(),
            });
        }
        entries
    }

    fn path_entry(&self) -> Entry {
        Entry {
            path: self.path.clone(),
            seq: self.seq,
            rowid: self.rowid,
            dir: String::new(),
            direct: false,
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

/// A query in progress: the ranges still to read, the one being read, and the rows given.
struct Query {
    /// `None` when the patterns are NULL: no row matches.
    patterns: Option<PathPatterns>,
    since: Option<i64>,
    scans: VecDeque<Scan>,
    reading: Option<(Scan, Finder)>,
    /// Rows already given: two patterns' ranges may hold one row.
    given: HashSet<i64>,
    /// The rowid of the row the query stands at.
    row: Option<i64>,
}

impl Query {
    fn new(patterns: Option<PathPatterns>, since: Option<i64>, depth: usize) -> Self {
        let scans = patterns
            .as_ref()
            .map_or_else(VecDeque::new, |patterns| plan(patterns, since, depth));
        Self {
            patterns,
            since,
            scans,
            reading: None,
            given: HashSet::new(),
            row: None,
        }
    }

    /// Moves to the next matching row. Returns whether there is one.
    fn advance(
        &mut self,
        by_path: &mut dyn CursorTrait,
        by_directory: &mut dyn CursorTrait,
    ) -> IOResultOr<bool> {
        let Some(patterns) = &self.patterns else {
            return Ok(IOResult::Done(false));
        };
        loop {
            let Some((scan, finder)) = &mut self.reading else {
                let Some(scan) = self.scans.pop_front() else {
                    self.row = None;
                    return Ok(IOResult::Done(false));
                };
                let finder = Finder::at(scan.start()?);
                self.reading = Some((scan, finder));
                continue;
            };
            let tree = scan.tree();
            let cursor: &mut dyn CursorTrait = match tree {
                Tree::ByPath => &mut *by_path,
                Tree::ByDirectory => &mut *by_directory,
            };
            let entry = return_if_io!(finder.step(cursor, tree));
            // The read is done. Nothing below waits for IO, so each entry is handled once.
            let Some(entry) = entry.filter(|entry| scan.holds(entry)) else {
                self.reading = None;
                continue;
            };
            finder.next();
            if self.since.is_some_and(|since| entry.seq <= since)
                || !patterns.matches(&entry.path)
                || !self.given.insert(entry.rowid)
            {
                continue;
            }
            self.row = Some(entry.rowid);
            return Ok(IOResult::Done(true));
        }
    }
}

/// A range of one B-tree that a query reads.
#[derive(Debug, PartialEq, Eq)]
enum Scan {
    /// The rows of exactly this path, in `by_path`.
    Path(String),
    /// The rows whose path starts with this, in `by_path`: a directory's path and `/`, or
    /// nothing for every row.
    Below(String),
    /// The entries of `dir` in `by_directory` of one kind, newer than `above`.
    Directory {
        dir: String,
        direct: bool,
        above: i64,
    },
}

impl Scan {
    fn tree(&self) -> Tree {
        match self {
            Self::Path(_) | Self::Below(_) => Tree::ByPath,
            Self::Directory { .. } => Tree::ByDirectory,
        }
    }

    /// The key the range starts at.
    fn start(&self) -> Result<ImmutableRecord> {
        match self {
            Self::Path(path) | Self::Below(path) => {
                ImmutableRecord::from_values(&[Value::build_text(path.clone())], 1)
            }
            Self::Directory { dir, direct, above } => ImmutableRecord::from_values(
                &[
                    Value::build_text(dir.clone()),
                    Value::from_i64(i64::from(*direct)),
                    Value::from_i64(above.saturating_add(1)),
                ],
                3,
            ),
        }
    }

    /// Whether `entry`, read in the range's B-tree at or after its start, is still in it.
    fn holds(&self, entry: &Entry) -> bool {
        match self {
            Self::Path(path) => entry.path == *path,
            Self::Below(prefix) => entry.path.starts_with(prefix.as_str()),
            Self::Directory { dir, direct, above } => {
                entry.dir == *dir && entry.direct == *direct && entry.seq > *above
            }
        }
    }
}

/// The ranges that hold every row the patterns may match, newer than `since` if it is set.
fn plan(patterns: &PathPatterns, since: Option<i64>, depth: usize) -> VecDeque<Scan> {
    let mut scans = VecDeque::new();
    for components in &patterns.components {
        let literals: Vec<&str> = components
            .iter()
            .map_while(|component| match component {
                Component::Literal(name) => Some(name.as_str()),
                _ => None,
            })
            .collect();
        if literals.len() == components.len() {
            scans.push_back(Scan::Path(literals.join("/")));
            continue;
        }
        let rest = &components[literals.len()..];
        let only_direct_children = matches!(
            rest,
            [Component::Glob {
                crosses_separator: false
            }]
        );
        match since {
            None => scans.push_back(Scan::Below(directory_path(&literals))),
            // No sequence number is above the largest.
            Some(i64::MAX) => {}
            Some(above) if literals.len() <= depth => {
                let dir = directory_path(&literals);
                scans.push_back(Scan::Directory {
                    dir: dir.clone(),
                    direct: true,
                    above,
                });
                if !only_direct_children {
                    scans.push_back(Scan::Directory {
                        dir,
                        direct: false,
                        above,
                    });
                }
            }
            // Every row under a directory deeper than `depth` is below, and no direct child,
            // of the directory `depth` levels down that holds it.
            Some(above) => scans.push_back(Scan::Directory {
                dir: directory_path(&literals[..depth]),
                direct: false,
                above,
            }),
        }
    }
    scans
}

/// The path of the directory these names lead to, each followed by `/`: empty for the root.
fn directory_path(names: &[&str]) -> String {
    names.iter().map(|name| format!("{name}/")).collect()
}

/// An entry of either B-tree. A `by_path` entry has no directory.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    path: String,
    seq: i64,
    rowid: i64,
    dir: String,
    direct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tree {
    ByPath,
    ByDirectory,
}

fn by_path_key(entry: &Entry) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[
            Value::build_text(entry.path.clone()),
            Value::from_i64(entry.seq),
            Value::from_i64(entry.rowid),
        ],
        3,
    )
}

fn by_directory_key(entry: &Entry) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[
            Value::build_text(entry.dir.clone()),
            Value::from_i64(i64::from(entry.direct)),
            Value::from_i64(entry.seq),
            Value::build_text(entry.path[entry.dir.len()..].to_owned()),
            Value::from_i64(entry.rowid),
        ],
        5,
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
        Tree::ByPath => Ok(Entry {
            path: text(0)?,
            seq: integer(1)?,
            rowid: integer(2)?,
            dir: String::new(),
            direct: false,
        }),
        Tree::ByDirectory => {
            let dir = text(0)?;
            Ok(Entry {
                path: format!("{dir}{}", text(3)?),
                seq: integer(2)?,
                rowid: integer(4)?,
                direct: integer(1)? != 0,
                dir,
            })
        }
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
    /// The row's entries, to add.
    fn inserting(row: &IndexedRow, depth: usize) -> Result<Self> {
        Self::of(row, depth, WriteOp::Insert)
    }

    /// The row's entries, to remove.
    fn deleting(row: &IndexedRow, depth: usize) -> Result<Self> {
        Self::of(row, depth, WriteOp::Delete)
    }

    fn of(
        row: &IndexedRow,
        depth: usize,
        op: fn(Tree, ImmutableRecord) -> WriteOp,
    ) -> Result<Self> {
        let mut ops = VecDeque::from([op(Tree::ByPath, by_path_key(&row.path_entry())?)]);
        for entry in row.directory_entries(depth) {
            ops.push_back(op(Tree::ByDirectory, by_directory_key(&entry)?));
        }
        Ok(Self {
            ops,
            phase: WritePhase::Seek,
        })
    }

    fn drain(
        &mut self,
        by_path: &mut dyn CursorTrait,
        by_directory: &mut dyn CursorTrait,
    ) -> IOResultOr<()> {
        while let Some(op) = self.ops.front() {
            let (WriteOp::Insert(tree, key) | WriteOp::Delete(tree, key)) = op;
            let is_delete = matches!(op, WriteOp::Delete(..));
            let cursor: &mut dyn CursorTrait = match tree {
                Tree::ByPath => &mut *by_path,
                Tree::ByDirectory => &mut *by_directory,
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

fn corrupt(what: &str) -> Box<LimboError> {
    Box::new(LimboError::Corrupt(format!(
        "{PATH_TRIE_INDEX_METHOD_NAME} index corrupted: {what}"
    )))
}

/// A set of glob patterns over slash-separated paths, with `globset`'s rules and a literal
/// separator: `*` and `?` do not match `/`, and `**` as a whole component matches any number
/// of directories. A class may still match `/`: `[!x]` does.
pub struct PathPatterns {
    /// Decides whether a whole path matches.
    whole: GlobSet,
    /// Each pattern split at `/`, for deciding which ranges hold the rows it may match.
    components: Vec<Vec<Component>>,
}

/// What a pattern needs of one path component.
#[derive(Debug)]
enum Component {
    Literal(String),
    /// A glob of one component. One with a class may match a `/`, and so span components.
    Glob {
        crosses_separator: bool,
    },
    /// `**`: any number of directories, none included.
    AnyDirectories,
    /// The pattern from here on is not split: anything below might match, and `whole` decides.
    Anything,
}

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
    let mut class = false;
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
                let mut one = chars.clone();
                match (one.next(), one.next()) {
                    (Some(only), Some(']')) if only != '!' && only != '^' => {
                        name.push(only);
                        chars = one;
                    }
                    _ => {
                        exact = false;
                        class = true;
                    }
                }
            }
            c => name.push(c),
        }
        after_star = star;
    }
    if exact {
        Some(Component::Literal(name))
    } else {
        path_glob(part).ok().map(|_| Component::Glob {
            crosses_separator: class,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patterns(globs: &[&str]) -> PathPatterns {
        PathPatterns::new(&globs.iter().map(|g| (*g).to_owned()).collect::<Vec<_>>()).unwrap()
    }

    fn row(path: &str, seq: i64) -> IndexedRow {
        IndexedRow {
            path: path.to_owned(),
            components: path.split('/').map(str::to_owned).collect(),
            seq,
            rowid: 7,
        }
    }

    /// Whether the planned ranges hold one of the row's entries that the query reads.
    fn covered(scans: &VecDeque<Scan>, row: &IndexedRow, depth: usize) -> bool {
        let path_entry = row.path_entry();
        let directory_entries = row.directory_entries(depth);
        scans.iter().any(|scan| match scan.tree() {
            Tree::ByPath => scan.holds(&path_entry),
            Tree::ByDirectory => directory_entries.iter().any(|entry| scan.holds(entry)),
        })
    }

    const GLOBS: &[&str] = &[
        "crates/foo/**",
        "**/*.toml",
        "*.toml",
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
        "a[!x]b",
        "a[.-0]b",
        "a{/x,}/b",
        "deep/er/than/two/*.txt",
        "deep/er/**",
    ];
    const PATHS: &[&str] = &[
        "Cargo.lock",
        "Cargo.toml",
        "crates/foo/src/lib.rs",
        "crates/foo/Cargo.toml",
        "crates/bar/Cargo.toml",
        "crates/bar/src/Cargo.toml",
        "a/b",
        "a/x/y/b",
        "a/b/c",
        "a/x/b",
        "docs/guide/intro.md",
        "docs/api/v1/index.md",
        "src/ab.rs",
        "src/zz.rs",
        "we*ird/file",
        "xy/z",
        "x/q/y/z",
        "",
        "a//b",
        "deep/er/than/two/pattern.txt",
        "deep/er/than/any/pattern.txt",
        "app/[id]/page.tsx",
        "q?/*.rs",
        "{lit}",
        "back\\slash/*",
        "b/x",
        "/x",
    ];

    /// Every row a pattern matches is in a range its plan reads, at every depth, with and
    /// without a minimum sequence number below the row's.
    #[test]
    fn the_plan_reads_every_row_the_whole_patterns_match() {
        for depth in [0, 1, 2, 3, 6] {
            for glob in GLOBS {
                let one = patterns(&[glob]);
                for path in PATHS {
                    if !one.matches(path) {
                        continue;
                    }
                    let row = row(path, 5);
                    for since in [None, Some(4), Some(i64::MIN)] {
                        let scans = plan(&one, since, depth);
                        assert!(
                            covered(&scans, &row, depth),
                            "{glob:?} matches {path:?}, depth {depth}, since {since:?}: {scans:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn a_row_newer_than_nothing_the_query_asks_for_is_in_no_range() {
        let one = patterns(&["**"]);
        assert!(plan(&one, Some(i64::MAX), 2).is_empty());
        let scans = plan(&one, Some(5), 2);
        assert!(!covered(&scans, &row("a/b/c.rs", 5), 2));
        assert!(covered(&scans, &row("a/b/c.rs", 6), 2));
    }

    #[test]
    fn a_pattern_of_exact_names_reads_one_path() {
        let one = patterns(&["app/[[]id[]]/page.tsx", "we\\*ird/[?]"]);
        assert!(one.matches("app/[id]/page.tsx"));
        assert!(one.matches("we*ird/?"));
        assert_eq!(
            plan(&one, None, 2),
            VecDeque::from([
                Scan::Path("app/[id]/page.tsx".to_owned()),
                Scan::Path("we*ird/?".to_owned()),
            ])
        );
    }

    /// `*.toml` can match only a file of the root: it reads the root's direct children. A class
    /// may match `/`, so `a[!x]b` reads everything below the root.
    #[test]
    fn only_a_pattern_that_cannot_cross_a_separator_reads_direct_children_alone() {
        let direct = |dir: &str| Scan::Directory {
            dir: dir.to_owned(),
            direct: true,
            above: 1,
        };
        let below = |dir: &str| Scan::Directory {
            dir: dir.to_owned(),
            direct: false,
            above: 1,
        };
        assert_eq!(
            plan(&patterns(&["*.toml"]), Some(1), 2),
            VecDeque::from([direct("")])
        );
        assert_eq!(
            plan(&patterns(&["a[!x]b"]), Some(1), 2),
            VecDeque::from([direct(""), below("")])
        );
        assert_eq!(
            plan(&patterns(&["crates/*/Cargo.toml"]), Some(1), 2),
            VecDeque::from([direct("crates/"), below("crates/")])
        );
        // Deeper than the index keeps: the directory two levels down, below it.
        assert_eq!(
            plan(&patterns(&["a/b/c/*.rs"]), Some(1), 2),
            VecDeque::from([below("a/b/")])
        );
    }

    #[test]
    fn a_row_has_an_entry_for_each_directory_down_to_the_depth() {
        let entries = |path: &str, depth| {
            row(path, 1)
                .directory_entries(depth)
                .into_iter()
                .map(|entry| (entry.dir, entry.direct, entry.path))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            entries("a/b/c.rs", 2),
            vec![
                (String::new(), false, "a/b/c.rs".to_owned()),
                ("a/".to_owned(), false, "a/b/c.rs".to_owned()),
                ("a/b/".to_owned(), true, "a/b/c.rs".to_owned()),
            ]
        );
        assert_eq!(
            entries("a/b/c/d.rs", 1),
            vec![
                (String::new(), false, "a/b/c/d.rs".to_owned()),
                ("a/".to_owned(), false, "a/b/c/d.rs".to_owned()),
            ]
        );
        assert_eq!(
            entries("x.rs", 2),
            vec![(String::new(), true, "x.rs".to_owned())]
        );
        assert_eq!(
            entries("/x", 2),
            vec![
                (String::new(), false, "/x".to_owned()),
                ("/".to_owned(), true, "/x".to_owned()),
            ]
        );
    }
}
