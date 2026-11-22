//! An index of the trigrams of a text column, which answers one question fast: which rows' text a
//! regular expression matches somewhere.
//!
//! > CREATE INDEX docs_by_trigram ON docs USING trigram (text)
//! > SELECT rowid FROM docs WHERE code_match(text, 'fn\s+main')
//!
//! It is Google Code Search's design (Russ Cox, "Regular Expression Matching with a Trigram
//! Index", 2012): each text's trigrams, three bytes of its UTF-8, go into a posting list per
//! trigram; an expression becomes a query over trigrams that every text it matches satisfies
//! ([`plan`], codesearch's `index/regexp.go`); the texts the query admits are read and the
//! expression run on each, so the index returns exactly the rows `code_match` is true for.
//!
//! The lists are kept as Lucene keeps its postings: in immutable segments. Every statement that
//! adds texts writes one segment of them when it ends, with a number for each of its texts from
//! 0 and, for each trigram, the numbers of the texts holding it, 128 to a row, each after the
//! first as the varint of its distance from the one before, as codesearch writes its lists. A
//! query reads, in each segment, only the rows of the trigrams it names. Each text keeps the
//! identity it was given when it was added, the segment it was added in and its rowid; a removed
//! text is a tombstone of that identity, so a segment is never written again once it is made.
//!
//! The rows are in five key-only B-trees, on a table of the method's own that stays empty:
//!
//! - `segments`: (segment, texts), one per segment.
//! - `postings`: (segment, trigram, first, rest), the lists.
//! - `docs`: (segment, number, rowid, origin), each text of a segment.
//! - `ids`: (rowid, origin), the identity of each text the index holds, found by rowid when the
//!   row is removed.
//! - `tombs`: (origin, rowid), each text removed.
//!
//! Every segment, text and tombstone has entries of its own, under a segment id drawn at
//! random, so writers of different statements never write the same entry: under MVCC they
//! commit side by side.

pub(crate) mod plan;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};

use turso_parser::ast::{self, SortOrder};

use crate::numeric::Numeric;
use crate::types::IOResultOr;
use crate::{
    index_method::{
        parse_patterns, BackingColumn, BackingIndex, BackingSchema, BackingStoreOp, BackingTable,
        IndexMethod, IndexMethodAttachment, IndexMethodConfiguration, IndexMethodContext,
        IndexMethodCursor, IndexMethodDefinition, IndexMethodMvccSupport,
        TRIGRAM_INDEX_METHOD_NAME,
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

use plan::Trigram;

/// The SQL function the index serves.
pub const CODE_MATCH_FUNCTION_NAME: &str = "code_match";

/// How many texts' numbers one `postings` row holds, as Lucene blocks its postings.
const BLOCK: usize = 128;

/// The bytes of a segment's id.
const SEGMENT_ID_LEN: usize = 16;

thread_local! {
    /// The last expression `code_match` compiled, which a scan asks for row after row.
    static COMPILED: RefCell<Option<(String, regex::Regex)>> = const { RefCell::new(None) };
}

/// `code_match(text, pattern)` for one row, without the index: whether the regular expression
/// `pattern` (the `regex` crate's syntax) matches somewhere in `text`. NULL if either is NULL or
/// not text. The index returns exactly the rows for which this is true.
pub fn code_match(text: &Value, pattern: &Value) -> Result<Value> {
    let (Some(text), Some(pattern)) = (text.to_text(), pattern.to_text()) else {
        return Ok(Value::Null);
    };
    COMPILED.with(|compiled| {
        let mut compiled = compiled.borrow_mut();
        if compiled.as_ref().is_none_or(|(last, _)| last != pattern) {
            *compiled = Some((pattern.to_owned(), compile(pattern)?));
        }
        let (_, regex) = compiled.as_ref().expect("compiled above");
        Ok(Value::from_i64(i64::from(regex.is_match(text))))
    })
}

fn compile(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(pattern)
        .map_err(|err| LimboError::InvalidArgument(format!("{CODE_MATCH_FUNCTION_NAME}: {err}")))
}

/// The distinct trigrams of `text`'s bytes, each as the key the index keeps it under.
fn trigrams_of(text: &[u8]) -> Vec<i64> {
    let mut trigrams: Vec<i64> = text
        .windows(3)
        .map(|w| trigram_key(&[w[0], w[1], w[2]]))
        .collect();
    trigrams.sort_unstable();
    trigrams.dedup();
    trigrams
}

fn trigram_key(trigram: &Trigram) -> i64 {
    (i64::from(trigram[0]) << 16) | (i64::from(trigram[1]) << 8) | i64::from(trigram[2])
}

#[derive(Debug)]
pub struct TrigramIndexMethod;

impl IndexMethod for TrigramIndexMethod {
    fn attach(
        &self,
        configuration: &IndexMethodConfiguration,
    ) -> Result<Arc<dyn IndexMethodAttachment>> {
        let [text] = configuration.columns.as_slice() else {
            return Err(LimboError::ParseError(format!(
                "{TRIGRAM_INDEX_METHOD_NAME} index needs one column, a text, but {} has {}",
                configuration.index_name,
                configuration.columns.len()
            )));
        };
        if let Some(option) = configuration.parameters.keys().next() {
            return Err(LimboError::ParseError(format!(
                "{TRIGRAM_INDEX_METHOD_NAME} index has no option {option}"
            )));
        }
        let pattern = format!(
            "SELECT * FROM {} WHERE {CODE_MATCH_FUNCTION_NAME}({}, ?)",
            configuration.table_name, text.name
        );
        Ok(Arc::new(TrigramAttachment {
            configuration: configuration.clone(),
            patterns: parse_patterns(&[&pattern])?,
        }))
    }
}

#[derive(Debug)]
pub struct TrigramAttachment {
    configuration: IndexMethodConfiguration,
    patterns: Vec<ast::Select>,
}

impl IndexMethodAttachment for TrigramAttachment {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a> {
        IndexMethodDefinition {
            method_name: TRIGRAM_INDEX_METHOD_NAME,
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
        Ok(Box::new(TrigramCursor::new(self.configuration.clone())))
    }

    fn backing_schema(&self) -> BackingSchema {
        backing_schema(&self.configuration)
    }
}

/// The trees, in the order of the backing schema's indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tree {
    Segments = 0,
    Postings = 1,
    Docs = 2,
    Ids = 3,
    Tombs = 4,
}

const TREES: [Tree; 5] = [
    Tree::Segments,
    Tree::Postings,
    Tree::Docs,
    Tree::Ids,
    Tree::Tombs,
];

/// One table of the method's own, which stays empty, and the five key-only B-trees on it.
fn backing_schema(configuration: &IndexMethodConfiguration) -> BackingSchema {
    let table = BackingTable::new(
        format!("trigram_{}", configuration.index_name),
        vec![
            BackingColumn::new("segment", Type::Blob),
            BackingColumn::new("texts", Type::Integer),
            BackingColumn::new("trigram", Type::Integer),
            BackingColumn::new("first", Type::Integer),
            BackingColumn::new("rest", Type::Blob),
            BackingColumn::new("number", Type::Integer),
            BackingColumn::new("row", Type::Integer),
            BackingColumn::new("origin", Type::Blob),
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
    let indexes = vec![
        index("segments", &["segment", "texts"]),
        index("postings", &["segment", "trigram", "first", "rest"]),
        index("docs", &["segment", "number", "row", "origin"]),
        index("ids", &["row", "origin"]),
        index("tombs", &["origin", "row"]),
    ];
    BackingSchema::new(vec![table], indexes)
}

fn key_info() -> KeyInfo {
    KeyInfo {
        collation: CollationSeq::BINARY,
        sort_order: SortOrder::Asc,
        nulls_order: None,
    }
}

fn key(values: &[Value]) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(values, values.len())
}

fn blob(bytes: &[u8]) -> Value {
    Value::from_blob(bytes.to_vec())
}

fn corrupt(what: &str) -> Box<LimboError> {
    Box::new(LimboError::Corrupt(format!(
        "{TRIGRAM_INDEX_METHOD_NAME} index corrupted: {what}"
    )))
}

fn integer_at(values: &[Value], column: usize) -> Result<i64> {
    match values.get(column) {
        Some(Value::Numeric(Numeric::Integer(value))) => Ok(*value),
        _ => Err(*corrupt(&format!("column {column} is not an integer"))),
    }
}

fn blob_at(values: &[Value], column: usize) -> Result<Vec<u8>> {
    match values.get(column).and_then(Value::to_blob) {
        Some(bytes) => Ok(bytes.to_vec()),
        None => Err(*corrupt(&format!("column {column} is not a blob"))),
    }
}

/// Appends `value` as an unsigned LEB128 varint.
fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// The numbers a `postings` row holds: `first`, then each of `rest`'s varints added to the one
/// before.
fn decode_block(first: i64, rest: &[u8], out: &mut Vec<u32>) -> Result<()> {
    let mut number = u32::try_from(first).map_err(|_| *corrupt("a text's number"))?;
    out.push(number);
    let (mut value, mut shift) = (0u64, 0u32);
    for byte in rest {
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            let delta = u32::try_from(value).map_err(|_| *corrupt("a distance"))?;
            number = number
                .checked_add(delta)
                .ok_or_else(|| *corrupt("a text's number"))?;
            out.push(number);
            value = 0;
            shift = 0;
        } else {
            shift += 7;
            if shift > 63 {
                return Err(*corrupt("a varint"));
            }
        }
    }
    if shift != 0 {
        return Err(*corrupt("a varint cut short"));
    }
    Ok(())
}

/// The `postings` rows of one trigram's sorted numbers, `BLOCK` to a row.
fn encode_blocks(numbers: &[u32]) -> Vec<(i64, Vec<u8>)> {
    numbers
        .chunks(BLOCK)
        .map(|block| {
            let mut rest = Vec::with_capacity(block.len());
            for pair in block.windows(2) {
                put_varint(&mut rest, u64::from(pair[1] - pair[0]));
            }
            (i64::from(block[0]), rest)
        })
        .collect()
}

/// The texts a statement added, until it ends and they become a segment.
#[derive(Debug, Default)]
struct Pending {
    /// Each text's rowid, by its number in the statement.
    rows: Vec<i64>,
    /// Whether the statement removed it again.
    removed: Vec<bool>,
    by_row: HashMap<i64, usize>,
    /// The numbers of the texts holding each trigram, ascending.
    postings: BTreeMap<i64, Vec<u32>>,
}

impl Pending {
    fn add(&mut self, row: i64, text: &[u8]) {
        let number = self.rows.len();
        self.rows.push(row);
        self.removed.push(false);
        self.by_row.insert(row, number);
        let number = number as u32;
        for trigram in trigrams_of(text) {
            self.postings.entry(trigram).or_default().push(number);
        }
    }

    /// Takes back the text the statement added for `row`, if it did.
    fn take_back(&mut self, row: i64) -> bool {
        match self.by_row.remove(&row) {
            Some(number) => {
                self.removed[number] = true;
                true
            }
            None => false,
        }
    }

    /// The writes that make the texts left segment `segment`, numbered again from 0.
    fn segment(&self, segment: &[u8]) -> Result<Writes> {
        let mut renumbered = vec![u32::MAX; self.rows.len()];
        let mut texts = 0u32;
        let mut ops = VecDeque::new();
        for (number, row) in self.rows.iter().enumerate() {
            if self.removed[number] {
                continue;
            }
            renumbered[number] = texts;
            ops.push_back(WriteOp::Insert(
                Tree::Docs,
                key(&[
                    blob(segment),
                    Value::from_i64(i64::from(texts)),
                    Value::from_i64(*row),
                    blob(segment),
                ])?,
            ));
            ops.push_back(WriteOp::Insert(
                Tree::Ids,
                key(&[Value::from_i64(*row), blob(segment)])?,
            ));
            texts += 1;
        }
        if texts == 0 {
            return Ok(Writes::default());
        }
        ops.push_front(WriteOp::Insert(
            Tree::Segments,
            key(&[blob(segment), Value::from_i64(i64::from(texts))])?,
        ));
        for (trigram, numbers) in &self.postings {
            let numbers: Vec<u32> = numbers
                .iter()
                .map(|number| renumbered[*number as usize])
                .filter(|number| *number != u32::MAX)
                .collect();
            for (first, rest) in encode_blocks(&numbers) {
                ops.push_back(WriteOp::Insert(
                    Tree::Postings,
                    key(&[
                        blob(segment),
                        Value::from_i64(*trigram),
                        Value::from_i64(first),
                        Value::from_blob(rest),
                    ])?,
                ));
            }
        }
        Ok(Writes {
            ops,
            phase: WritePhase::Seek,
        })
    }
}

#[derive(Debug)]
enum WriteOp {
    Insert(Tree, ImmutableRecord),
    Delete(Tree, ImmutableRecord),
}

/// Writes decided on, done in order: `path_trie`'s, over five trees.
#[derive(Debug, Default)]
struct Writes {
    ops: VecDeque<WriteOp>,
    phase: WritePhase,
}

#[derive(Debug, Default, Clone, Copy)]
enum WritePhase {
    #[default]
    Seek,
    /// A delete's seek stopped at the end of a leaf: the entry, when its key is the divider in
    /// the page above, is the next one.
    Advance,
    Check,
    Apply,
}

impl Writes {
    fn drain(&mut self, cursors: &mut Cursors) -> IOResultOr<()> {
        while let Some(op) = self.ops.front() {
            let (WriteOp::Insert(tree, key) | WriteOp::Delete(tree, key)) = op;
            let is_delete = matches!(op, WriteOp::Delete(..));
            let cursor = cursors.get(*tree)?;
            match self.phase {
                WritePhase::Seek => {
                    let result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(key.as_record_ref()),
                        SeekOp::GE { eq_only: true },
                    ));
                    self.phase = match (is_delete, result) {
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

/// The cursors of the five trees, and of the indexed table for a query.
#[derive(Default)]
struct Cursors {
    trees: [Option<Box<dyn CursorTrait>>; 5],
    table: Option<Box<dyn CursorTrait>>,
}

impl Cursors {
    fn get(&mut self, tree: Tree) -> Result<&mut dyn CursorTrait> {
        match &mut self.trees[tree as usize] {
            Some(cursor) => Ok(cursor.as_mut()),
            None => Err(*not_open()),
        }
    }

    fn table(&mut self) -> Result<&mut dyn CursorTrait> {
        match &mut self.table {
            Some(cursor) => Ok(cursor.as_mut()),
            None => Err(*not_open()),
        }
    }
}

fn not_open() -> Box<LimboError> {
    Box::new(LimboError::InternalError(format!(
        "{TRIGRAM_INDEX_METHOD_NAME} cursors must be opened"
    )))
}

/// Reads entries in key order, from the first, or from the first at or after a key.
#[derive(Debug)]
struct Reader {
    start: Option<ImmutableRecord>,
    phase: ReadPhase,
}

#[derive(Debug, Clone, Copy)]
enum ReadPhase {
    Start,
    Next,
    Read,
}

impl Reader {
    fn from_first() -> Self {
        Self {
            start: None,
            phase: ReadPhase::Start,
        }
    }

    fn at(key: ImmutableRecord) -> Self {
        Self {
            start: Some(key),
            phase: ReadPhase::Start,
        }
    }

    /// The entry it stands at, or `None` past the last one. Called again without `advance`, it
    /// gives the same entry.
    fn step(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<Option<Vec<Value>>> {
        loop {
            match self.phase {
                ReadPhase::Start => match &self.start {
                    None => {
                        return_if_io!(cursor.rewind());
                        self.phase = ReadPhase::Read;
                    }
                    Some(key) => {
                        let result = return_if_io!(cursor.seek(
                            SeekKey::IndexKey(key.as_record_ref()),
                            SeekOp::GE { eq_only: false },
                        ));
                        self.phase = match result {
                            SeekResult::Found => ReadPhase::Read,
                            SeekResult::TryAdvance => ReadPhase::Next,
                            SeekResult::NotFound => return Ok(IOResult::Done(None)),
                        };
                    }
                },
                ReadPhase::Next => {
                    return_if_io!(cursor.next());
                    self.phase = ReadPhase::Read;
                }
                ReadPhase::Read => {
                    let record = return_if_io!(cursor.record());
                    return Ok(IOResult::Done(match record {
                        Some(record) => Some(record.get_values_owned()?),
                        None => None,
                    }));
                }
            }
        }
    }

    fn advance(&mut self) {
        self.phase = ReadPhase::Next;
    }
}

/// A removal of a text the index already holds: its identity found by rowid, then its `ids`
/// entry removed and its tombstone written.
#[derive(Debug)]
enum Removal {
    Find { row: i64, reader: Reader },
    Write(Writes),
}

impl Removal {
    fn of(row: i64) -> Result<Self> {
        Ok(Self::Find {
            row,
            reader: Reader::at(key(&[Value::from_i64(row)])?),
        })
    }

    fn run(&mut self, cursors: &mut Cursors) -> IOResultOr<()> {
        loop {
            match self {
                Self::Find { row, reader } => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Ids)?));
                    let Some(entry) = entry else {
                        return Err(corrupt("a removed text has no identity"));
                    };
                    if integer_at(&entry, 0)? != *row {
                        return Err(corrupt("a removed text has no identity"));
                    }
                    let origin = blob_at(&entry, 1)?;
                    let row = *row;
                    *self = Self::Write(Writes {
                        ops: VecDeque::from([
                            WriteOp::Delete(
                                Tree::Ids,
                                key(&[Value::from_i64(row), blob(&origin)])?,
                            ),
                            WriteOp::Insert(
                                Tree::Tombs,
                                key(&[blob(&origin), Value::from_i64(row)])?,
                            ),
                        ]),
                        phase: WritePhase::Seek,
                    });
                }
                Self::Write(writes) => return writes.drain(cursors),
            }
        }
    }
}

/// A query in progress.
struct Search {
    regex: regex::Regex,
    /// The indexed column's place in the table's records.
    text_column: usize,
    plan: plan::Query,
    /// The trigrams the plan names, as keys.
    wanted: Vec<i64>,
    step: SearchStep,
    /// Every segment, with how many texts it holds, once read.
    segments: Vec<(Vec<u8>, u32)>,
    segment: usize,
    /// The lists of the current segment's wanted trigrams read so far.
    lists: HashMap<i64, Vec<u32>>,
    candidates: Vec<u32>,
    next: usize,
    /// The rowid of the text the query stands at.
    row: Option<i64>,
}

enum SearchStep {
    Segments(Reader),
    /// Reading the list of the current segment's `wanted[index]`.
    Postings {
        index: usize,
        reader: Reader,
        numbers: Vec<u32>,
    },
    /// Taking the next candidate of the current segment.
    Candidate,
    Doc(Reader),
    Tomb {
        row: i64,
        origin: Vec<u8>,
        reader: Reader,
    },
    TableSeek(i64),
    TableRead(i64),
    Done,
}

impl Search {
    fn new(pattern: &str) -> Result<Self> {
        let regex = compile(pattern)?;
        let hir = regex_syntax::Parser::new().parse(pattern).map_err(|err| {
            LimboError::InvalidArgument(format!("{CODE_MATCH_FUNCTION_NAME}: {err}"))
        })?;
        let plan = plan::query_of(&hir);
        let wanted = plan.every_trigram().iter().map(trigram_key).collect();
        let step = if plan.op == plan::Op::None {
            SearchStep::Done
        } else {
            SearchStep::Segments(Reader::from_first())
        };
        Ok(Self {
            regex,
            text_column: 0,
            plan,
            wanted,
            step,
            segments: Vec::new(),
            segment: 0,
            lists: HashMap::new(),
            candidates: Vec::new(),
            next: 0,
            row: None,
        })
    }

    /// The step that starts the current segment, or the end.
    fn start_segment(&mut self) -> Result<SearchStep> {
        self.lists.clear();
        self.candidates.clear();
        self.next = 0;
        let Some((segment, _)) = self.segments.get(self.segment) else {
            return Ok(SearchStep::Done);
        };
        Ok(match self.wanted.first() {
            Some(trigram) => SearchStep::Postings {
                index: 0,
                reader: Reader::at(key(&[blob(segment), Value::from_i64(*trigram)])?),
                numbers: Vec::new(),
            },
            None => {
                self.evaluate();
                SearchStep::Candidate
            }
        })
    }

    fn evaluate(&mut self) {
        let texts = self.segments[self.segment].1;
        self.candidates = evaluate(&self.plan, &self.lists, texts);
        self.next = 0;
    }

    /// Moves to the next row whose text matches. Returns whether there is one.
    fn advance(&mut self, cursors: &mut Cursors) -> IOResultOr<bool> {
        loop {
            match &mut self.step {
                SearchStep::Done => {
                    self.row = None;
                    return Ok(IOResult::Done(false));
                }
                SearchStep::Segments(reader) => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Segments)?));
                    match entry {
                        Some(entry) => {
                            reader.advance();
                            let texts = u32::try_from(integer_at(&entry, 1)?)
                                .map_err(|_| *corrupt("a segment's size"))?;
                            self.segments.push((blob_at(&entry, 0)?, texts));
                        }
                        None => {
                            self.segment = 0;
                            self.step = self.start_segment()?;
                        }
                    }
                }
                SearchStep::Postings {
                    index,
                    reader,
                    numbers,
                } => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Postings)?));
                    let segment = &self.segments[self.segment].0;
                    let trigram = self.wanted[*index];
                    let ours = match &entry {
                        Some(entry) => {
                            blob_at(entry, 0)? == *segment && integer_at(entry, 1)? == trigram
                        }
                        None => false,
                    };
                    if ours {
                        let entry = entry.expect("ours");
                        decode_block(integer_at(&entry, 2)?, &blob_at(&entry, 3)?, numbers)?;
                        reader.advance();
                        continue;
                    }
                    let numbers = std::mem::take(numbers);
                    self.lists.insert(trigram, numbers);
                    let index = *index + 1;
                    self.step = match self.wanted.get(index) {
                        Some(next) => SearchStep::Postings {
                            index,
                            reader: Reader::at(key(&[blob(segment), Value::from_i64(*next)])?),
                            numbers: Vec::new(),
                        },
                        None => {
                            self.evaluate();
                            SearchStep::Candidate
                        }
                    };
                }
                SearchStep::Candidate => {
                    let Some(number) = self.candidates.get(self.next).copied() else {
                        self.segment += 1;
                        self.step = self.start_segment()?;
                        continue;
                    };
                    self.next += 1;
                    let segment = &self.segments[self.segment].0;
                    self.step = SearchStep::Doc(Reader::at(key(&[
                        blob(segment),
                        Value::from_i64(i64::from(number)),
                    ])?));
                }
                SearchStep::Doc(reader) => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Docs)?));
                    let number = self.candidates[self.next - 1];
                    let segment = &self.segments[self.segment].0;
                    let Some(entry) = entry.filter(|entry| {
                        blob_at(entry, 0).is_ok_and(|s| s == *segment)
                            && integer_at(entry, 1).is_ok_and(|n| n == i64::from(number))
                    }) else {
                        return Err(corrupt("a segment's text is missing"));
                    };
                    let (row, origin) = (integer_at(&entry, 2)?, blob_at(&entry, 3)?);
                    self.step = SearchStep::Tomb {
                        row,
                        reader: Reader::at(key(&[blob(&origin), Value::from_i64(row)])?),
                        origin,
                    };
                }
                SearchStep::Tomb {
                    row,
                    origin,
                    reader,
                } => {
                    let entry = return_if_io!(reader.step(cursors.get(Tree::Tombs)?));
                    let removed = match &entry {
                        Some(entry) => {
                            blob_at(entry, 0)? == *origin && integer_at(entry, 1)? == *row
                        }
                        None => false,
                    };
                    self.step = if removed {
                        SearchStep::Candidate
                    } else {
                        SearchStep::TableSeek(*row)
                    };
                }
                SearchStep::TableSeek(row) => {
                    let row = *row;
                    let result = return_if_io!(cursors
                        .table()?
                        .seek(SeekKey::TableRowId(row), SeekOp::GE { eq_only: true }));
                    if !matches!(result, SeekResult::Found) {
                        return Err(corrupt("an indexed row is missing from its table"));
                    }
                    self.step = SearchStep::TableRead(row);
                }
                SearchStep::TableRead(row) => {
                    let row = *row;
                    let column = self.text_column;
                    let record = return_if_io!(cursors.table()?.record());
                    let matches = match record.map(|record| record.get_value(column)).transpose()? {
                        Some(ValueRef::Text(text)) => self.regex.is_match(text.as_str()),
                        _ => false,
                    };
                    self.step = SearchStep::Candidate;
                    if matches {
                        self.row = Some(row);
                        return Ok(IOResult::Done(true));
                    }
                }
            }
        }
    }
}

/// The numbers of a segment of `texts` texts that the plan admits, given the lists of the
/// trigrams it names, ascending.
fn evaluate(query: &plan::Query, lists: &HashMap<i64, Vec<u32>>, texts: u32) -> Vec<u32> {
    let list = |trigram: &Trigram| {
        lists
            .get(&trigram_key(trigram))
            .cloned()
            .unwrap_or_default()
    };
    match query.op {
        plan::Op::All => (0..texts).collect(),
        plan::Op::None => Vec::new(),
        plan::Op::And => {
            let mut sets: Vec<Vec<u32>> = query.trigrams.iter().map(list).collect();
            sets.extend(query.sub.iter().map(|sub| evaluate(sub, lists, texts)));
            sets.sort_by_key(Vec::len);
            let mut sets = sets.into_iter();
            let Some(mut out) = sets.next() else {
                return (0..texts).collect();
            };
            for set in sets {
                out = intersect(&out, &set);
                if out.is_empty() {
                    break;
                }
            }
            out
        }
        plan::Op::Or => {
            let mut out: Vec<u32> = query.trigrams.iter().flat_map(list).collect();
            for sub in &query.sub {
                out.extend(evaluate(sub, lists, texts));
            }
            out.sort_unstable();
            out.dedup();
            out
        }
    }
}

fn intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (mut i, mut j, mut out) = (0, 0, Vec::new());
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

pub struct TrigramCursor {
    schema: BackingSchema,
    /// The indexed column's place in the table's records.
    text_column: usize,
    table_name: String,
    cursors: Cursors,
    /// An open of its cursors is under way: entered again, it goes on with the cursors it made.
    opening: bool,
    pending_store_ops: VecDeque<BackingStoreOp>,
    pending: Pending,
    /// The writes the statement's end decided on, until they are written.
    staging: Option<Writes>,
    removal: Option<Removal>,
    search: Option<Search>,
    /// `query_start` made `search` and has not yet given its first answer.
    search_starting: bool,
}

impl TrigramCursor {
    pub fn new(configuration: IndexMethodConfiguration) -> Self {
        Self {
            schema: backing_schema(&configuration),
            text_column: configuration.columns[0].pos_in_table,
            table_name: configuration.table_name.clone(),
            cursors: Cursors::default(),
            opening: false,
            pending_store_ops: VecDeque::new(),
            pending: Pending::default(),
            staging: None,
            removal: None,
            search: None,
            search_starting: false,
        }
    }

    fn drive_pending_store_ops(&mut self) -> IOResultOr<()> {
        while let Some(op) = self.pending_store_ops.front_mut() {
            return_if_io!(op.step());
            self.pending_store_ops.pop_front();
        }
        Ok(IOResult::Done(()))
    }

    fn open_cursors(&mut self, context: &IndexMethodContext, read: bool) -> IOResultOr<()> {
        let out = self.open_cursors_inner(context, read);
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.opening = false;
        }
        out
    }

    fn open_cursors_inner(&mut self, context: &IndexMethodContext, read: bool) -> IOResultOr<()> {
        if !self.opening {
            for tree in TREES {
                if self.cursors.trees[tree as usize].is_none() {
                    self.cursors.trees[tree as usize] = Some(open_store_cursor(
                        context,
                        &self.schema.indexes[tree as usize],
                    )?);
                }
            }
            if read && self.cursors.table.is_none() {
                self.cursors.table = Some(context.open_table_cursor(&self.table_name)?);
            }
            self.opening = true;
        }
        for cursor in self
            .cursors
            .trees
            .iter_mut()
            .chain(std::iter::once(&mut self.cursors.table))
            .flatten()
        {
            return_if_io!(cursor.finish_open());
        }
        Ok(IOResult::Done(()))
    }

    fn advance_search(&mut self) -> IOResultOr<bool> {
        let Some(search) = &mut self.search else {
            return Err(not_open());
        };
        search.advance(&mut self.cursors)
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
                "backing store {} not found: an index made by an earlier \
                 {TRIGRAM_INDEX_METHOD_NAME} must be dropped and made again",
                index.name
            ))
        })?
        .open_cursor()
}

/// A segment's id: 16 bytes from the database's IO random source, so seeded simulator runs
/// replay byte for byte, as `fts` draws its segments' ids.
fn segment_id(context: &IndexMethodContext) -> Result<Vec<u8>> {
    let pager = context
        .connection()?
        .get_pager_from_database_index(&context.database().id)?;
    let mut id = Vec::with_capacity(SEGMENT_ID_LEN);
    for _ in 0..SEGMENT_ID_LEN / 8 {
        id.extend_from_slice(&(pager.io.generate_random_number() as u64).to_be_bytes());
    }
    Ok(id)
}

impl IndexMethodCursor for TrigramCursor {
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
        self.open_cursors(context, true)
    }

    fn open_write(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.open_cursors(context, false)
    }

    fn close(&mut self, _context: &IndexMethodContext) {
        self.cursors = Cursors::default();
        self.opening = false;
    }

    /// Takes the text into the statement's segment. It reads and writes nothing until the
    /// statement ends.
    fn insert(&mut self, values: &[Register]) -> IOResultOr<()> {
        let Some(row) = values[1].get_value().as_int() else {
            return Err(Box::new(LimboError::InternalError(format!(
                "{TRIGRAM_INDEX_METHOD_NAME}: the second value must be the rowid"
            ))));
        };
        // A row whose value is not text is not indexed: no expression matches it.
        if let Some(text) = values[0].get_value().to_text() {
            self.pending.add(row, text.as_bytes());
        }
        Ok(IOResult::Done(()))
    }

    /// Takes the text back from the statement's segment, or writes its tombstone.
    fn delete(&mut self, values: &[Register]) -> IOResultOr<()> {
        if self.removal.is_none() {
            let Some(row) = values[1].get_value().as_int() else {
                return Err(Box::new(LimboError::InternalError(format!(
                    "{TRIGRAM_INDEX_METHOD_NAME}: the second value must be the rowid"
                ))));
            };
            if values[0].get_value().to_text().is_none() || self.pending.take_back(row) {
                return Ok(IOResult::Done(()));
            }
            self.removal = Some(Removal::of(row)?);
        }
        let removal = self.removal.as_mut().expect("made above");
        let out = removal.run(&mut self.cursors);
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.removal = None;
        }
        out
    }

    /// Writes the statement's texts as one segment.
    fn stage_statement_commit(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.staging.is_none() {
            if self.pending.rows.is_empty() {
                return Ok(IOResult::Done(()));
            }
            let segment = segment_id(context)?;
            self.staging = Some(self.pending.segment(&segment)?);
        }
        let staging = self.staging.as_mut().expect("made above");
        let out = staging.drain(&mut self.cursors);
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.staging = None;
            self.pending = Pending::default();
        }
        out
    }

    fn abort_statement(&mut self, _context: &IndexMethodContext) {
        self.pending = Pending::default();
        self.staging = None;
        self.removal = None;
    }

    /// `values` holds the pattern's number (there is one pattern), then the expression.
    fn query_start(&mut self, values: &[Register]) -> IOResultOr<bool> {
        if !self.search_starting {
            let Some(pattern) = values[1].get_value().to_text() else {
                self.search = None;
                return Ok(IOResult::Done(false));
            };
            let mut search = Search::new(pattern)?;
            search.text_column = self.text_column;
            self.search = Some(search);
            self.search_starting = true;
        }
        let out = self.advance_search();
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.search_starting = false;
        }
        out
    }

    fn query_next(&mut self) -> IOResultOr<bool> {
        if self.search.is_none() {
            return Ok(IOResult::Done(false));
        }
        self.advance_search()
    }

    fn query_rowid(&mut self) -> IOResultOr<Option<i64>> {
        Ok(IOResult::Done(
            self.search.as_ref().and_then(|search| search.row),
        ))
    }

    fn query_column(&mut self, _: usize) -> IOResultOr<Value> {
        Err(Box::new(LimboError::InternalError(format!(
            "{TRIGRAM_INDEX_METHOD_NAME} serves no column of its own"
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_gives_back_the_numbers_it_was_made_of() {
        let numbers: Vec<u32> = (0..1000u32)
            .map(|n| n * 7 + n / 3)
            .chain([1 << 20, (1 << 31) + 5])
            .collect();
        let mut back = Vec::new();
        let blocks = encode_blocks(&numbers);
        assert_eq!(blocks.len(), numbers.len().div_ceil(BLOCK));
        for (first, rest) in &blocks {
            decode_block(*first, rest, &mut back).unwrap();
        }
        assert_eq!(back, numbers);
        assert!(
            decode_block(0, &[0x80], &mut Vec::new()).is_err(),
            "a varint cut short"
        );
    }

    /// A segment's candidates: an AND of trigrams is the texts holding all of them, an OR those
    /// holding any, and a query that names none every text.
    #[test]
    fn a_query_admits_the_texts_its_trigrams_lists_say() {
        let query =
            |pattern: &str| plan::query_of(&regex_syntax::Parser::new().parse(pattern).unwrap());
        let key = |t: &str| trigram_key(&[t.as_bytes()[0], t.as_bytes()[1], t.as_bytes()[2]]);
        let lists: HashMap<i64, Vec<u32>> = [
            (key("abc"), vec![0, 2, 4, 6]),
            (key("bcd"), vec![1, 2, 3, 6]),
            (key("xyz"), vec![5]),
        ]
        .into_iter()
        .collect();
        assert_eq!(evaluate(&query("abcd"), &lists, 8), [2, 6]);
        assert_eq!(evaluate(&query("abc|xyz"), &lists, 8), [0, 2, 4, 5, 6]);
        assert_eq!(
            evaluate(&query("a.c"), &lists, 8),
            (0..8).collect::<Vec<u32>>()
        );
        assert!(evaluate(&query("qqq"), &lists, 8).is_empty());
    }

    #[test]
    fn code_match_runs_the_expression_on_the_text() {
        let text = Value::build_text("fn main() {}\n".to_owned());
        let matches =
            |pattern: &str| code_match(&text, &Value::build_text(pattern.to_owned())).unwrap();
        assert_eq!(matches(r"fn\s+main"), Value::from_i64(1));
        assert_eq!(matches(r"^main"), Value::from_i64(0));
        assert_eq!(
            code_match(&Value::Null, &Value::build_text("a".to_owned())).unwrap(),
            Value::Null
        );
        assert!(code_match(&text, &Value::build_text("(".to_owned())).is_err());
    }
}
