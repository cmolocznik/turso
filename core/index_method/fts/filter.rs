//! The document filter of a filtered FTS query (`fts_filtered_score`), read
//! from the query's JSON argument and evaluated per segment over the index's
//! fast fields, and the BM25 statistics counted over the documents it admits.

use super::intervals_of;
use crate::{LimboError, Result};
use rustc_hash::FxHashMap as HashMap;
use tantivy::{
    columnar::{BytesColumn, Column},
    query::Bm25StatisticsProvider,
    schema::{Field, IndexRecordOption},
    DocId, DocSet, SegmentReader, Term, TERMINATED,
};

/// Deepest nesting of `all`/`any` a filter may use.
const MAX_FILTER_DEPTH: usize = 32;

/// A filter over the index's filter columns and event intervals.
#[derive(Debug, Clone)]
pub(crate) enum Filter {
    All(Vec<Filter>),
    Any(Vec<Filter>),
    /// The column's value is one of these.
    In(String, Vec<i64>),
    /// `lo <= value < hi`; an absent bound is open, a missing value never matches.
    Range(String, Option<i64>, Option<i64>),
    /// The column has no value.
    Null(String),
    /// Some event interval lies inside the window (the cognition store's
    /// `window_terms` containment rule).
    Within(Option<i64>, Option<i64>),
}

fn invalid(what: impl std::fmt::Display) -> LimboError {
    LimboError::InvalidArgument(format!("FTS filter: {what}"))
}

impl Filter {
    /// Parse the JSON filter, refusing a column that is not a filter column of
    /// the index and `within` on an index with no interval field.
    pub(crate) fn parse(text: &str, columns: &[(String, Field)], intervals: bool) -> Result<Self> {
        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|e| invalid(format!("not JSON: {e}")))?;
        Self::from_value(&value, columns, intervals, 0)
    }

    fn from_value(
        value: &serde_json::Value,
        columns: &[(String, Field)],
        intervals: bool,
        depth: usize,
    ) -> Result<Self> {
        if depth > MAX_FILTER_DEPTH {
            return Err(invalid(format!("nests deeper than {MAX_FILTER_DEPTH}")));
        }
        let object = value
            .as_object()
            .filter(|object| object.len() == 1)
            .ok_or_else(|| invalid("each filter is an object with exactly one key"))?;
        let (key, arg) = object.iter().next().expect("one key");
        let column = |value: &serde_json::Value| -> Result<String> {
            let name = value
                .as_str()
                .ok_or_else(|| invalid("a column is named by a string"))?;
            columns
                .iter()
                .find(|(column, _)| column.eq_ignore_ascii_case(name))
                .map(|(column, _)| column.clone())
                .ok_or_else(|| invalid(format!("'{name}' is not a filter column of the index")))
        };
        let bound = |value: &serde_json::Value| -> Result<Option<i64>> {
            match value {
                serde_json::Value::Null => Ok(None),
                value => value
                    .as_i64()
                    .map(Some)
                    .ok_or_else(|| invalid("a bound is an integer or null")),
            }
        };
        let pair = |arg: &serde_json::Value, len: usize| -> Result<Vec<serde_json::Value>> {
            arg.as_array()
                .filter(|items| items.len() == len)
                .cloned()
                .ok_or_else(|| invalid(format!("'{key}' takes an array of {len}")))
        };
        match key.as_str() {
            "all" | "any" => {
                let items = arg
                    .as_array()
                    .ok_or_else(|| invalid(format!("'{key}' takes an array of filters")))?;
                let items = items
                    .iter()
                    .map(|item| Self::from_value(item, columns, intervals, depth + 1))
                    .collect::<Result<Vec<_>>>()?;
                Ok(if key == "all" {
                    Self::All(items)
                } else {
                    Self::Any(items)
                })
            }
            "in" => {
                let items = pair(arg, 2)?;
                let values = items[1]
                    .as_array()
                    .ok_or_else(|| invalid("'in' takes a column and an array of integers"))?
                    .iter()
                    .map(|v| {
                        v.as_i64()
                            .ok_or_else(|| invalid("'in' values are integers"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Self::In(column(&items[0])?, values))
            }
            "range" => {
                let items = pair(arg, 3)?;
                Ok(Self::Range(
                    column(&items[0])?,
                    bound(&items[1])?,
                    bound(&items[2])?,
                ))
            }
            "null" => Ok(Self::Null(column(arg)?)),
            "within" => {
                if !intervals {
                    return Err(invalid("'within' needs an index with an interval_field"));
                }
                let items = pair(arg, 2)?;
                Ok(Self::Within(bound(&items[0])?, bound(&items[1])?))
            }
            other => Err(invalid(format!("unknown filter '{other}'"))),
        }
    }
}

/// A set of documents of one segment.
#[derive(Debug, Clone)]
pub(crate) struct DocBits {
    words: Vec<u64>,
    len: u32,
}

impl DocBits {
    pub(crate) fn empty(len: u32) -> Self {
        Self {
            words: vec![0; (len as usize).div_ceil(64)],
            len,
        }
    }

    fn full(len: u32) -> Self {
        let mut bits = Self {
            words: vec![u64::MAX; (len as usize).div_ceil(64)],
            len,
        };
        bits.trim();
        bits
    }

    fn trim(&mut self) {
        let tail = self.len % 64;
        if tail != 0 {
            if let Some(last) = self.words.last_mut() {
                *last &= (1u64 << tail) - 1;
            }
        }
    }

    pub(crate) fn insert(&mut self, doc: DocId) {
        self.words[doc as usize / 64] |= 1 << (doc % 64);
    }

    pub(crate) fn contains(&self, doc: DocId) -> bool {
        doc < self.len && self.words[doc as usize / 64] & (1 << (doc % 64)) != 0
    }

    fn and(&mut self, other: &Self) {
        for (word, other) in self.words.iter_mut().zip(&other.words) {
            *word &= other;
        }
    }

    fn or(&mut self, other: &Self) {
        for (word, other) in self.words.iter_mut().zip(&other.words) {
            *word |= other;
        }
    }

    pub(crate) fn count(&self) -> u64 {
        self.words
            .iter()
            .map(|word| u64::from(word.count_ones()))
            .sum()
    }

    pub(crate) fn docs(&self) -> impl Iterator<Item = DocId> + '_ {
        self.words.iter().enumerate().flat_map(|(i, &word)| {
            let mut word = word;
            std::iter::from_fn(move || {
                if word == 0 {
                    return None;
                }
                let bit = word.trailing_zeros();
                word &= word - 1;
                Some(i as u32 * 64 + bit)
            })
        })
    }
}

/// A segment's interval column and each of its distinct values decoded.
type DecodedIntervals = (BytesColumn, Vec<Vec<(i64, i64)>>);

/// The columns one segment's filter reads, opened once per segment.
struct SegmentColumns<'a> {
    reader: &'a SegmentReader,
    columns: HashMap<String, Column<i64>>,
    intervals: Option<Option<DecodedIntervals>>,
    interval_column: Option<&'a str>,
}

impl SegmentColumns<'_> {
    fn column(&mut self, name: &str) -> Result<&Column<i64>> {
        if !self.columns.contains_key(name) {
            let column =
                self.reader.fast_fields().i64(name).map_err(|e| {
                    LimboError::InternalError(format!("FTS filter column {name}: {e}"))
                })?;
            self.columns.insert(name.to_string(), column);
        }
        Ok(&self.columns[name])
    }

    /// The segment's interval column with every distinct value decoded once.
    fn intervals(&mut self) -> Result<Option<&DecodedIntervals>> {
        if self.intervals.is_none() {
            let name = self
                .interval_column
                .ok_or_else(|| invalid("'within' needs an index with an interval_field"))?;
            let column = self
                .reader
                .fast_fields()
                .bytes(name)
                .map_err(|e| LimboError::InternalError(format!("FTS interval column: {e}")))?;
            self.intervals = Some(match column {
                None => None,
                Some(column) => {
                    let mut decoded = Vec::with_capacity(column.num_terms());
                    let mut stream = column.dictionary().stream().map_err(|e| {
                        LimboError::InternalError(format!("FTS interval terms: {e}"))
                    })?;
                    while stream.advance() {
                        decoded.push(intervals_of(stream.key()).collect());
                    }
                    Some((column, decoded))
                }
            });
        }
        Ok(self.intervals.as_ref().expect("set above").as_ref())
    }
}

fn within(intervals: &[(i64, i64)], lo: Option<i64>, hi: Option<i64>) -> bool {
    intervals.iter().any(|&(from, to)| {
        let from_known = from != i64::MIN;
        let to_known = to != i64::MAX;
        match (lo, hi) {
            (Some(lo), Some(hi)) => from_known && from >= lo && from < hi && to_known && to <= hi,
            (Some(lo), None) => from_known && from >= lo,
            (None, Some(hi)) => from < hi && to_known && to <= hi,
            (None, None) => true,
        }
    })
}

fn evaluate(filter: &Filter, columns: &mut SegmentColumns<'_>, max_doc: u32) -> Result<DocBits> {
    Ok(match filter {
        Filter::All(items) => {
            let mut bits = DocBits::full(max_doc);
            for item in items {
                bits.and(&evaluate(item, columns, max_doc)?);
            }
            bits
        }
        Filter::Any(items) => {
            let mut bits = DocBits::empty(max_doc);
            for item in items {
                bits.or(&evaluate(item, columns, max_doc)?);
            }
            bits
        }
        Filter::In(name, values) => {
            let column = columns.column(name)?;
            let mut bits = DocBits::empty(max_doc);
            for doc in 0..max_doc {
                if column
                    .first(doc)
                    .is_some_and(|value| values.contains(&value))
                {
                    bits.insert(doc);
                }
            }
            bits
        }
        Filter::Range(name, lo, hi) => {
            let column = columns.column(name)?;
            let mut bits = DocBits::empty(max_doc);
            for doc in 0..max_doc {
                if column.first(doc).is_some_and(|value| {
                    lo.is_none_or(|lo| value >= lo) && hi.is_none_or(|hi| value < hi)
                }) {
                    bits.insert(doc);
                }
            }
            bits
        }
        Filter::Null(name) => {
            let column = columns.column(name)?;
            let mut bits = DocBits::empty(max_doc);
            for doc in 0..max_doc {
                if column.first(doc).is_none() {
                    bits.insert(doc);
                }
            }
            bits
        }
        Filter::Within(lo, hi) => {
            let mut bits = DocBits::empty(max_doc);
            if let Some((column, decoded)) = columns.intervals()? {
                let held: Vec<bool> = decoded.iter().map(|ivs| within(ivs, *lo, *hi)).collect();
                for doc in 0..max_doc {
                    if column
                        .term_ords(doc)
                        .any(|ord| held.get(ord as usize).copied().unwrap_or(false))
                    {
                        bits.insert(doc);
                    }
                }
            }
            bits
        }
    })
}

/// The live documents of a segment the filter admits.
pub(crate) fn admitted(
    filter: &Filter,
    reader: &SegmentReader,
    interval_column: Option<&str>,
) -> Result<DocBits> {
    let max_doc = reader.max_doc();
    let mut columns = SegmentColumns {
        reader,
        columns: HashMap::default(),
        intervals: None,
        interval_column,
    };
    let mut bits = evaluate(filter, &mut columns, max_doc)?;
    if let Some(alive) = reader.alive_bitset() {
        let mut live = DocBits::empty(max_doc);
        for doc in alive.iter_alive() {
            live.insert(doc);
        }
        bits.and(&live);
    }
    Ok(bits)
}

/// BM25 statistics counted over exactly the admitted documents: document
/// count, total tokens per field (the documents' field norms) and the
/// document frequency of every term of the query.
pub(crate) struct FilteredStatistics {
    pub(crate) documents: u64,
    tokens: HashMap<Field, u64>,
    doc_freqs: HashMap<Term, u64>,
}

impl FilteredStatistics {
    pub(crate) fn count(
        readers: &[SegmentReader],
        admitted: &[DocBits],
        fields: &[Field],
        terms: &[Term],
    ) -> Result<Self> {
        let map =
            |e: tantivy::TantivyError| LimboError::InternalError(format!("FTS statistics: {e}"));
        let mut documents = 0;
        let mut tokens: HashMap<Field, u64> = HashMap::default();
        let mut doc_freqs: HashMap<Term, u64> = terms.iter().map(|t| (t.clone(), 0)).collect();
        for (reader, bits) in readers.iter().zip(admitted) {
            documents += bits.count();
            for &field in fields {
                let norms = reader.get_fieldnorms_reader(field).map_err(map)?;
                let sum: u64 = bits.docs().map(|doc| u64::from(norms.fieldnorm(doc))).sum();
                *tokens.entry(field).or_default() += sum;
            }
            for term in terms {
                let inverted = reader.inverted_index(term.field()).map_err(map)?;
                let Some(mut postings) = inverted
                    .read_postings(term, IndexRecordOption::Basic)
                    .map_err(|e| LimboError::InternalError(format!("FTS statistics: {e}")))?
                else {
                    continue;
                };
                let mut count = 0;
                let mut doc = postings.doc();
                while doc != TERMINATED {
                    if bits.contains(doc) {
                        count += 1;
                    }
                    doc = postings.advance();
                }
                *doc_freqs.get_mut(term).expect("seeded with every term") += count;
            }
        }
        Ok(Self {
            documents,
            tokens,
            doc_freqs,
        })
    }
}

impl Bm25StatisticsProvider for FilteredStatistics {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        Ok(self.tokens.get(&field).copied().unwrap_or(0))
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.documents)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        self.doc_freqs.get(term).copied().ok_or_else(|| {
            tantivy::TantivyError::InvalidArgument(
                "FTS filtered statistics: a term outside the query's own terms".into(),
            )
        })
    }
}
