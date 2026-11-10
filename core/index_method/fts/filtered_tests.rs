//! `fts_filtered_score`: the top documents a filter admits, scored with every
//! BM25 statistic counted over exactly that set, held to an exact oracle.

use super::*;
use crate::{ConnectOpts, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect};
use std::collections::HashMap as StdHashMap;

/// A small deterministic generator, so a failure names its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    /// A word index skewed toward the common words.
    fn word(&mut self, vocab: usize) -> usize {
        let a = self.below(vocab as u64) as usize;
        let b = self.below(vocab as u64) as usize;
        a.min(b)
    }
}

const VOCAB: usize = 40;

fn word(i: usize) -> String {
    format!("w{i}x")
}

#[derive(Clone, Debug)]
struct Doc {
    words: Vec<usize>,
    shape: Option<i64>,
    closed: i64,
    observed: Option<i64>,
    intervals: Vec<(i64, i64)>,
}

fn packed(intervals: &[(i64, i64)]) -> String {
    let mut bytes = Vec::new();
    for (from, to) in intervals {
        bytes.extend_from_slice(&from.to_le_bytes());
        bytes.extend_from_slice(&to.to_le_bytes());
    }
    format!(
        "X'{}'",
        bytes.iter().map(|b| format!("{b:02X}")).collect::<String>()
    )
}

fn random_doc(rng: &mut Rng) -> Doc {
    let len = 1 + rng.below(25) as usize;
    let mut intervals = Vec::new();
    for _ in 0..rng.below(3) {
        let from = if rng.below(6) == 0 {
            i64::MIN
        } else {
            rng.below(100) as i64
        };
        let to = if rng.below(6) == 0 {
            i64::MAX
        } else {
            from.max(0) + 1 + rng.below(30) as i64
        };
        intervals.push((from, to));
    }
    Doc {
        words: (0..len).map(|_| rng.word(VOCAB)).collect(),
        shape: (rng.below(8) != 0).then(|| 1 + rng.below(5) as i64),
        closed: (rng.below(3) == 0) as i64,
        observed: (rng.below(5) != 0).then(|| rng.below(100) as i64),
        intervals,
    }
}

fn sql_value(value: Option<i64>) -> String {
    value.map_or("NULL".into(), |v| v.to_string())
}

fn insert_sql(rows: &[(i64, Doc)]) -> String {
    let values: Vec<String> = rows
        .iter()
        .map(|(id, doc)| {
            let body: Vec<String> = doc.words.iter().map(|&w| word(w)).collect();
            format!(
                "({id}, '{}', {}, {}, {}, {})",
                body.join(" "),
                sql_value(doc.shape),
                doc.closed,
                sql_value(doc.observed),
                if doc.intervals.is_empty() {
                    "NULL".to_string()
                } else {
                    packed(&doc.intervals)
                }
            )
        })
        .collect();
    format!("INSERT INTO docs VALUES {}", values.join(","))
}

/// The filter as the oracle reads it.
#[derive(Clone, Debug)]
enum F {
    All(Vec<F>),
    Any(Vec<F>),
    In(&'static str, Vec<i64>),
    Range(&'static str, Option<i64>, Option<i64>),
    Null(&'static str),
    Within(Option<i64>, Option<i64>),
}

fn json_bound(b: Option<i64>) -> String {
    b.map_or("null".into(), |v| v.to_string())
}

impl F {
    fn json(&self) -> String {
        match self {
            F::All(items) => format!(
                "{{\"all\":[{}]}}",
                items.iter().map(F::json).collect::<Vec<_>>().join(",")
            ),
            F::Any(items) => format!(
                "{{\"any\":[{}]}}",
                items.iter().map(F::json).collect::<Vec<_>>().join(",")
            ),
            F::In(col, values) => format!(
                "{{\"in\":[\"{col}\",[{}]]}}",
                values
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            F::Range(col, lo, hi) => format!(
                "{{\"range\":[\"{col}\",{},{}]}}",
                json_bound(*lo),
                json_bound(*hi)
            ),
            F::Null(col) => format!("{{\"null\":\"{col}\"}}"),
            F::Within(lo, hi) => {
                format!("{{\"within\":[{},{}]}}", json_bound(*lo), json_bound(*hi))
            }
        }
    }

    fn admits(&self, doc: &Doc) -> bool {
        let column = |col: &str| match col {
            "shape" => doc.shape,
            "closed" => Some(doc.closed),
            "observed" => doc.observed,
            other => panic!("no column {other}"),
        };
        match self {
            F::All(items) => items.iter().all(|f| f.admits(doc)),
            F::Any(items) => items.iter().any(|f| f.admits(doc)),
            F::In(col, values) => column(col).is_some_and(|v| values.contains(&v)),
            F::Range(col, lo, hi) => column(col)
                .is_some_and(|v| lo.is_none_or(|lo| v >= lo) && hi.is_none_or(|hi| v < hi)),
            F::Null(col) => column(col).is_none(),
            F::Within(lo, hi) => doc.intervals.iter().any(|&(from, to)| {
                let (from_known, to_known) = (from != i64::MIN, to != i64::MAX);
                match (lo, hi) {
                    (Some(lo), Some(hi)) => {
                        from_known && from >= *lo && from < *hi && to_known && to <= *hi
                    }
                    (Some(lo), None) => from_known && from >= *lo,
                    (None, Some(hi)) => from < *hi && to_known && to <= *hi,
                    (None, None) => true,
                }
            }),
        }
    }
}

fn random_bound(rng: &mut Rng) -> Option<i64> {
    (rng.below(4) != 0).then(|| rng.below(110) as i64)
}

fn random_filter(rng: &mut Rng, depth: u32) -> F {
    match rng.below(if depth >= 2 { 5 } else { 7 }) {
        0 => F::In(
            "shape",
            (0..1 + rng.below(3))
                .map(|_| 1 + rng.below(5) as i64)
                .collect(),
        ),
        1 => F::Range("closed", Some(0), Some(1)),
        2 => {
            let lo = random_bound(rng);
            let hi = random_bound(rng);
            F::Range("observed", lo, hi)
        }
        3 => F::Null(if rng.below(2) == 0 {
            "shape"
        } else {
            "observed"
        }),
        4 => {
            let lo = random_bound(rng);
            let hi = random_bound(rng);
            F::Within(lo, hi)
        }
        5 => F::All(
            (0..1 + rng.below(3))
                .map(|_| random_filter(rng, depth + 1))
                .collect(),
        ),
        _ => F::Any(
            (0..1 + rng.below(3))
                .map(|_| random_filter(rng, depth + 1))
                .collect(),
        ),
    }
}

/// BM25 as Tantivy scores it (k1 1.2, b 0.75; every document here is shorter
/// than the length past which field norms are quantized), with the document
/// count, the average length and each term's document frequency counted over
/// the admitted documents.
fn oracle(docs: &StdHashMap<i64, Doc>, filter: &F, terms: &[usize]) -> Vec<(f64, i64)> {
    let admitted: Vec<(&i64, &Doc)> = docs.iter().filter(|(_, d)| filter.admits(d)).collect();
    let n = admitted.len() as f64;
    if n == 0.0 {
        return Vec::new();
    }
    let total: f64 = admitted.iter().map(|(_, d)| d.words.len() as f64).sum();
    let avg = total / n;
    let mut terms = terms.to_vec();
    terms.sort_unstable();
    terms.dedup();
    let mut scored = Vec::new();
    for (id, doc) in &admitted {
        let mut score = 0.0;
        let mut matched = false;
        for &term in &terms {
            let tf = doc.words.iter().filter(|&&w| w == term).count() as f64;
            if tf == 0.0 {
                continue;
            }
            matched = true;
            let df = admitted
                .iter()
                .filter(|(_, d)| d.words.contains(&term))
                .count() as f64;
            let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
            let len = doc.words.len() as f64;
            score += idf * (1.2 + 1.0) * tf / (tf + 1.2 * (1.0 - 0.75 + 0.75 * len / avg));
        }
        if matched {
            scored.push((score, **id));
        }
    }
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    scored
}

fn open(mvcc: bool) -> (tempfile::TempDir, Arc<Database>, Arc<Connection>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("filtered.db");
    let io: Arc<dyn crate::IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    if mvcc {
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    conn.execute(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT, shape INTEGER, closed INTEGER, observed INTEGER, ivs BLOB)",
    )
    .unwrap();
    conn.execute(
        "CREATE INDEX docs_fts ON docs USING fts(body, shape, closed, observed, ivs) \
         WITH (filter_fields = 'shape,closed,observed', interval_field = 'ivs')",
    )
    .unwrap();
    (dir, db, conn)
}

fn filtered(conn: &Arc<Connection>, terms: &[usize], filter: &str, limit: i64) -> Vec<(f64, i64)> {
    let query: Vec<String> = terms.iter().map(|&t| word(t)).collect();
    let mut statement = conn
        .prepare(
            "SELECT id, fts_filtered_score(body, ?1, ?2) AS score FROM docs ORDER BY score DESC LIMIT ?3",
        )
        .unwrap();
    statement
        .bind_at(1.try_into().unwrap(), Value::build_text(query.join(" ")))
        .unwrap();
    statement
        .bind_at(2.try_into().unwrap(), Value::build_text(filter.to_string()))
        .unwrap();
    statement
        .bind_at(3.try_into().unwrap(), Value::from_i64(limit))
        .unwrap();
    statement
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| (row[1].as_float(), row[0].as_int().unwrap()))
        .collect()
}

/// The index's answer equals the oracle's: the same scores in order, and the
/// same documents wherever a score is not tied with the last one kept.
fn assert_agrees(got: &[(f64, i64)], want: &[(f64, i64)], limit: usize, context: &str) {
    let want = &want[..want.len().min(limit)];
    assert_eq!(got.len(), want.len(), "{context}: hit count");
    for (g, w) in got.iter().zip(want) {
        assert!(
            (g.0 - w.0).abs() <= 1e-3 * w.0.abs().max(1.0),
            "{context}: score {} where the oracle has {} (got {got:?}, want {want:?})",
            g.0,
            w.0
        );
    }
    if let Some(last) = want.last() {
        let strict = |hits: &[(f64, i64)]| -> Vec<i64> {
            let mut ids: Vec<i64> = hits
                .iter()
                .filter(|h| h.0 > last.0 + 1e-3 * last.0.abs().max(1.0))
                .map(|h| h.1)
                .collect();
            ids.sort_unstable();
            ids
        };
        assert_eq!(
            strict(got),
            strict(want),
            "{context}: documents above the last score"
        );
    }
}

/// Random corpora written over several statements (so several segments),
/// with deletes and updates that close documents and move their shape, read
/// under random filters in WAL and MVCC mode: every answer equals the exact
/// BM25 oracle over the admitted set, including after OPTIMIZE INDEX merges.
#[test]
fn filtered_top_k_equals_the_exact_oracle_over_the_admitted_set() {
    for mvcc in [false, true] {
        for seed in [7u64, 11, 23] {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let (_dir, _db, conn) = open(mvcc);
            let mut docs: StdHashMap<i64, Doc> = StdHashMap::new();
            let mut next_id = 1i64;
            let mut queries = 0;
            let mut answered = 0;
            for round in 0..6 {
                let batch: Vec<(i64, Doc)> = (0..60)
                    .map(|_| {
                        let id = next_id;
                        next_id += 1;
                        (id, random_doc(&mut rng))
                    })
                    .collect();
                conn.execute(insert_sql(&batch)).unwrap();
                docs.extend(batch);
                // Deletes and updates of earlier documents.
                let ids: Vec<i64> = docs.keys().copied().collect();
                for _ in 0..8 {
                    let id = ids[rng.below(ids.len() as u64) as usize];
                    if !docs.contains_key(&id) {
                        continue;
                    }
                    if rng.below(2) == 0 {
                        conn.execute(format!("DELETE FROM docs WHERE id = {id}"))
                            .unwrap();
                        docs.remove(&id);
                    } else {
                        let shape = 1 + rng.below(5) as i64;
                        conn.execute(format!(
                            "UPDATE docs SET closed = 1, shape = {shape} WHERE id = {id}"
                        ))
                        .unwrap();
                        let doc = docs.get_mut(&id).unwrap();
                        doc.closed = 1;
                        doc.shape = Some(shape);
                    }
                }
                if round == 4 {
                    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
                }
                for _ in 0..12 {
                    let filter = random_filter(&mut rng, 0);
                    let terms: Vec<usize> =
                        (0..1 + rng.below(3)).map(|_| rng.word(VOCAB)).collect();
                    let limit = [1i64, 5, 10, 50][rng.below(4) as usize];
                    let got = filtered(&conn, &terms, &filter.json(), limit);
                    let want = oracle(&docs, &filter, &terms);
                    answered += usize::from(!want.is_empty());
                    assert_agrees(
                        &got,
                        &want,
                        limit as usize,
                        &format!(
                            "mvcc={mvcc} seed={seed} round={round} filter={} terms={terms:?}",
                            filter.json()
                        ),
                    );
                    queries += 1;
                }
            }
            assert_eq!(queries, 72);
            assert!(
                answered >= 36,
                "mvcc={mvcc} seed={seed}: only {answered} of 72 queries had an answer"
            );
        }
    }
}

/// Statistics are counted over the admitted set, not the index: the same
/// document scores differently when the filter changes which other documents
/// count, exactly as the oracle says.
#[test]
fn a_filter_that_changes_the_admitted_set_changes_the_weights() {
    let (_dir, _db, conn) = open(false);
    // 'w1x' is rare among shape 1 and common among shape 2.
    let mut docs = StdHashMap::new();
    let mut rows = Vec::new();
    for id in 1..=20i64 {
        let shape = if id <= 10 { 1 } else { 2 };
        let words = if id == 1 || id > 10 {
            vec![1, 2]
        } else {
            vec![2, 3]
        };
        let doc = Doc {
            words,
            shape: Some(shape),
            closed: 0,
            observed: None,
            intervals: Vec::new(),
        };
        rows.push((id, doc.clone()));
        docs.insert(id, doc);
    }
    conn.execute(insert_sql(&rows)).unwrap();
    let only_one = F::In("shape", vec![1]);
    let both = F::In("shape", vec![1, 2]);
    let narrow = filtered(&conn, &[1], &only_one.json(), 5);
    let wide = filtered(&conn, &[1], &both.json(), 50);
    assert_agrees(&narrow, &oracle(&docs, &only_one, &[1]), 5, "shape 1");
    assert_agrees(&wide, &oracle(&docs, &both, &[1]), 50, "shapes 1 and 2");
    let one_narrow = narrow.iter().find(|h| h.1 == 1).unwrap().0;
    let one_wide = wide.iter().find(|h| h.1 == 1).unwrap().0;
    assert!(
        one_narrow > one_wide * 1.5,
        "document 1 weighs {one_narrow} among shape 1 and {one_wide} among both"
    );
}

/// Under MVCC two BEGIN CONCURRENT writers add documents, a rolled-back one
/// adds none, and a merge runs between them: the filtered answer is the
/// oracle's over what committed.
#[test]
fn concurrent_writers_a_rollback_and_a_merge_keep_the_filtered_answer_exact() {
    let (_dir, db, conn) = open(true);
    let mut rng = Rng(0xC0FFEE);
    let mut docs: StdHashMap<i64, Doc> = StdHashMap::new();
    let first: Vec<(i64, Doc)> = (1..=80).map(|id| (id, random_doc(&mut rng))).collect();
    conn.execute(insert_sql(&first)).unwrap();
    docs.extend(first);
    let a = db.connect(ConnectOpts::new()).unwrap();
    let b = db.connect(ConnectOpts::new()).unwrap();
    let c = db.connect(ConnectOpts::new()).unwrap();
    let to_a: Vec<(i64, Doc)> = (100..=140).map(|id| (id, random_doc(&mut rng))).collect();
    let to_b: Vec<(i64, Doc)> = (200..=240).map(|id| (id, random_doc(&mut rng))).collect();
    let to_c: Vec<(i64, Doc)> = (300..=340).map(|id| (id, random_doc(&mut rng))).collect();
    a.execute("BEGIN CONCURRENT").unwrap();
    b.execute("BEGIN CONCURRENT").unwrap();
    c.execute("BEGIN CONCURRENT").unwrap();
    a.execute(insert_sql(&to_a)).unwrap();
    b.execute(insert_sql(&to_b)).unwrap();
    c.execute(insert_sql(&to_c)).unwrap();
    // A writer sees its own uncommitted documents.
    let probe = F::Range("closed", Some(0), Some(2));
    let mut own = docs.clone();
    own.extend(to_a.clone());
    assert_agrees(
        &filtered(&a, &[0, 1], &probe.json(), 500),
        &oracle(&own, &probe, &[0, 1]),
        500,
        "own writes",
    );
    a.execute("COMMIT").unwrap();
    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
    b.execute("COMMIT").unwrap();
    c.execute("ROLLBACK").unwrap();
    docs.extend(to_a);
    docs.extend(to_b);
    for seed in 0..20 {
        let mut rng = Rng(seed + 99);
        let filter = random_filter(&mut rng, 0);
        let terms: Vec<usize> = (0..1 + rng.below(3)).map(|_| rng.word(VOCAB)).collect();
        assert_agrees(
            &filtered(&conn, &terms, &filter.json(), 20),
            &oracle(&docs, &filter, &terms),
            20,
            &format!("after commits, filter={}", filter.json()),
        );
    }
}

/// A filter naming a column that is not a filter column, malformed JSON, and
/// `within` on an index with no interval field are refused, not ignored.
#[test]
fn a_filter_the_index_cannot_apply_is_refused() {
    let (_dir, _db, conn) = open(false);
    conn.execute("INSERT INTO docs VALUES (1, 'w1x', 1, 0, 5, NULL)")
        .unwrap();
    for bad in [
        "{\"in\":[\"body\",[1]]}",
        "not json",
        "{\"in\":[\"shape\",[1]],\"null\":\"shape\"}",
        "{\"range\":[\"shape\",1]}",
    ] {
        let mut statement = conn
            .prepare("SELECT id, fts_filtered_score(body, ?1, ?2) AS score FROM docs ORDER BY score DESC LIMIT 5")
            .unwrap();
        statement
            .bind_at(1.try_into().unwrap(), Value::build_text("w1x"))
            .unwrap();
        statement
            .bind_at(2.try_into().unwrap(), Value::build_text(bad))
            .unwrap();
        assert!(statement.run_collect_rows().is_err(), "{bad} was accepted");
    }
    conn.execute("CREATE TABLE plain(id INTEGER PRIMARY KEY, body TEXT, shape INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE INDEX plain_fts ON plain USING fts(body, shape) WITH (filter_fields = 'shape')",
    )
    .unwrap();
    conn.execute("INSERT INTO plain VALUES (1, 'w1x', 1)")
        .unwrap();
    let mut statement = conn
        .prepare("SELECT id, fts_filtered_score(body, ?1, ?2) AS score FROM plain ORDER BY score DESC LIMIT 5")
        .unwrap();
    statement
        .bind_at(1.try_into().unwrap(), Value::build_text("w1x"))
        .unwrap();
    statement
        .bind_at(
            2.try_into().unwrap(),
            Value::build_text("{\"within\":[1,2]}"),
        )
        .unwrap();
    assert!(statement.run_collect_rows().is_err());
    // A filter column must hold an integer, and the intervals a valid packing.
    assert!(conn
        .execute("INSERT INTO docs VALUES (2, 'w1x', 'one', 0, 5, NULL)")
        .is_err());
    assert!(conn
        .execute("INSERT INTO docs VALUES (3, 'w1x', 1, 0, 5, X'0102')")
        .is_err());
    // Searched columns come first.
    assert!(conn
        .execute(
            "CREATE INDEX bad_fts ON plain USING fts(shape, body) WITH (filter_fields = 'shape')"
        )
        .is_err());
}

/// An index names a tokenizer an embedder registered: queries tokenize with
/// it, and an index naming an unregistered one is refused.
#[test]
fn a_registered_tokenizer_serves_its_index_and_an_unregistered_one_is_refused() {
    use tantivy::tokenizer::{LowerCaser, TextAnalyzer, Token, TokenStream, Tokenizer};
    #[derive(Clone)]
    struct Pipes;
    struct PipeStream {
        tokens: Vec<Token>,
        at: usize,
    }
    impl TokenStream for PipeStream {
        fn advance(&mut self) -> bool {
            self.at += 1;
            self.at <= self.tokens.len()
        }
        fn token(&self) -> &Token {
            &self.tokens[self.at - 1]
        }
        fn token_mut(&mut self) -> &mut Token {
            &mut self.tokens[self.at - 1]
        }
    }
    impl Tokenizer for Pipes {
        type TokenStream<'a> = PipeStream;
        fn token_stream<'a>(&'a mut self, text: &'a str) -> PipeStream {
            let mut tokens = Vec::new();
            let mut offset = 0;
            for (position, part) in text.split('|').enumerate() {
                tokens.push(Token {
                    offset_from: offset,
                    offset_to: offset + part.len(),
                    position,
                    text: part.to_string(),
                    position_length: 1,
                });
                offset += part.len() + 1;
            }
            PipeStream { tokens, at: 0 }
        }
    }
    register_tokenizer(
        "test_pipes_v1",
        TextAnalyzer::builder(Pipes).filter(LowerCaser).build(),
    )
    .unwrap();
    assert!(register_tokenizer("test_pipes_v1", TextAnalyzer::from(Pipes)).is_err());
    assert!(register_tokenizer("default", TextAnalyzer::from(Pipes)).is_err());
    let (_dir, _db, conn) = open(false);
    conn.execute("CREATE TABLE p(id INTEGER PRIMARY KEY, body TEXT, k INTEGER)")
        .unwrap();
    assert!(conn
        .execute("CREATE INDEX p_bad ON p USING fts(body) WITH (tokenizers = 'body=never_registered_v1')")
        .is_err());
    conn.execute(
        "CREATE INDEX p_fts ON p USING fts(body, k) WITH (tokenizers = 'body=test_pipes_v1', filter_fields = 'k')",
    )
    .unwrap();
    conn.execute("INSERT INTO p VALUES (1, 'red apple|Green Pear', 1), (2, 'red|apple', 1)")
        .unwrap();
    let ids = |query: &str| -> Vec<i64> {
        let mut statement = conn
            .prepare("SELECT id, fts_filtered_score(body, ?1, ?2) AS score FROM p ORDER BY score DESC LIMIT 5")
            .unwrap();
        statement
            .bind_at(1.try_into().unwrap(), Value::build_text(query.to_string()))
            .unwrap();
        statement
            .bind_at(
                2.try_into().unwrap(),
                Value::build_text("{\"in\":[\"k\",[1]]}"),
            )
            .unwrap();
        let mut ids: Vec<i64> = statement
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[0].as_int().unwrap())
            .collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        ids("\"green pear\""),
        vec![1],
        "a pipe-delimited token with a space in it"
    );
    assert_eq!(
        ids("red"),
        vec![2],
        "'red apple' is one token, so only document 2 has 'red'"
    );
}

/// Erasure: after DELETE, OPTIMIZE INDEX and a TRUNCATE checkpoint under
/// secure_delete, none of the segment rows that held the erased document is
/// left in the backing store or the database file, while before the
/// OPTIMIZE they still are.
#[test]
fn optimize_after_a_delete_leaves_none_of_the_erased_documents_segment_in_the_file() {
    for mvcc in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("erase.db");
        let io: Arc<dyn crate::IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("PRAGMA secure_delete = ON").unwrap();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT, k INTEGER)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body, k) WITH (filter_fields = 'k')")
            .unwrap();
        for batch in 0..4 {
            let values: Vec<String> = (batch * 40 + 1..=batch * 40 + 40)
                .map(|id| format!("({id}, 'shared words mark{id}', 1)"))
                .collect();
            conn.execute(format!("INSERT INTO docs VALUES {}", values.join(",")))
                .unwrap();
        }
        // The erased document is the only one of its segment.
        conn.execute("INSERT INTO docs VALUES (999, 'zqxplantedsecretterm shared', 1)")
            .unwrap();
        let before = super::tests::backing_bytes(&conn, &io);
        conn.execute("DELETE FROM docs WHERE id = 999").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let file = std::fs::read(&path).unwrap();
        let present = |file: &[u8]| {
            before
                .iter()
                .filter(|blob| {
                    blob.len() >= 64 && file.windows(blob.len()).any(|w| w == blob.as_slice())
                })
                .count()
        };
        let held = present(&file);
        assert!(
            held > 0,
            "mvcc={mvcc}: the segments are in the file before the OPTIMIZE"
        );
        conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let after = super::tests::backing_bytes(&conn, &io);
        let kept = before
            .iter()
            .filter(|blob| blob.len() >= 64 && after.contains(*blob))
            .count();
        assert_eq!(
            kept, 0,
            "mvcc={mvcc}: {kept} old segment rows still in the backing store"
        );
        drop(conn);
        drop(db);
        let file = std::fs::read(&path).unwrap();
        assert_eq!(
            present(&file),
            0,
            "mvcc={mvcc}: old segment rows left in the file after the OPTIMIZE"
        );
        let db = Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        let mut statement = conn
            .prepare("SELECT id, fts_filtered_score(body, ?1, ?2) AS score FROM docs ORDER BY score DESC LIMIT 500")
            .unwrap();
        statement
            .bind_at(1.try_into().unwrap(), Value::build_text("shared"))
            .unwrap();
        statement
            .bind_at(
                2.try_into().unwrap(),
                Value::build_text("{\"in\":[\"k\",[1]]}"),
            )
            .unwrap();
        assert_eq!(
            statement.run_collect_rows().unwrap().len(),
            160,
            "mvcc={mvcc}"
        );
    }
}

/// Timing over real memory texts (`FTS_BENCH_TEXTS`, JSON lines with a
/// `text`; `FTS_BENCH_QUERIES` likewise): 41 admission shapes of which 38 are
/// admitted, 80% of documents closed, a recorded time spread over a year and
/// event intervals on some. Prints p50/p95/p99 of the filtered top 10 and top
/// 50, undated and dated, before and after OPTIMIZE INDEX. Run with
/// `--ignored --nocapture` in a release build.
#[test]
#[ignore = "timing over a real corpus; needs FTS_BENCH_TEXTS"]
fn bench_filtered_over_real_texts() {
    let Ok(texts) = std::env::var("FTS_BENCH_TEXTS") else {
        return;
    };
    let queries = std::env::var("FTS_BENCH_QUERIES").unwrap();
    let limit_docs: usize = std::env::var("FTS_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(usize::MAX);
    let read = |path: &str| -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).unwrap()["text"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    };
    let words = |text: &str| -> String {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let docs: Vec<String> = read(&texts).into_iter().take(limit_docs).collect();
    let queries: Vec<String> = read(&queries).iter().map(|q| words(q)).take(100).collect();
    let (_dir, _db, conn) = open(true);
    let mut rng = Rng(0x5EED);
    let year = 365 * 24 * 3600 * 1_000_000i64;
    let start = std::time::Instant::now();
    for chunk in docs.chunks(128).enumerate() {
        let (offset, chunk) = chunk;
        let values: Vec<String> = chunk
            .iter()
            .enumerate()
            .map(|(i, text)| {
                let id = offset * 128 + i + 1;
                let observed = rng.below(year as u64) as i64;
                let ivs = if rng.below(3) == 0 {
                    packed(&[(observed, observed + 24 * 3600 * 1_000_000)])
                } else {
                    "NULL".into()
                };
                format!(
                    "({id}, '{}', {}, {}, {observed}, {ivs})",
                    text.replace('\'', "''"),
                    1 + rng.below(41),
                    i64::from(rng.below(5) != 0)
                )
            })
            .collect();
        conn.execute(format!("INSERT INTO docs VALUES {}", values.join(",")))
            .unwrap();
    }
    eprintln!("BENCH built {} docs in {:?}", docs.len(), start.elapsed());
    let admitted: Vec<String> = (1..=38).map(|s| s.to_string()).collect();
    let undated = format!(
        "{{\"all\":[{{\"in\":[\"shape\",[{}]]}},{{\"range\":[\"closed\",0,1]}}]}}",
        admitted.join(",")
    );
    let month_from = year - 30 * 24 * 3600 * 1_000_000;
    let dated = format!(
        "{{\"all\":[{{\"in\":[\"shape\",[{}]]}},{{\"range\":[\"closed\",0,1]}},{{\"any\":[{{\"range\":[\"observed\",{month_from},{year}]}},{{\"within\":[{month_from},{year}]}}]}}]}}",
        admitted.join(",")
    );
    let pct = |mut v: Vec<f64>, p: f64| -> f64 {
        v.sort_by(f64::total_cmp);
        v[((v.len() as f64 - 1.0) * p).round() as usize]
    };
    for pass in ["as-written", "optimized"] {
        if pass == "optimized" {
            let t = std::time::Instant::now();
            conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
            eprintln!("BENCH optimize {:?}", t.elapsed());
        }
        for (name, filter) in [("undated", &undated), ("dated", &dated)] {
            for limit in [10i64, 50] {
                // One warm-up pass, then the timed one.
                for timed in [false, true] {
                    let mut times = Vec::new();
                    let mut hits = 0;
                    for q in &queries {
                        let t = std::time::Instant::now();
                        let mut statement = conn
                            .prepare("SELECT id, fts_filtered_score(body, ?1, ?2) AS score FROM docs ORDER BY score DESC LIMIT ?3")
                            .unwrap();
                        statement
                            .bind_at(1.try_into().unwrap(), Value::build_text(q.clone()))
                            .unwrap();
                        statement
                            .bind_at(2.try_into().unwrap(), Value::build_text(filter.clone()))
                            .unwrap();
                        statement
                            .bind_at(3.try_into().unwrap(), Value::from_i64(limit))
                            .unwrap();
                        hits += statement.run_collect_rows().unwrap().len();
                        times.push(t.elapsed().as_secs_f64() * 1e3);
                    }
                    if timed {
                        eprintln!(
                            "BENCH {pass} {name} top{limit} n={} p50={:.2}ms p95={:.2}ms p99={:.2}ms hits={hits}",
                            times.len(),
                            pct(times.clone(), 0.5),
                            pct(times.clone(), 0.95),
                            pct(times, 0.99)
                        );
                    }
                }
            }
        }
    }
}
