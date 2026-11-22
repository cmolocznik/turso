//! The `trigram` index method: `code_match` returns the same rows through the index as a plain
//! scan does, whatever was inserted, updated and deleted before, and in how many statements.

use std::sync::Arc;

use core_tester::common::rng_from_time_or_env;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::common::{limbo_exec_rows_fallible, ExecRows, TempDatabase};

const TABLE: &str = "CREATE TABLE docs (path TEXT, text TEXT)";
const INDEX: &str = "CREATE INDEX docs_by_trigram ON docs USING trigram (text)";

/// Pieces of texts: words, code, a multi-byte letter, and line ends.
const PIECES: &[&str] = &[
    "fn main() {",
    "let x = 1;",
    "let y = 22;",
    "struct Foo",
    "impl Foo for Bar",
    "hello world",
    "Hello",
    "café",
    "abc",
    "abcd",
    "bcd",
    "}",
    " ",
    "\n",
    "\n",
];

/// Expressions of every kind the planner meets: literals, classes, alternations, repetitions,
/// anchors, flags, ones with no trigram and ones nothing matches.
const PATTERNS: &[&str] = &[
    r"fn\s+main",
    r"(?i)foo",
    r"abc",
    r"ab.d",
    r"x = [0-9]",
    r"^let",
    r"(?m)^impl",
    r"(hello|world)",
    r"café",
    r"[^a]bc",
    r"zzz",
    r".",
    r"struct \w+",
    r"y = 2+;",
    r"(?s)main.*Bar",
    r"o w",
    r"\bfor\b",
    r"[A-Z]ello",
];

fn random_text(rng: &mut ChaCha8Rng) -> String {
    (0..rng.next_u32() % 8)
        .map(|_| PIECES[rng.next_u32() as usize % PIECES.len()])
        .collect()
}

fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn matching(conn: &Arc<turso_core::Connection>, pattern: &str) -> Vec<String> {
    let rows: Vec<(String,)> = conn.exec_rows(&format!(
        "SELECT path FROM docs WHERE code_match(text, {}) ORDER BY path",
        quoted(pattern)
    ));
    rows.into_iter().map(|(path,)| path).collect()
}

/// Applies a random history to a table with the index and to one without: inserts of one text
/// and of many in one statement, rewrites, a replacement of a row the same statement added, and
/// deletions. Every expression gives the same rows from both.
fn the_index_agrees_with_a_plain_scan(
    plain: &Arc<turso_core::Connection>,
    indexed: &Arc<turso_core::Connection>,
    steps: u32,
) {
    let (mut seeds, _) = rng_from_time_or_env();
    let seed = seeds.next_u64();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut next_path = 0u32;
    let mut present: Vec<u32> = Vec::new();
    for step in 1..=steps {
        let statement = match rng.next_u32() % 6 {
            0 | 1 if !present.is_empty() => {
                let path = present[rng.next_u32() as usize % present.len()];
                format!(
                    "UPDATE docs SET text = {} WHERE path = 'p{path}'",
                    quoted(&random_text(&mut rng))
                )
            }
            2 if !present.is_empty() => {
                let index = rng.next_u32() as usize % present.len();
                let path = present.swap_remove(index);
                format!("DELETE FROM docs WHERE path = 'p{path}'")
            }
            3 => {
                // A row added twice in one statement: the second replaces the first.
                next_path += 1;
                present.push(next_path);
                let rowid = 1_000_000 + next_path;
                format!(
                    "INSERT OR REPLACE INTO docs (rowid, path, text) VALUES ({rowid}, 'p{next_path}', {}), ({rowid}, 'p{next_path}', {})",
                    quoted(&random_text(&mut rng)),
                    quoted(&random_text(&mut rng))
                )
            }
            _ => {
                let rows: Vec<String> = (0..1 + rng.next_u32() % 5)
                    .map(|_| {
                        next_path += 1;
                        present.push(next_path);
                        format!("('p{next_path}', {})", quoted(&random_text(&mut rng)))
                    })
                    .collect();
                format!("INSERT INTO docs VALUES {}", rows.join(", "))
            }
        };
        plain.execute(&statement).unwrap();
        indexed.execute(&statement).unwrap();

        if step % 5 == 0 || step == steps {
            for pattern in PATTERNS {
                assert_eq!(
                    matching(indexed, pattern),
                    matching(plain, pattern),
                    "seed {seed}, after {statement:?}: pattern {pattern}"
                );
            }
        }
    }
    let rows: Vec<(String,)> = indexed.exec_rows("PRAGMA integrity_check");
    assert_eq!(rows, vec![("ok".to_string(),)], "seed {seed}");
}

#[turso_macros::test(mvcc)]
fn code_match_gives_the_same_rows_with_and_without_the_index(tmp_db: TempDatabase) {
    let builder = TempDatabase::builder()
        .with_opts(tmp_db.db_opts)
        .with_flags(tmp_db.db_flags)
        .with_init_sql(TABLE);
    let plain_db = builder.clone().build();
    let index_db = builder.build();
    let plain = plain_db.connect_limbo();
    let indexed = index_db.connect_limbo();
    indexed.execute(INDEX).unwrap();
    the_index_agrees_with_a_plain_scan(&plain, &indexed, 400);
}

/// With an IO that finishes nothing until it is stepped, and a cache of a few small pages, the
/// index's reads and writes are interrupted and entered again at every B-tree operation.
#[cfg(feature = "io_memory_yield")]
#[test]
fn code_match_gives_the_same_rows_when_every_io_waits() {
    use turso_core::{
        ConnectOpts, Database, DatabaseOpts, MemoryYieldIO, OpenFlags, SqliteDialect,
    };

    let open = |name: &str| {
        let db = Database::open_file_with_flags(
            Arc::new(MemoryYieldIO::new()),
            name,
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA page_size = 512").unwrap();
        conn.execute("PRAGMA cache_size = 12").unwrap();
        conn.execute(TABLE).unwrap();
        (db, conn)
    };
    let (_plain_db, plain) = open("trigram_plain.db");
    let (_index_db, indexed) = open("trigram_indexed.db");
    indexed.execute(INDEX).unwrap();
    the_index_agrees_with_a_plain_scan(&plain, &indexed, 250);
}

#[turso_macros::test]
fn a_query_with_code_match_reads_the_index(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute(INDEX).unwrap();
    let plan: Vec<(i64, i64, i64, String)> =
        conn.exec_rows("EXPLAIN QUERY PLAN SELECT path FROM docs WHERE code_match(text, 'abc')");
    assert!(
        plan.iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX METHOD")),
        "{plan:?}"
    );
}

#[turso_macros::test]
fn an_index_made_after_the_rows_holds_them(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute(
        "INSERT INTO docs VALUES ('a', 'fn main() {}'), ('b', 'struct Foo'), ('c', NULL), ('d', X'616263')",
    )
    .unwrap();
    conn.execute(INDEX).unwrap();
    assert_eq!(matching(&conn, r"fn\s+main"), vec!["a".to_string()]);
    assert_eq!(matching(&conn, r"(?i)FOO"), vec!["b".to_string()]);
    // A value that is not text, here a blob, is in no answer, as `code_match` gives it NULL.
    assert_eq!(
        matching(&conn, r"."),
        vec!["a".to_string(), "b".to_string()]
    );
}

/// A row whose value is not text is in no list, and leaves none behind: removed, or given a
/// text, it is as a row that always held what it holds now.
#[turso_macros::test]
fn a_row_that_is_not_text_is_never_indexed(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute(INDEX).unwrap();
    conn.execute("INSERT INTO docs VALUES ('a', X'616263'), ('b', NULL), ('c', 'abc')")
        .unwrap();
    conn.execute("DELETE FROM docs WHERE path = 'a'").unwrap();
    assert_eq!(matching(&conn, "."), vec!["c".to_string()]);
    conn.execute("UPDATE docs SET text = 'abc' WHERE path = 'b'")
        .unwrap();
    assert_eq!(
        matching(&conn, "abc"),
        vec!["b".to_string(), "c".to_string()]
    );
    conn.execute("UPDATE docs SET text = NULL WHERE path = 'c'")
        .unwrap();
    conn.execute("DELETE FROM docs WHERE path = 'c'").unwrap();
    assert_eq!(matching(&conn, "."), vec!["b".to_string()]);
}

#[turso_macros::test]
fn an_expression_that_is_not_one_is_refused(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute(INDEX).unwrap();
    conn.execute("INSERT INTO docs VALUES ('a', 'abc')")
        .unwrap();
    let query = "SELECT path FROM docs WHERE code_match(text, '(')";
    assert!(limbo_exec_rows_fallible(&tmp_db, &conn, query).is_err());
}

#[turso_macros::test]
fn an_index_takes_one_column_and_no_option(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    assert!(conn
        .execute("CREATE INDEX two ON docs USING trigram (path, text)")
        .is_err());
    assert!(conn
        .execute("CREATE INDEX opt ON docs USING trigram (text) WITH (depth = 2)")
        .is_err());
}

fn mvcc_index_db(name: &str) -> TempDatabase {
    TempDatabase::builder()
        .with_db_name(name)
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build()
}

/// Writers adding, rewriting and removing texts in concurrent transactions commit side by side:
/// each statement's segment and each tombstone are entries of their own.
#[test]
fn writers_commit_side_by_side_under_mvcc() {
    let tmp_db = mvcc_index_db("trigram-side-by-side.db");
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();
    first.execute(TABLE).unwrap();
    first.execute(INDEX).unwrap();
    first
        .execute("INSERT INTO docs VALUES ('a', 'fn main() {}'), ('b', 'struct Foo')")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO docs VALUES ('c', 'impl Foo for Bar')")
        .unwrap();
    second
        .execute("INSERT INTO docs VALUES ('d', 'hello world')")
        .unwrap();
    first
        .execute("UPDATE docs SET text = 'fn main() { hello }' WHERE path = 'a'")
        .unwrap();
    second.execute("DELETE FROM docs WHERE path = 'b'").unwrap();
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();

    let reader = tmp_db.connect_limbo();
    assert_eq!(matching(&reader, "Foo"), vec!["c".to_string()]);
    assert_eq!(
        matching(&reader, "hello"),
        vec!["a".to_string(), "d".to_string()]
    );
    assert_eq!(matching(&reader, r"fn\s+main"), vec!["a".to_string()]);
}

#[cfg(feature = "io_memory_yield")]
fn rows_and_waits(
    conn: &Arc<turso_core::Connection>,
    io: &dyn turso_core::IO,
    query: &str,
) -> (usize, usize) {
    use turso_core::StepResult;

    let mut statement = conn.prepare(query).unwrap();
    let (mut rows, mut waits) = (0, 0);
    loop {
        match statement.step().unwrap() {
            StepResult::Row => rows += 1,
            StepResult::Done => return (rows, waits),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                waits += 1;
                io.step().unwrap();
            }
            StepResult::Interrupt | StepResult::Busy => panic!("{query} was interrupted"),
        }
    }
}

/// With a cache far smaller than the index, a query waits for IO about once per page it reads.
/// Returns the waits of a query for a word one text holds, and of one every text matches, in a
/// table of `texts` texts written in eight statements, so in eight segments.
#[cfg(feature = "io_memory_yield")]
fn waits_among(texts: usize) -> (usize, usize) {
    use turso_core::{
        ConnectOpts, Database, DatabaseOpts, MemoryYieldIO, OpenFlags, SqliteDialect,
    };

    let io = Arc::new(MemoryYieldIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        &format!("trigram_reads_{texts}.db"),
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA page_size = 512").unwrap();
    conn.execute(TABLE).unwrap();
    conn.execute(INDEX).unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(7);
    let per_statement = texts / 8;
    for batch in 0..8 {
        let rows: Vec<String> = (0..per_statement)
            .map(|n| {
                let words: Vec<String> = (0..12)
                    .map(|_| format!("w{}", rng.next_u32() % 5000))
                    .collect();
                format!("('p{batch}_{n}', '{}')", words.join(" "))
            })
            .collect();
        conn.execute(format!("INSERT INTO docs VALUES {}", rows.join(", ")))
            .unwrap();
    }
    conn.execute("UPDATE docs SET text = text || ' needle' WHERE path = 'p3_5'")
        .unwrap();

    let query = |pattern: &str, expected_rows: usize| {
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA cache_size = 12").unwrap();
        let (rows, waits) = rows_and_waits(
            &conn,
            io.as_ref(),
            &format!("SELECT path FROM docs WHERE code_match(text, '{pattern}')"),
        );
        assert_eq!(rows, expected_rows, "{pattern}");
        waits
    };
    (query("needle", 1), query("w", texts))
}

/// What a query costs follows the trigrams it names and what it returns, not the size of the
/// index: among eight times as many texts, a query for a word one text holds waits about as
/// often, while one every text matches waits several times as often.
#[cfg(feature = "io_memory_yield")]
#[test]
fn a_query_reads_only_its_trigrams_lists() {
    let (needle_small, every_small) = waits_among(400);
    let (needle_large, every_large) = waits_among(3200);
    // Each B-tree a query reads may be a level or two taller in the larger table, in each of
    // its eight segments' seeks.
    let taller = 8 * 6;
    assert!(
        needle_large <= needle_small + taller,
        "one text: {needle_small} waits among 400, {needle_large} among 3200"
    );
    assert!(
        every_large >= every_small * 4,
        "every text: {every_small} waits among 400, {every_large} among 3200"
    );
    assert!(
        needle_large * 10 < every_large,
        "{needle_large} against {every_large}"
    );
}
