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
    the_index_agrees_with_a_plain_scan_merging(plain, indexed, steps, None);
}

/// [`the_index_agrees_with_a_plain_scan`], with `OPTIMIZE INDEX` run every `merge_every` steps.
fn the_index_agrees_with_a_plain_scan_merging(
    plain: &Arc<turso_core::Connection>,
    indexed: &Arc<turso_core::Connection>,
    steps: u32,
    merge_every: Option<u32>,
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
        if merge_every.is_some_and(|every| step % every == 0) {
            indexed.execute("OPTIMIZE INDEX docs_by_trigram").unwrap();
        }

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

/// The same random history with `OPTIMIZE INDEX` run now and then, each merge in steps of a few
/// hundred bytes, so that it stops within its texts, within its lists and within a trigram:
/// merges change no answer.
#[cfg(feature = "test_helper")]
#[turso_macros::test(mvcc)]
fn merges_change_no_answer(tmp_db: TempDatabase) {
    turso_core::index_method::trigram::set_trigram_step_bytes_for_test(Some(300));
    let builder = TempDatabase::builder()
        .with_opts(tmp_db.db_opts)
        .with_flags(tmp_db.db_flags)
        .with_init_sql(TABLE);
    let plain_db = builder.clone().build();
    let index_db = builder.build();
    let plain = plain_db.connect_limbo();
    let indexed = index_db.connect_limbo();
    indexed.execute(INDEX).unwrap();
    the_index_agrees_with_a_plain_scan_merging(&plain, &indexed, 400, Some(9));
    turso_core::index_method::trigram::set_trigram_step_bytes_for_test(None);
}

/// Writers add, rewrite and remove texts between the steps of a merge under MVCC, each step
/// committing on its own: every answer after is a plain scan's.
#[cfg(feature = "test_helper")]
#[test]
fn writers_go_on_between_the_steps_of_a_merge() {
    turso_core::index_method::trigram::set_trigram_step_bytes_for_test(Some(200));
    let tmp_db = mvcc_index_db("trigram-merge-steps.db");
    let plain_db = TempDatabase::builder().with_init_sql(TABLE).build();
    let plain = plain_db.connect_limbo();
    let writer = tmp_db.connect_limbo();
    writer.execute(TABLE).unwrap();
    writer.execute(INDEX).unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(11);
    let mut next = 0;
    let mut write = |rng: &mut ChaCha8Rng| {
        next += 1;
        let statement = match rng.next_u32() % 3 {
            0 if next > 3 => format!(
                "UPDATE docs SET text = {} WHERE path = 'p{}'",
                quoted(&random_text(rng)),
                rng.next_u32() % next
            ),
            1 if next > 3 => format!("DELETE FROM docs WHERE path = 'p{}'", rng.next_u32() % next),
            _ => format!(
                "INSERT INTO docs VALUES ('p{next}', {})",
                quoted(&random_text(rng))
            ),
        };
        plain.execute(&statement).unwrap();
        // As the steps of the merge do, in a concurrent transaction.
        writer.execute("BEGIN CONCURRENT").unwrap();
        writer.execute(&statement).unwrap();
        writer.execute("COMMIT").unwrap();
    };
    // Twelve segments of a few texts each.
    for _ in 0..12 {
        for _ in 0..3 {
            write(&mut rng);
        }
    }
    let optimizer = tmp_db.connect_limbo();
    let mut optimize = optimizer.prepare("OPTIMIZE INDEX docs_by_trigram").unwrap();
    // Each write is a segment of its own, which a later merge takes: writing at every wait, the
    // run would never be done. The writes stop after forty.
    let mut waits = 0;
    loop {
        match optimize.step().unwrap() {
            turso_core::StepResult::Done => break,
            turso_core::StepResult::Row => {}
            _ => {
                waits += 1;
                assert!(waits < 100_000, "the merges never ended");
                if waits <= 40 {
                    write(&mut rng);
                }
                optimize.wait_io().unwrap();
            }
        }
    }
    assert!(waits > 40, "the merges took {waits} waits");
    let reader = tmp_db.connect_limbo();
    for pattern in PATTERNS {
        assert_eq!(
            matching(&reader, pattern),
            matching(&plain, pattern),
            "pattern {pattern}"
        );
    }
    turso_core::index_method::trigram::set_trigram_step_bytes_for_test(None);
}

/// After `OPTIMIZE INDEX` a query for a word one text holds seeks in one segment instead of
/// many, and waits for IO far less often.
#[cfg(feature = "io_memory_yield")]
#[test]
fn a_merged_index_is_read_in_fewer_seeks() {
    use turso_core::{
        ConnectOpts, Database, DatabaseOpts, MemoryYieldIO, OpenFlags, SqliteDialect,
    };

    let io = Arc::new(MemoryYieldIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        "trigram_merged_reads.db",
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
    let mut rng = ChaCha8Rng::seed_from_u64(5);
    for statement in 0..16 {
        let rows: Vec<String> = (0..20)
            .map(|n| {
                let words: Vec<String> = (0..12)
                    .map(|_| format!("w{}", rng.next_u32() % 5000))
                    .collect();
                format!("('p{statement}_{n}', '{}')", words.join(" "))
            })
            .collect();
        conn.execute(format!("INSERT INTO docs VALUES {}", rows.join(", ")))
            .unwrap();
    }
    conn.execute("UPDATE docs SET text = text || ' needle' WHERE path = 'p3_5'")
        .unwrap();
    let waits = || {
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA cache_size = 12").unwrap();
        let (rows, waits) = rows_and_waits(
            &conn,
            io.as_ref(),
            "SELECT path FROM docs WHERE code_match(text, 'needle')",
        );
        assert_eq!(rows, 1);
        waits
    };
    let before = waits();
    conn.execute("OPTIMIZE INDEX docs_by_trigram").unwrap();
    let after = waits();
    assert!(
        after * 3 < before,
        "{before} waits in 17 segments, {after} merged"
    );
}

/// A merge drops the texts removed before it: neither their entries nor their numbers in the
/// lists are copied, so a query reads no more for them. Of 8,000 texts holding `gone`, all but
/// the 50 of the last statement are removed; merged, a query for it waits about as often as one
/// for `kept`, which only those 50 hold.
#[cfg(feature = "io_memory_yield")]
#[test]
fn a_merge_drops_the_texts_removed_before_it() {
    use turso_core::{
        ConnectOpts, Database, DatabaseOpts, MemoryYieldIO, OpenFlags, SqliteDialect,
    };

    let io = Arc::new(MemoryYieldIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        "trigram_dropped_reads.db",
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
    for statement in 0..16 {
        let rows: Vec<String> = (0..500)
            .map(|n| {
                let kept = if statement == 15 && n < 50 {
                    " kept"
                } else {
                    ""
                };
                format!("('p{statement}_{n}', 'gone w{n}{kept}')")
            })
            .collect();
        conn.execute(format!("INSERT INTO docs VALUES {}", rows.join(", ")))
            .unwrap();
    }
    conn.execute("DELETE FROM docs WHERE text NOT LIKE '%kept%'")
        .unwrap();
    let waits = |pattern: &str| {
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA cache_size = 12").unwrap();
        let (rows, waits) = rows_and_waits(
            &conn,
            io.as_ref(),
            &format!("SELECT path FROM docs WHERE code_match(text, '{pattern}')"),
        );
        assert_eq!(rows, 50, "{pattern}");
        waits
    };
    let before = waits("gone");
    conn.execute("OPTIMIZE INDEX docs_by_trigram").unwrap();
    let (gone, kept) = (waits("gone"), waits("kept"));
    assert!(
        gone * 2 < kept * 3,
        "merged, {gone} waits for gone against {kept} for kept ({before} for gone before)"
    );
}

/// A merge copies its texts within its step's budget too, not only its lists: with a budget of
/// one byte, each text copied and each removed is a step, so a merge of 400 texts yields between
/// steps at least 800 times.
#[cfg(feature = "test_helper")]
#[test]
fn a_merge_copies_texts_a_step_at_a_time() {
    turso_core::index_method::trigram::set_trigram_step_bytes_for_test(Some(1));
    let tmp_db = mvcc_index_db("trigram-step-budget.db");
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute(INDEX).unwrap();
    for statement in 0..8 {
        let rows: Vec<String> = (0..50)
            .map(|n| format!("('p{statement}_{n}', 'abcd')"))
            .collect();
        conn.execute(format!("INSERT INTO docs VALUES {}", rows.join(", ")))
            .unwrap();
    }
    let mut optimize = conn.prepare("OPTIMIZE INDEX docs_by_trigram").unwrap();
    let mut yields = 0;
    loop {
        match optimize.step().unwrap() {
            turso_core::StepResult::Done => break,
            turso_core::StepResult::Yield => {
                yields += 1;
                optimize.wait_io().unwrap();
            }
            turso_core::StepResult::Row => {}
            _ => optimize.wait_io().unwrap(),
        }
    }
    drop(optimize);
    turso_core::index_method::trigram::set_trigram_step_bytes_for_test(None);
    assert!(yields >= 800, "{yields} steps");
    assert_eq!(matching(&conn, "abcd").len(), 400);
}

/// Two runs of `OPTIMIZE INDEX` stepped by turns under MVCC choose no segment twice: a step that
/// claims what the other claimed loses its commit and is done again, carrying the other's merge
/// on. Both end, and every answer is a plain scan's.
#[cfg(feature = "test_helper")]
#[test]
fn two_merging_at_once_claim_no_segment_twice() {
    turso_core::index_method::trigram::set_trigram_step_bytes_for_test(Some(200));
    let tmp_db = mvcc_index_db("trigram-two-merging.db");
    let plain_db = TempDatabase::builder().with_init_sql(TABLE).build();
    let plain = plain_db.connect_limbo();
    let writer = tmp_db.connect_limbo();
    writer.execute(TABLE).unwrap();
    writer.execute(INDEX).unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(13);
    for n in 0..24 {
        let statement = format!(
            "INSERT INTO docs VALUES ('p{n}', {})",
            quoted(&random_text(&mut rng))
        );
        plain.execute(&statement).unwrap();
        writer.execute(&statement).unwrap();
        if n % 5 == 4 {
            let statement = format!("DELETE FROM docs WHERE path = 'p{}'", n - 2);
            plain.execute(&statement).unwrap();
            writer.execute(&statement).unwrap();
        }
    }
    let (first, second) = (tmp_db.connect_limbo(), tmp_db.connect_limbo());
    let mut runs = [
        Some(first.prepare("OPTIMIZE INDEX docs_by_trigram").unwrap()),
        Some(second.prepare("OPTIMIZE INDEX docs_by_trigram").unwrap()),
    ];
    let mut turns = 0;
    while runs.iter().any(Option::is_some) {
        for run in runs.iter_mut() {
            let Some(statement) = run else { continue };
            turns += 1;
            assert!(turns < 200_000, "the merges never ended");
            match statement.step().unwrap() {
                turso_core::StepResult::Done => *run = None,
                turso_core::StepResult::Row => {}
                _ => statement.wait_io().unwrap(),
            }
        }
    }
    let reader = tmp_db.connect_limbo();
    for pattern in PATTERNS {
        assert_eq!(
            matching(&reader, pattern),
            matching(&plain, pattern),
            "pattern {pattern}"
        );
    }
    turso_core::index_method::trigram::set_trigram_step_bytes_for_test(None);
}

/// On a connection told to merge whole (atoll-forge's decision 103), `OPTIMIZE INDEX` merges
/// segments its policy leaves alone, three of them here, and drops every text removed before it:
/// a query for a word only those texts held then reads about as little as one for a word no text
/// ever held, where without it their lists stay to be read. A lone segment holding a removed text
/// is merged on its own too.
#[cfg(feature = "io_memory_yield")]
#[test]
fn a_whole_merge_drops_every_removed_text_where_the_policy_merges_nothing() {
    use turso_core::{
        ConnectOpts, Database, DatabaseOpts, MemoryYieldIO, OpenFlags, SqliteDialect,
    };

    let io = Arc::new(MemoryYieldIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        "trigram_whole_merge.db",
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
    for statement in 0..3 {
        let rows: Vec<String> = (0..300)
            .map(|n| {
                let word = match (statement, n % 3) {
                    (1, _) => "qqgonexx",
                    (2, 0) => "qqsecond",
                    _ => "keep",
                };
                format!("('p{statement}_{n}', '{word} w{n}')")
            })
            .collect();
        conn.execute(format!("INSERT INTO docs VALUES {}", rows.join(", ")))
            .unwrap();
    }
    conn.execute("DELETE FROM docs WHERE text LIKE '%qqgonexx%'")
        .unwrap();
    let waits = |pattern: &str, expected: usize| {
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA cache_size = 12").unwrap();
        let (rows, waits) = rows_and_waits(
            &conn,
            io.as_ref(),
            &format!("SELECT path FROM docs WHERE code_match(text, '{pattern}')"),
        );
        assert_eq!(rows, expected, "{pattern}");
        waits
    };
    // Its policy merges no level of fewer than eight: the removed texts' lists stay.
    conn.execute("OPTIMIZE INDEX docs_by_trigram").unwrap();
    let (unmerged, never) = (waits("qqgonexx", 0), waits("zzvvyyww", 0));
    assert!(
        unmerged > never * 3,
        "unmerged: {unmerged} waits for gone, {never} for never"
    );

    conn.set_index_merges_whole(true);
    conn.execute("OPTIMIZE INDEX docs_by_trigram").unwrap();
    let (gone, never) = (waits("qqgonexx", 0), waits("zzvvyyww", 0));
    assert!(
        gone <= never * 2 && gone * 4 < unmerged,
        "merged whole: {gone} waits for gone, {never} for never, {unmerged} unmerged"
    );
    assert_eq!(matching(&conn, "keep w7$").len(), 2);

    // A lone segment that holds a removed text is merged on its own.
    let held = waits("qqsecond", 100);
    conn.execute("DELETE FROM docs WHERE text LIKE '%qqsecond%'")
        .unwrap();
    conn.execute("OPTIMIZE INDEX docs_by_trigram").unwrap();
    let (second, never) = (waits("qqsecond", 0), waits("zzvvyyww", 0));
    assert!(
        second <= never * 2 && second * 2 < held,
        "a lone segment: {second} waits for the removed word, {never} for never, {held} held"
    );
    assert_eq!(matching(&conn, "keep w7$").len(), 2);
}
