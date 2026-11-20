//! The `path_trie` index method: `path_sync` returns the same rows through the index as a plain
//! scan does, whatever was inserted, updated and deleted before.

use std::sync::Arc;

use core_tester::common::rng_from_time_or_env;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::common::{limbo_exec_rows_fallible, ExecRows, TempDatabase};

const TABLE: &str = "CREATE TABLE files (path TEXT, kind INTEGER, seq INTEGER)";
const INDEX: &str = "CREATE INDEX files_by_path ON files USING path_trie (path, seq)";

/// Globs of every form the index splits, and some it does not.
const GLOBS: &[&str] = &[
    "**",
    "a/**",
    "a/b/**",
    "**/*.rs",
    "**/Cargo.toml",
    "*/Cargo.toml",
    "a/*/x.rs",
    "a/**/x.rs",
    "Cargo.lock",
    "b/c",
    "{a,b}/*.rs",
    "[ab]/?/*",
    "c/**/d/**",
    "nothing/here/**",
    "**/[[]id[]]/**",
    "a/[[]id[]]",
    "**/a[*]b",
    "b/q[?]/*",
];

/// Names with characters that are special in a glob are names like any other.
const DIRECTORIES: &[&str] = &["a", "b", "c", "d", "src", "[id]", "q?"];
const FILES: &[&str] = &[
    "x.rs",
    "y.rs",
    "Cargo.toml",
    "Cargo.lock",
    "c",
    "a",
    "[id]",
    "a*b",
];

fn random_path(rng: &mut ChaCha8Rng) -> String {
    let mut path = String::new();
    for _ in 0..rng.next_u32() % 4 {
        path.push_str(DIRECTORIES[rng.next_u32() as usize % DIRECTORIES.len()]);
        path.push('/');
    }
    path.push_str(FILES[rng.next_u32() as usize % FILES.len()]);
    path
}

fn random_patterns(rng: &mut ChaCha8Rng) -> String {
    let globs: Vec<String> = (0..1 + rng.next_u32() % 3)
        .map(|_| format!("\"{}\"", GLOBS[rng.next_u32() as usize % GLOBS.len()]))
        .collect();
    format!("[{}]", globs.join(","))
}

fn matching(conn: &Arc<turso_core::Connection>, patterns: &str, since: Option<u32>) -> Vec<String> {
    let since = since.map_or("NULL".to_string(), |since| since.to_string());
    let rows: Vec<(String,)> = conn.exec_rows(&format!(
        "SELECT path FROM files WHERE path_sync(path, seq, '{patterns}', {since}) ORDER BY path"
    ));
    rows.into_iter().map(|(path,)| path).collect()
}

/// Applies a random history of adds, changes, deletions and removals to a table with the index
/// and to one without, and checks that every query gives the same rows from both.
fn the_index_agrees_with_a_plain_scan(
    plain: &Arc<turso_core::Connection>,
    indexed: &Arc<turso_core::Connection>,
    steps: u32,
) {
    let (mut seeds, _) = rng_from_time_or_env();
    let seed = seeds.next_u64();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut present = std::collections::BTreeSet::new();
    for seq in 1..=steps {
        let path = random_path(&mut rng);
        let statement = if !present.contains(&path) {
            present.insert(path.clone());
            format!("INSERT INTO files VALUES ('{path}', 1, {seq})")
        } else {
            match rng.next_u32() % 4 {
                0 => format!("UPDATE files SET kind = 1, seq = {seq} WHERE path = '{path}'"),
                1 => format!("UPDATE files SET kind = 0, seq = {seq} WHERE path = '{path}'"),
                2 => {
                    let moved = random_path(&mut rng);
                    if present.contains(&moved) {
                        continue;
                    }
                    present.remove(&path);
                    present.insert(moved.clone());
                    format!("UPDATE files SET path = '{moved}', seq = {seq} WHERE path = '{path}'")
                }
                _ => {
                    present.remove(&path);
                    format!("DELETE FROM files WHERE path = '{path}'")
                }
            }
        };
        plain.execute(&statement).unwrap();
        indexed.execute(&statement).unwrap();

        if seq % 7 == 0 || seq == steps {
            let patterns = random_patterns(&mut rng);
            for since in [None, Some(rng.next_u32() % (seq + 1)), Some(seq - 1)] {
                assert_eq!(
                    matching(indexed, &patterns, since),
                    matching(plain, &patterns, since),
                    "seed {seed}, after {statement:?}: patterns {patterns}, since {since:?}"
                );
            }
        }
    }
    let rows: Vec<(String,)> = indexed.exec_rows("PRAGMA integrity_check");
    assert_eq!(rows, vec![("ok".to_string(),)], "seed {seed}");
}

#[turso_macros::test(mvcc)]
fn path_sync_gives_the_same_rows_with_and_without_the_index(tmp_db: TempDatabase) {
    let builder = TempDatabase::builder()
        .with_opts(tmp_db.db_opts)
        .with_flags(tmp_db.db_flags)
        .with_init_sql(TABLE);
    let plain_db = builder.clone().build();
    let index_db = builder.build();
    let plain = plain_db.connect_limbo();
    let indexed = index_db.connect_limbo();
    indexed.execute(INDEX).unwrap();
    the_index_agrees_with_a_plain_scan(&plain, &indexed, 600);
}

/// The same random history and queries with the index keeping entries for fewer or more
/// levels below the root: the rows it gives do not depend on how deep it keeps them.
#[turso_macros::test(mvcc)]
fn path_sync_gives_the_same_rows_at_every_depth(tmp_db: TempDatabase) {
    for depth in [0, 1, 3, 6] {
        let builder = TempDatabase::builder()
            .with_opts(tmp_db.db_opts)
            .with_flags(tmp_db.db_flags)
            .with_init_sql(TABLE);
        let plain_db = builder.clone().build();
        let index_db = builder.build();
        let plain = plain_db.connect_limbo();
        let indexed = index_db.connect_limbo();
        indexed
            .execute(format!("{INDEX} WITH (depth = {depth})"))
            .unwrap();
        the_index_agrees_with_a_plain_scan(&plain, &indexed, 300);
    }
}

#[turso_macros::test]
fn a_depth_that_is_no_small_whole_number_is_refused(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    for option in ["depth = -1", "depth = 65", "depth = 'two'", "levels = 2"] {
        assert!(
            conn.execute(format!("{INDEX} WITH ({option})")).is_err(),
            "{option}"
        );
    }
    conn.execute(format!("{INDEX} WITH (depth = 64)")).unwrap();
}

/// With an IO that finishes nothing until it is stepped, and a cache of a few small pages, the
/// index's reads and writes are interrupted and entered again at every B-tree operation.
#[cfg(feature = "io_memory_yield")]
#[test]
fn path_sync_gives_the_same_rows_when_every_io_waits() {
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
    let (_plain_db, plain) = open("path_trie_plain.db");
    let (_index_db, indexed) = open("path_trie_indexed.db");
    indexed.execute(INDEX).unwrap();
    the_index_agrees_with_a_plain_scan(&plain, &indexed, 400);
}

#[turso_macros::test]
fn a_query_with_path_sync_reads_the_index(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute(INDEX).unwrap();
    let plan: Vec<(i64, i64, i64, String)> = conn.exec_rows(
        "EXPLAIN QUERY PLAN SELECT path FROM files WHERE path_sync(path, seq, '[\"a/**\"]', 3)",
    );
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
        "INSERT INTO files VALUES ('a/x.rs', 1, 1), ('a/b/y.rs', 1, 2), ('Cargo.lock', 1, 3)",
    )
    .unwrap();
    conn.execute(INDEX).unwrap();
    assert_eq!(
        matching(&conn, "[\"a/**\"]", None),
        vec!["a/b/y.rs".to_string(), "a/x.rs".to_string()]
    );
    assert_eq!(
        matching(&conn, "[\"**\"]", Some(1)),
        vec!["Cargo.lock".to_string(), "a/b/y.rs".to_string()]
    );
}

#[turso_macros::test]
fn a_change_deep_in_a_directory_is_found_from_the_top(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute(INDEX).unwrap();
    conn.execute(
        "INSERT INTO files VALUES ('a/b/c/x.rs', 1, 1), ('a/b/y.rs', 1, 1), ('d/x.rs', 1, 1)",
    )
    .unwrap();
    assert!(matching(&conn, "[\"**\"]", Some(1)).is_empty());

    conn.execute("UPDATE files SET seq = 2 WHERE path = 'a/b/c/x.rs'")
        .unwrap();
    assert_eq!(
        matching(&conn, "[\"**\"]", Some(1)),
        vec!["a/b/c/x.rs".to_string()]
    );
    assert!(matching(&conn, "[\"d/**\"]", Some(1)).is_empty());

    // A deletion that keeps its row is a change like any other.
    conn.execute("UPDATE files SET kind = 0, seq = 3 WHERE path = 'a/b/y.rs'")
        .unwrap();
    assert_eq!(
        matching(&conn, "[\"a/**\"]", Some(2)),
        vec!["a/b/y.rs".to_string()]
    );
}

/// Two rows may have one path: the index is an index, and keeping paths unique is a UNIQUE
/// index's work (below).
#[turso_macros::test(mvcc)]
fn two_rows_may_have_one_path(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute(INDEX).unwrap();
    conn.execute("INSERT INTO files VALUES ('a/x.rs', 1, 1)")
        .unwrap();
    conn.execute("INSERT INTO files VALUES ('a/x.rs', 1, 2)")
        .unwrap();
    assert_eq!(
        matching(&conn, "[\"**\"]", None),
        vec!["a/x.rs".to_string(), "a/x.rs".to_string()]
    );
    assert_eq!(
        matching(&conn, "[\"a/x.rs\"]", Some(1)),
        vec!["a/x.rs".to_string()]
    );
    conn.execute("DELETE FROM files WHERE seq = 1").unwrap();
    assert_eq!(
        matching(&conn, "[\"**\"]", None),
        vec!["a/x.rs".to_string()]
    );
    // The same name may be a file in one landing and a directory in a later one.
    conn.execute("DELETE FROM files WHERE path = 'a/x.rs'")
        .unwrap();
    conn.execute("INSERT INTO files VALUES ('a/x.rs/inner', 1, 3)")
        .unwrap();
    assert_eq!(
        matching(&conn, "[\"a/**\"]", None),
        vec!["a/x.rs/inner".to_string()]
    );
}

fn mvcc_index_db(name: &str) -> TempDatabase {
    TempDatabase::builder()
        .with_db_name(name)
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build()
}

/// The paths of the rows in the table, read without the index.
fn plain_paths(conn: &Arc<turso_core::Connection>) -> Vec<String> {
    let rows: Vec<(String,)> = conn.exec_rows("SELECT path FROM files ORDER BY path");
    rows.into_iter().map(|(path,)| path).collect()
}

/// Writers of different paths under one directory, each in a concurrent transaction, commit
/// side by side: the index keeps no entry that two rows share.
#[test]
fn writers_of_different_paths_commit_side_by_side_under_mvcc() {
    let tmp_db = mvcc_index_db("path-trie-side-by-side.db");
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();
    first.execute(TABLE).unwrap();
    first.execute(INDEX).unwrap();
    first
        .execute("INSERT INTO files VALUES ('a/b/x.rs', 1, 1), ('a/c/y.rs', 1, 1)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO files VALUES ('a/b/z.rs', 1, 2)")
        .unwrap();
    second
        .execute("INSERT INTO files VALUES ('a/b/w.rs', 1, 3)")
        .unwrap();
    first
        .execute("UPDATE files SET seq = 2 WHERE path = 'a/b/x.rs'")
        .unwrap();
    second
        .execute("UPDATE files SET seq = 3 WHERE path = 'a/c/y.rs'")
        .unwrap();
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();

    let reader = tmp_db.connect_limbo();
    assert_eq!(matching(&reader, "[\"**\"]", None), plain_paths(&reader));
    assert_eq!(
        matching(&reader, "[\"a/**\"]", Some(1)),
        vec!["a/b/w.rs", "a/b/x.rs", "a/b/z.rs", "a/c/y.rs"]
    );
    assert_eq!(matching(&reader, "[\"a/b/*\"]", Some(2)), vec!["a/b/w.rs"]);
}

/// A writer that removes the last file of a directory and one that adds a file there commit
/// side by side, and the index holds exactly the table's rows after.
#[test]
fn emptying_a_directory_beside_a_writer_that_fills_it() {
    let tmp_db = mvcc_index_db("path-trie-empty-and-fill.db");
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();
    first.execute(TABLE).unwrap();
    first.execute(INDEX).unwrap();
    first
        .execute("INSERT INTO files VALUES ('a/x.rs', 1, 1)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO files VALUES ('a/y.rs', 1, 2)")
        .unwrap();
    second
        .execute("DELETE FROM files WHERE path = 'a/x.rs'")
        .unwrap();
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();

    let reader = tmp_db.connect_limbo();
    assert_eq!(plain_paths(&reader), vec!["a/y.rs"]);
    for since in [None, Some(0), Some(1)] {
        assert_eq!(
            matching(&reader, "[\"**\"]", since),
            vec!["a/y.rs"],
            "{since:?}"
        );
        assert_eq!(
            matching(&reader, "[\"a/*\"]", since),
            vec!["a/y.rs"],
            "{since:?}"
        );
    }
}

/// Keeping paths unique is a UNIQUE index's work, and it refuses a second writer of one path
/// even when both transactions are open at once.
#[test]
fn a_unique_index_on_the_path_refuses_a_second_writer_of_it() {
    let tmp_db = mvcc_index_db("path-trie-unique.db");
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();
    first.execute(TABLE).unwrap();
    first
        .execute("CREATE UNIQUE INDEX files_path ON files (path)")
        .unwrap();
    first.execute(INDEX).unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO files VALUES ('a/x.rs', 1, 1)")
        .unwrap();
    let second_wrote = second
        .execute("INSERT INTO files VALUES ('a/x.rs', 1, 2)")
        .is_ok();
    first.execute("COMMIT").unwrap();
    let second_committed = second_wrote && second.execute("COMMIT").is_ok();
    assert!(!second_committed, "both writers of one path committed");
    if !second_wrote {
        let _ = second.execute("ROLLBACK");
    }
    let reader = tmp_db.connect_limbo();
    assert_eq!(matching(&reader, "[\"**\"]", None), vec!["a/x.rs"]);
}

/// Paths a glob reaches only by crossing a `/` with something other than `**`: a class may
/// match `/` (`[!x]`, `[^x]`, a range that holds it), and an alternation may hold one.
const CROSSING_PATHS: &[&str] = &[
    "a/b",
    "ab",
    "axb",
    "a/x/b",
    "a.b",
    "a0b",
    "a/x.rs",
    "b/c/x.rs",
    "crates/a/Cargo.toml",
    "crates/a/b/Cargo.toml",
    "docs/api/v1/index.md",
    "Cargo.toml",
    "x/y",
    "/x",
    "a//b",
];
const CROSSING_GLOBS: &[&str] = &[
    "a[!x]b",
    "a[^x]b",
    "a[.-0]b",
    "[!q]/*",
    "{a,b/c}/x.rs",
    "a{/x,}/b",
    "crates/*/Cargo.toml",
    "docs/{guide,api/v1}/*.md",
    "*.toml",
    "*",
    "a?b",
    "**/b",
    "a/**",
];

#[turso_macros::test(mvcc)]
fn a_glob_that_crosses_a_separator_finds_the_same_rows_with_and_without_the_index(
    tmp_db: TempDatabase,
) {
    let builder = TempDatabase::builder()
        .with_opts(tmp_db.db_opts)
        .with_flags(tmp_db.db_flags)
        .with_init_sql(TABLE);
    let plain_db = builder.clone().build();
    let index_db = builder.build();
    let plain = plain_db.connect_limbo();
    let indexed = index_db.connect_limbo();
    indexed.execute(INDEX).unwrap();
    for (seq, path) in CROSSING_PATHS.iter().enumerate() {
        let insert = format!("INSERT INTO files VALUES ('{path}', 1, {})", seq + 1);
        plain.execute(&insert).unwrap();
        indexed.execute(&insert).unwrap();
    }
    for glob in CROSSING_GLOBS {
        let patterns = format!("[\"{glob}\"]");
        for since in [None, Some(0), Some(5)] {
            assert_eq!(
                matching(&indexed, &patterns, since),
                matching(&plain, &patterns, since),
                "{glob} since {since:?}"
            );
        }
    }
}

#[turso_macros::test]
fn bad_arguments_are_refused_the_same_way_with_and_without_the_index(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute(TABLE).unwrap();
    conn.execute("CREATE TABLE plain (path TEXT, seq INTEGER)")
        .unwrap();
    conn.execute(INDEX).unwrap();
    conn.execute("INSERT INTO files VALUES ('a', 1, 1)")
        .unwrap();
    conn.execute("INSERT INTO plain VALUES ('a', 1)").unwrap();
    for arguments in [
        "'not json', NULL",
        "'{\"a\": 1}', NULL",
        "'[1, 2]', NULL",
        "'[\"[\"]', NULL",
        "'[\"**\"]', 'soon'",
    ] {
        for table in ["files", "plain"] {
            let query = format!("SELECT path FROM {table} WHERE path_sync(path, seq, {arguments})");
            assert!(
                limbo_exec_rows_fallible(&tmp_db, &conn, &query).is_err(),
                "{query}"
            );
        }
    }
    // NULL patterns match nothing, and are no error.
    for table in ["files", "plain"] {
        let rows: Vec<(String,)> = conn.exec_rows(&format!(
            "SELECT path FROM {table} WHERE path_sync(path, seq, NULL, NULL)"
        ));
        assert!(rows.is_empty());
    }
}

/// Runs `query` over an IO that finishes nothing until it is stepped, and returns how many rows
/// it gave and how many times it waited for IO.
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
/// Returns the waits of a query for every file, of one for what changed since the last sequence
/// number but one, and of one for a single directory, in a table of `directories` directories
/// of 20 files each, each query on a connection that has nothing cached.
#[cfg(feature = "io_memory_yield")]
fn waits_in_a_table_of(directories: usize) -> (usize, usize, usize) {
    use turso_core::{
        ConnectOpts, Database, DatabaseOpts, MemoryYieldIO, OpenFlags, SqliteDialect,
    };

    let io = Arc::new(MemoryYieldIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        &format!("path_trie_reads_{directories}.db"),
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
    conn.execute("BEGIN").unwrap();
    for directory in 0..directories {
        for file in 0..20 {
            conn.execute(format!(
                "INSERT INTO files VALUES ('d{directory}/sub/f{file}.rs', 1, 1)"
            ))
            .unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("UPDATE files SET seq = 2 WHERE path = 'd7/sub/f7.rs'")
        .unwrap();

    let query = |patterns: &str, since: &str, expected_rows: usize| {
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA cache_size = 12").unwrap();
        let (rows, waits) = rows_and_waits(
            &conn,
            io.as_ref(),
            &format!("SELECT path FROM files WHERE path_sync(path, seq, '{patterns}', {since})"),
        );
        assert_eq!(rows, expected_rows, "{patterns} since {since}");
        waits
    };
    (
        query("[\"**\"]", "NULL", directories * 20),
        query("[\"**\"]", "1", 1),
        query("[\"d7/**\"]", "NULL", 20),
    )
}

/// What a query costs follows what it returns, not the size of the table: in a table eight
/// times as large, a query for one changed file and a query for one directory read about the
/// same number of pages, while a query for everything reads several times as many.
#[cfg(feature = "io_memory_yield")]
#[test]
fn a_query_reads_only_the_directories_it_needs() {
    let (all_small, changed_small, directory_small) = waits_in_a_table_of(50);
    let (all_large, changed_large, directory_large) = waits_in_a_table_of(400);

    // Each of the three B-trees a query reads may be one level taller in the larger table.
    let taller = 3;
    assert!(
        changed_large <= changed_small + taller,
        "one changed file: {changed_small} waits among 50 directories, {changed_large} among 400"
    );
    assert!(
        directory_large <= directory_small + taller,
        "one directory: {directory_small} waits among 50 directories, {directory_large} among 400"
    );
    assert!(
        all_large >= all_small * 4,
        "every file: {all_small} waits for 50 directories, {all_large} for 400"
    );
    assert!(changed_large * 10 < all_large && directory_large * 10 < all_large);
}
