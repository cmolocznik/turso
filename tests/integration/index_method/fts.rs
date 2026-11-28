use turso_core::index_method::fts::FtsIndexAttachment;
use turso_core::index_method::IndexMethodConfiguration;
use turso_core::schema::IndexColumn;
use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_core::CollationSeq;
use turso_core::Connection;
use turso_core::LimboError;
use turso_core::Value;
use turso_sqlite::SqliteDialect;

/// 25 documents inserted by one statement, so the index holds them in one segment in id order:
/// ids 1 to 20 match 'hello' and the rest do not, and every match is in category 0 but the last,
/// id 20, in category 1.
fn documents_in_categories() -> Arc<Connection> {
    let db = turso_core::Database::open(
        Arc::new(turso_core::MemoryIO::new()),
        ":memory:",
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(turso_core::DatabaseOpts::default().with_index_method(true)),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT, cat INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    let rows: Vec<String> = (1..=25i64)
        .map(|id| {
            let body = if id <= 20 {
                "hello ".repeat((id % 3 + 1) as usize) + "world"
            } else {
                "goodbye world".to_string()
            };
            format!("({id}, '{body}', {})", i64::from(id == 20))
        })
        .collect();
    conn.execute(format!("INSERT INTO docs VALUES {}", rows.join(", ")))
        .unwrap();
    conn
}

fn collected(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

/// An FTS pattern with LIMIT applies the LIMIT to the matches it gives out, so it cannot serve a
/// query whose aggregate or DISTINCT comes before the LIMIT: the aggregate counts every match, and
/// the DISTINCT keeps every category.
#[test]
fn fts_limit_applies_after_aggregates_and_distinct() {
    let conn = documents_in_categories();
    assert_eq!(
        collected(
            &conn,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'hello') LIMIT 5"
        ),
        vec![vec![Value::from_i64(20)]]
    );
    let mut categories = collected(
        &conn,
        "SELECT DISTINCT cat FROM docs WHERE fts_match(body, 'hello') LIMIT 2",
    );
    categories.sort_by_key(|row| row[0].as_int());
    assert_eq!(
        categories,
        vec![vec![Value::from_i64(0)], vec![Value::from_i64(1)]]
    );
}

/// Nine documents inserted by one statement, mentioning 'hello' once, twice or three times, so
/// their FTS scores take three values.
fn scored_documents() -> Arc<Connection> {
    let db = turso_core::Database::open(
        Arc::new(turso_core::MemoryIO::new()),
        ":memory:",
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(turso_core::DatabaseOpts::default().with_index_method(true)),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX notes_fts ON notes USING fts(body)")
        .unwrap();
    let rows: Vec<String> = (1..=9i64)
        .map(|id| format!("({id}, '{}world')", "hello ".repeat((id % 3 + 1) as usize)))
        .collect();
    conn.execute(format!("INSERT INTO notes VALUES {}", rows.join(", ")))
        .unwrap();
    conn
}

fn rows_of(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

/// The score the FTS index provides for a query is a computed value with no column collation, so
/// a DISTINCT, a compound SELECT and an IN subquery over it plan and run.
#[test]
fn fts_score_from_the_index_has_no_column_collation() {
    let conn = scored_documents();
    let score = "fts_score(body, 'hello')";
    let matching = "FROM notes WHERE fts_match(body, 'hello')";
    assert_eq!(
        rows_of(&conn, &format!("SELECT DISTINCT {score} {matching}")).len(),
        3
    );
    assert_eq!(
        rows_of(
            &conn,
            &format!("SELECT {score} {matching} UNION SELECT {score} {matching}")
        )
        .len(),
        3
    );
    assert_eq!(
        rows_of(
            &conn,
            &format!(
                "SELECT count(*) FROM (SELECT {score} AS s {matching}) WHERE s IN (SELECT {score} {matching})"
            )
        ),
        vec![vec![Value::from_i64(9)]]
    );
}

#[test]
fn field_weights_reject_non_finite_and_non_positive_values() {
    let mut accepted = Vec::new();
    for weight in [
        "NaN",
        "nan",
        "+NaN",
        "-NaN",
        "inf",
        "+inf",
        "-inf",
        "Infinity",
        "+INFINITY",
        "-infinity",
        "1e39",
        "-1e39",
        "0",
        "-0",
        "-1",
        "1e-46",
    ] {
        let result = FtsIndexAttachment::new(
            &SqliteDialect,
            IndexMethodConfiguration {
                table_name: "w".to_string(),
                index_name: "wx".to_string(),
                columns: turso_core::alloc::vec![
                    IndexColumn::new("title", 0, CollationSeq::BINARY),
                    IndexColumn::new("body", 1, CollationSeq::BINARY)
                ],
                parameters: turso_core::HashMap::from_iter([(
                    "weights".to_string(),
                    Value::from_text(format!("title={weight},body=1")),
                )]),
            },
        );
        match result {
            Ok(_) => accepted.push(weight),
            Err(error) => assert!(
                matches!(error, LimboError::ParseError(_)),
                "{weight}: {error}"
            ),
        }
    }
    assert!(
        accepted.is_empty(),
        "accepted invalid weights: {accepted:?}"
    );
}

/// Drive a resumable step to its end, waiting on the I/O it asks for.
#[cfg(feature = "test_helper")]
fn drive<T>(
    io: &Arc<dyn turso_core::IO>,
    mut step: impl FnMut() -> turso_core::types::IOResultOr<T>,
) -> T {
    loop {
        match step().unwrap() {
            turso_core::types::IOResult::Done(value) => return value,
            turso_core::types::IOResult::IO(completion) => completion.wait(io.as_ref()).unwrap(),
        }
    }
}

/// The bytes of every row of the FTS index `docs_fts`'s backing store, read in a transaction.
#[cfg(feature = "test_helper")]
pub(super) fn backing_bytes(
    conn: &Arc<Connection>,
    io: &Arc<dyn turso_core::IO>,
) -> std::collections::HashSet<Vec<u8>> {
    conn.execute("BEGIN").unwrap();
    conn.prepare("SELECT count(*) FROM docs")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    let mut dumper = turso_core::index_method::fts::FtsBackingRowDumper::new(
        conn,
        turso_core::MAIN_DB_ID,
        "docs_fts",
    )
    .unwrap();
    drive(io, || dumper.step());
    let rows = std::mem::take(&mut dumper.row_bytes)
        .into_iter()
        .collect::<std::collections::HashSet<Vec<u8>>>();
    drop(dumper);
    conn.execute("COMMIT").unwrap();
    rows
}

/// REINDEX makes an FTS index afresh from the table's rows: after documents are deleted and the
/// index made again, a search finds the documents left alone, and under `secure_delete` none of the
/// segment files and tombstones of the index it replaced is left in the checkpointed file, in WAL
/// and in MVCC mode.
#[cfg(feature = "test_helper")]
#[test]
fn reindex_makes_the_index_afresh_and_leaves_none_of_the_old_one_in_the_file() {
    use std::collections::HashSet;
    use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags, PlatformIO};
    let ids = |conn: &Arc<Connection>, term: &str| -> Vec<i64> {
        conn.prepare(format!(
            "SELECT id FROM docs WHERE fts_match(body, '{term}') ORDER BY id"
        ))
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].as_int().unwrap())
        .collect()
    };
    for mvcc in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reindex.db");
        let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
            .unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("PRAGMA secure_delete = ON").unwrap();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        for batch in 0..8 {
            let values: Vec<String> = (batch * 50..batch * 50 + 50)
                .map(|id| {
                    format!(
                        "({id}, 'shared words of document mark{id} and some more text {}')",
                        "filler ".repeat(id as usize % 7)
                    )
                })
                .collect();
            conn.execute(format!("INSERT INTO docs VALUES {}", values.join(",")))
                .unwrap();
        }
        conn.execute("DELETE FROM docs WHERE id % 3 = 0").unwrap();
        let old = backing_bytes(&conn, &io);
        conn.execute("DELETE FROM docs WHERE id % 2 = 0").unwrap();
        conn.execute("REINDEX docs_fts").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(ids(&conn, "mark40"), Vec::<i64>::new(), "mvcc={mvcc}");
        assert_eq!(ids(&conn, "mark41"), vec![41], "mvcc={mvcc}");
        assert_eq!(ids(&conn, "mark39"), Vec::<i64>::new(), "mvcc={mvcc}");
        assert_eq!(
            ids(&conn, "shared").len(),
            (0..400).filter(|id| id % 2 == 1 && id % 3 != 0).count(),
            "mvcc={mvcc}"
        );
        let new = backing_bytes(&conn, &io);
        drop(conn);
        drop(db);
        let kept = old
            .iter()
            .filter(|blob| blob.len() >= 16 && new.contains(*blob))
            .count();
        assert_eq!(
            kept, 0,
            "mvcc={mvcc}: the index made again holds {kept} rows of the one it replaced"
        );
        let gone: Vec<&Vec<u8>> = old.iter().filter(|blob| blob.len() >= 16).collect();
        assert!(
            gone.len() > 8,
            "mvcc={mvcc}: the old index held only {} rows",
            gone.len()
        );
        let file = std::fs::read(&path).unwrap();
        let prefixes: HashSet<&[u8]> = gone.iter().map(|blob| &blob[..16]).collect();
        let seen: HashSet<&[u8]> = file.windows(16).filter(|w| prefixes.contains(w)).collect();
        let left = gone
            .iter()
            .filter(|blob| {
                seen.contains(&blob[..16]) && file.windows(blob.len()).any(|w| w == blob.as_slice())
            })
            .count();
        assert_eq!(
            left,
            0,
            "mvcc={mvcc}: {left} of {} rows of the old index left in the file",
            gone.len()
        );
    }
}
