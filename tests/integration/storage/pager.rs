use turso_core::io::MemoryIO;
use turso_core::io::IO;
use turso_core::security::roles::SessionRole;
use turso_core::storage::sqlite3_ondisk::PageSize;
use turso_core::sync::Arc;
use turso_sqlite::SqliteDialect;

#[test]
#[should_panic(expected = "rollback must discard pages while holding the WAL write lock")]
fn rollback_without_the_write_lock_panics() {
    let (_db, conn) = open_memory_database("rollback-without-write-lock.db");
    conn.execute("CREATE TABLE t(x)").unwrap();

    conn.get_pager().rollback(false, &conn, true);
}

#[test]
fn rollback_keeps_pages_the_transaction_did_not_change() {
    let (db, conn) = open_memory_database("rollback-keeps-unchanged-pages.db");
    conn.execute("CREATE TABLE changed(x)").unwrap();
    conn.execute("CREATE TABLE unchanged(x)").unwrap();
    conn.execute("INSERT INTO changed VALUES (1)").unwrap();
    conn.execute("INSERT INTO unchanged VALUES (1)").unwrap();
    let changed_root = root_page(&conn, "changed");
    let unchanged_root = root_page(&conn, "unchanged");
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unchanged"), 1);
    let pager = conn.get_pager();
    let cached_unchanged_root = pager.cache_get(unchanged_root).unwrap().unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO changed VALUES (2)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    assert!(!pager.cache_contains(changed_root));
    let unchanged_root_after_rollback = pager.cache_get(unchanged_root).unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &cached_unchanged_root,
        &unchanged_root_after_rollback
    ));
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM changed"), 1);
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unchanged"), 1);
    drop(db);
}

#[test]
fn rollback_drops_pages_spilled_to_the_wal() {
    let (db, conn) = open_memory_database("rollback-drops-spilled-pages.db");
    conn.execute("PRAGMA cache_size = 200").unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t SELECT randomblob(1000) FROM generate_series(1, 500)")
        .unwrap();
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 500);
    let pager = conn.get_pager();
    let wal = pager.wal().unwrap();
    let committed_max_frame = wal.get_max_frame();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET x = randomblob(1000)").unwrap();
    conn.execute("INSERT INTO t SELECT randomblob(1000) FROM generate_series(1, 1000)")
        .unwrap();
    assert!(
        wal.get_max_frame() > committed_max_frame,
        "the transaction must spill pages to the WAL"
    );
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 1500);
    let page_count = query_i64(&conn, "PRAGMA page_count") as usize;
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(wal.get_max_frame(), committed_max_frame);
    for page_idx in 1..=page_count {
        let Some(page) = pager.cache_get(page_idx).unwrap() else {
            continue;
        };
        assert!(!page.is_dirty());
        assert!(!page.has_wal_tag() || page.wal_tag_pair().0 <= committed_max_frame);
    }
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 500);
    let mut integrity_check = conn.prepare("PRAGMA integrity_check").unwrap();
    assert_eq!(
        integrity_check.run_collect_rows().unwrap(),
        vec![vec![turso_core::Value::build_text("ok")]]
    );
    drop(db);
}

#[test]
fn rollback_drops_pinned_pages_that_are_not_loaded() {
    let (db, conn) = open_memory_database("rollback-drops-pinned-unloaded-pages.db");
    conn.execute("CREATE TABLE changed(x)").unwrap();
    conn.execute("CREATE TABLE unloaded(x)").unwrap();
    conn.execute("INSERT INTO unloaded VALUES (1)").unwrap();
    let unloaded_root = root_page(&conn, "unloaded");
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unloaded"), 1);
    let pager = conn.get_pager();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO changed VALUES (1)").unwrap();
    let page = pager.cache_get(unloaded_root).unwrap().unwrap();
    page.pin();
    page.clear_loaded();
    conn.execute("ROLLBACK").unwrap();

    assert!(!pager.cache_contains(unloaded_root));
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unloaded"), 1);
    drop(db);
}

#[test]
fn record_pool_frees_buffers_of_rows_larger_than_a_page() {
    let (_db, conn) = open_memory_database("record-pool-large-rows.db");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT NOT NULL, tag TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST(zeroblob(6 * 1024 * 1024) AS TEXT), 'big')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST(zeroblob(5000) AS TEXT), 'small')")
        .unwrap();
    conn.execute("SELECT body FROM t WHERE id = 1").unwrap();
    conn.execute("UPDATE t SET tag = 'updated' WHERE id = 1")
        .unwrap();
    conn.execute("UPDATE t SET tag = 'updated' WHERE id = 2")
        .unwrap();

    let capacities = conn.get_pager().record_pool_capacities();
    assert!(!capacities.is_empty());
    for capacity in capacities {
        assert!(capacity <= PageSize::MAX as usize);
    }
}

fn open_memory_database(path: &str) -> (Arc<turso_core::Database>, Arc<turso_core::Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = turso_core::Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    (db, conn)
}

fn root_page(conn: &Arc<turso_core::Connection>, table: &str) -> usize {
    query_i64(
        conn,
        &format!("SELECT rootpage FROM sqlite_schema WHERE name = '{table}'"),
    ) as usize
}

fn query_i64(conn: &Arc<turso_core::Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    match rows.as_slice() {
        [row] => match row.as_slice() {
            [turso_core::Value::Numeric(turso_core::Numeric::Integer(value))] => *value,
            other => panic!("expected one integer, got {other:?}"),
        },
        other => panic!("expected one row, got {other:?}"),
    }
}

/// A read drops from its connection's page cache only what other connections changed since its
/// last read.
mod read_changes_tests {
    use std::collections::BTreeMap;

    use turso_core::sync::Arc;
    use turso_core::{ConnectOpts, Connection, Database, IOExt, MemoryIO, Value};
    use turso_sqlite::SqliteDialect;

    /// Each row's version and its body's length, by id.
    type Rows = BTreeMap<i64, (i64, usize)>;

    fn database(name: &str, mvcc: bool) -> Arc<Database> {
        let db =
            Database::open_file(Arc::new(MemoryIO::new()), name, Arc::new(SqliteDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
            .unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL, body TEXT NOT NULL)",
        )
        .unwrap();
        db
    }

    fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
        try_rows(conn, sql).unwrap()
    }

    fn try_rows(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<Vec<Vec<Value>>> {
        let mut stmt = conn.prepare(sql)?;
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect());
            Ok(())
        })?;
        Ok(rows)
    }

    /// A body of `length` copies of the letter of version `v`: its first and last characters
    /// tell a stale page, its own or an overflow page's.
    fn body(v: i64, length: usize) -> String {
        let letter = char::from(b'a' + (v % 26) as u8);
        std::iter::repeat_n(letter, length).collect()
    }

    fn write(conn: &Arc<Connection>, id: i64, v: i64, length: usize) {
        conn.execute(format!(
            "INSERT INTO t VALUES ({id}, {v}, '{}') ON CONFLICT(id) DO UPDATE SET v = excluded.v, body = excluded.body",
            body(v, length)
        ))
        .unwrap();
    }

    /// Every row as the connection reads it, checked against its body.
    fn read(conn: &Arc<Connection>) -> Rows {
        try_read(conn).unwrap()
    }

    fn try_read(conn: &Arc<Connection>) -> turso_core::Result<Rows> {
        Ok(try_rows(conn, "SELECT id, v, length(body), substr(body, 1, 1), substr(body, -1, 1) FROM t ORDER BY id")?
            .into_iter()
            .map(|row| {
                let id = row[0].as_int().unwrap();
                let v = row[1].as_int().unwrap();
                let length = row[2].as_int().unwrap() as usize;
                let letter = body(v, 1);
                for end in [&row[3], &row[4]] {
                    assert_eq!(end.to_string(), letter, "row {id} at version {v} has a body of another version");
                }
                (id, (v, length))
            })
            .collect())
    }

    fn cached(conn: &Arc<Connection>) -> usize {
        conn.get_pager().cache_len()
    }

    /// A reader beside a writer of one row keeps every other page it holds, whether the change
    /// is in the WAL's generation its last read saw, or past a checkpoint that restarted the WAL.
    #[test]
    fn a_read_keeps_the_pages_another_connection_did_not_write() {
        for mvcc in [false, true] {
            let db = database(&format!("read-keeps-{mvcc}.db"), mvcc);
            let writer = db
                .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
                .unwrap();
            let mut expected = Rows::new();
            for id in 1..=2000 {
                write(&writer, id, 0, 300);
                expected.insert(id, (0, 300));
            }
            writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            let reader = db
                .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
                .unwrap();
            assert_eq!(read(&reader), expected);
            let warm = cached(&reader);
            assert!(warm > 100, "a scan of the table left {warm} pages cached");
            for (id, restart) in [(1000, false), (1500, true)] {
                write(&writer, id, 1, 300);
                expected.insert(id, (1, 300));
                if restart || mvcc {
                    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
                }
                let v = rows(&reader, &format!("SELECT v FROM t WHERE id = {id}"));
                assert_eq!(v[0][0].as_int(), Some(1));
                let kept = cached(&reader);
                assert!(
                    kept + 8 >= warm,
                    "mvcc {mvcc}, restart {restart}: a read after a one-row update kept {kept} of {warm} pages"
                );
            }
            assert_eq!(read(&reader), expected);
        }
    }

    /// A connection that restarts the WAL in a checkpoint of its own, after another connection's
    /// checkpoint backfilled frames it never read, drops the pages those frames held: the restart
    /// moves its snapshot past them without a read. MVCC's checkpoint calls the WAL's checkpoint
    /// this way when it has nothing of its own to write.
    #[test]
    fn a_restart_past_frames_a_connection_never_read_drops_their_pages() {
        let db = database("restart-past-unread.db", false);
        let a = db
            .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
            .unwrap();
        let b = db
            .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
            .unwrap();
        let mut expected = Rows::new();
        for id in 1..=500 {
            write(&a, id, 0, 300);
            expected.insert(id, (0, 300));
        }
        a.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(read(&a), expected);
        for id in (1..=500).step_by(7) {
            write(&b, id, 1, 300);
            expected.insert(id, (1, 300));
        }
        b.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let pager = a.get_pager();
        let wal = pager.wal().unwrap();
        let truncate = turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        };
        let mut done = pager
            .io
            .block(|| wal.checkpoint(&pager, truncate, turso_core::SyncMode::Full))
            .unwrap();
        done.release_guard();
        assert_eq!(read(&a), expected);
    }

    /// Connections writing, reading in and out of transactions and checkpointing in every mode,
    /// interleaved at random with small page caches, read every commit their snapshots should.
    /// READ_CHANGES_SEEDS sets how many seeds run in each journal mode.
    #[test]
    fn reads_beside_writes_and_checkpoints_see_every_commit() {
        let seeds = std::env::var("READ_CHANGES_SEEDS")
            .ok()
            .and_then(|seeds| seeds.parse().ok())
            .unwrap_or(2u64);
        for mvcc in [false, true] {
            for seed in 0..seeds {
                workload(mvcc, seed);
            }
        }
    }

    fn workload(mvcc: bool, seed: u64) {
        let db = database(&format!("read-changes-{mvcc}-{seed}.db"), mvcc);
        let conns: Vec<_> = (0..4)
            .map(|_| {
                db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
                    .unwrap()
            })
            .collect();
        for conn in &conns {
            conn.execute("PRAGMA cache_size = 64").unwrap();
        }
        let begin = if mvcc { "BEGIN CONCURRENT" } else { "BEGIN" };
        let mut state = 0x9e37_79b9_7f4a_7c15 ^ seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1;
        let mut next = move |below: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % below
        };
        let mut model = Rows::new();
        // The rows each connection's open transaction reads.
        let mut snapshots: Vec<Option<Rows>> = vec![None; conns.len()];
        let mut reads = 0;
        let mut log: Vec<String> = Vec::new();
        for action in 0..400 {
            let who = next(conns.len() as u64) as usize;
            let conn = &conns[who];
            let context = format!("mvcc {mvcc}, seed {seed}, action {action}, connection {who}");
            match next(10) {
                0..=3 if snapshots[who].is_none() => {
                    let mut changed = model.clone();
                    conn.execute(begin).unwrap();
                    // Now and then a transaction large enough to spill past its page cache.
                    let changes = if next(10) == 0 { 150 } else { 1 + next(4) };
                    for _ in 0..changes {
                        let id = 1 + next(400) as i64;
                        if next(5) == 0 {
                            conn.execute(format!("DELETE FROM t WHERE id = {id}"))
                                .unwrap();
                            changed.remove(&id);
                        } else {
                            let v = changed.get(&id).map_or(0, |(v, _)| v + 1);
                            // Bodies up to twice a page long, so some rows overflow.
                            let length = 1 + next(8000) as usize;
                            write(conn, id, v, length);
                            changed.insert(id, (v, length));
                        }
                    }
                    if next(8) == 0 {
                        conn.execute("ROLLBACK").unwrap();
                        log.push(format!("{action}: {who} rolled back {changes} changes"));
                    } else {
                        conn.execute("COMMIT").unwrap();
                        model = changed;
                        log.push(format!("{action}: {who} committed {changes} changes"));
                    }
                }
                4..=6 => {
                    let expected = snapshots[who].as_ref().unwrap_or(&model);
                    let seen = try_read(conn).unwrap_or_else(|error| {
                        panic!(
                            "{context}: {error}\n{}",
                            log[log.len().saturating_sub(40)..].join("\n")
                        )
                    });
                    assert_eq!(&seen, expected, "{context}");
                    log.push(format!("{action}: {who} read"));
                    reads += 1;
                }
                7 => match snapshots[who].take() {
                    Some(_) => {
                        conn.execute("COMMIT").unwrap();
                        log.push(format!("{action}: {who} ended its read"));
                    }
                    None => {
                        conn.execute(begin).unwrap();
                        let seen = try_read(conn).unwrap_or_else(|error| {
                            panic!(
                                "{context}: {error}\n{}",
                                log[log.len().saturating_sub(40)..].join("\n")
                            )
                        });
                        assert_eq!(seen, model, "{context}");
                        snapshots[who] = Some(model.clone());
                        log.push(format!("{action}: {who} began a read"));
                    }
                },
                8 if snapshots[who].is_none() => {
                    let mode = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"][next(4) as usize];
                    // A checkpoint other connections' transactions keep from finishing reports busy.
                    let done = conn.execute(format!("PRAGMA wal_checkpoint({mode})"));
                    log.push(format!("{action}: {who} checkpoint {mode} {done:?}"));
                }
                _ => {}
            }
        }
        assert!(
            reads > 50,
            "mvcc {mvcc}, seed {seed}: only {reads} reads were checked"
        );
    }
}
