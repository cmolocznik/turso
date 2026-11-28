use crate::common::{compute_dbhash, do_flush, maybe_setup_tracing, TempDatabase};
use asserting::prelude::*;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use turso_core::{
    CheckpointMode, CheckpointResult, ConnectOpts, Connection, IOExt, LimboError, Result,
    SessionRole, SyncMode,
};

use super::test_checkpoint::get_database;

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_wal_checkpoint_result(tmp_db: TempDatabase) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t1 (id text);")?;

    let res = execute_and_get_strings(&conn, "pragma journal_mode;")?;
    assert_eq!(res, vec!["wal"]);

    conn.execute("insert into t1(id) values (1), (2);")?;
    do_flush(&conn, &tmp_db).unwrap();
    conn.execute("select * from t1;")?;
    do_flush(&conn, &tmp_db).unwrap();

    // hash BEFORE checkpoint
    let hash_before = compute_dbhash(&tmp_db);

    // checkpoint result should return > 0 num pages now as database has data
    let res = execute_and_get_ints(&conn, "pragma wal_checkpoint;")?;
    println!("'pragma wal_checkpoint;' returns: {res:?}");
    // [busy, pages in the wal, pages checkpointed]
    assert_that!(res)
        .has_length(3)
        .satisfies_with_message("report no busy flag and some pages", |res| {
            res[0] == 0 && res[1] > 0 && res[2] > 0
        });

    do_flush(&conn, &tmp_db).unwrap();

    // hash AFTER checkpoint - must be identical
    let hash_after = compute_dbhash(&tmp_db);
    // A checkpoint must not change the database content.
    assert_that!(hash_after.hash).is_equal_to(hash_before.hash);

    Ok(())
}

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_passive_checkpoint_between_read_and_write_does_not_fail_the_write(
    tmp_db: TempDatabase,
) -> Result<()> {
    let writer = tmp_db.connect_limbo();
    let checkpointer = tmp_db.connect_limbo();
    writer.execute("CREATE TABLE t (x)")?;
    writer.execute("INSERT INTO t VALUES (1)")?;

    writer.execute("BEGIN")?;
    assert_eq!(execute_and_get_ints(&writer, "SELECT x FROM t")?, vec![1]);
    let res = execute_and_get_ints(&checkpointer, "PRAGMA wal_checkpoint(PASSIVE)")?;
    assert_eq!(res[0], 0, "checkpoint must not be busy");
    assert_eq!(res[1], res[2], "checkpoint must backfill every frame");
    writer.execute("INSERT INTO t VALUES (2)")?;
    writer.execute("COMMIT")?;

    assert_eq!(
        execute_and_get_ints(&checkpointer, "SELECT x FROM t ORDER BY x")?,
        vec![1, 2]
    );
    Ok(())
}

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_truncate_checkpoint_not_busy_after_rollback(tmp_db: TempDatabase) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x);")?;
    conn.execute("INSERT INTO t VALUES (randomblob(10 * 4096 + 0));")?;
    conn.execute("INSERT INTO t VALUES (randomblob(10 * 4096 + 1));")?;
    conn.execute("INSERT INTO t VALUES (randomblob(10 * 4096 + 2));")?;

    conn.execute("BEGIN;")?;
    conn.execute("INSERT INTO t VALUES (1);")?;
    conn.execute("ROLLBACK;")?;

    let checkpoint = execute_and_get_ints(&conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;
    // A truncate checkpoint must not come back busy after a rollback.
    assert_that!(checkpoint).is_equal_to(vec![0, 0, 0]);
    Ok(())
}

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_savepoint_rollback_after_cache_spill_preserves_wal_pages(
    tmp_db: TempDatabase,
) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA page_size=512;")?;
    conn.execute("PRAGMA journal_mode=WAL;")?;
    conn.execute("PRAGMA cache_size=50;")?;

    conn.execute("CREATE TABLE filler(x INTEGER PRIMARY KEY);")?;
    conn.execute("INSERT INTO filler(x) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);")?;

    conn.execute("BEGIN;")?;
    conn.execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT NOT NULL);")?;
    conn.execute("CREATE TABLE b(k INTEGER PRIMARY KEY, v TEXT NOT NULL);")?;
    conn.execute(
        "INSERT INTO t(v)
         SELECT printf('%0*d', 4000, f1.x*1000 + f2.x)
         FROM filler f1, filler f2
         LIMIT 25;",
    )?;
    conn.execute(
        "UPDATE t
         SET v = printf('%0*d', 4000, k + 10000000)
         WHERE k BETWEEN 1 AND 23;",
    )?;
    conn.execute("SAVEPOINT s1;")?;
    conn.execute(
        "INSERT INTO b(v)
         SELECT printf('%0*d', 4000, f1.x*1000 + f2.x)
         FROM filler f1, filler f2
         LIMIT 40;",
    )?;
    conn.execute("ROLLBACK TO s1;")?;
    conn.execute("RELEASE s1;")?;
    conn.execute("COMMIT;")?;

    let res = execute_and_get_strings(&conn, "PRAGMA integrity_check;")?;
    assert_eq!(res, vec!["ok"]);

    Ok(())
}

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_wal_cacheflush_savepoint_rollback_preserves_pre_savepoint_indexed_rows(
    tmp_db: TempDatabase,
) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA page_size=512;")?;
    conn.execute("PRAGMA journal_mode=WAL;")?;
    conn.execute("PRAGMA cache_size=200;")?;
    conn.execute("PRAGMA data_sync_retry=1;")?;

    conn.execute(
        "CREATE TABLE shy_rain_806 (
            hot_cave_669 BLOB NOT NULL,
            blue_wind_834 TEXT,
            hard_star_751 NUMERIC UNIQUE,
            angry_stone_202 INTEGER,
            slow_sun_632 NUMERIC NOT NULL,
            fast_fish_819 REAL PRIMARY KEY,
            happy_lake_980 REAL NOT NULL,
            brave_fish_935 NUMERIC NOT NULL
        );",
    )?;

    for i in 0..260 {
        conn.execute(format!(
            "INSERT INTO shy_rain_806
             (hot_cave_669, blue_wind_834, hard_star_751, angry_stone_202,
              slow_sun_632, fast_fish_819, happy_lake_980, brave_fish_935)
             VALUES (x'73656564', 'seed_{i}', {}, {}, {}, {:.2}, {:.2}, {});",
            10_000 + i,
            20_000 + i,
            30_000 + i,
            i as f64 + 0.25,
            i as f64 + 0.75,
            40_000 + i,
        ))?;
    }

    conn.execute("BEGIN;")?;
    conn.execute(
        "UPDATE shy_rain_806
         SET hot_cave_669 = x'75706461746564',
             blue_wind_834 = 'updated',
             hard_star_751 = hard_star_751 + 100000,
             angry_stone_202 = angry_stone_202 + 1,
             happy_lake_980 = happy_lake_980 + 1.0
         WHERE fast_fish_819 BETWEEN 180.25 AND 240.25;",
    )?;
    conn.execute("SAVEPOINT sp_55;")?;
    for completion in conn.cacheflush()? {
        tmp_db.io.wait_for_completion(completion)?;
    }
    conn.execute(
        "UPDATE shy_rain_806
         SET blue_wind_834 = 'rolled_back',
             hard_star_751 = hard_star_751 + 50000
         WHERE fast_fish_819 = 204.25;",
    )?;
    conn.execute("ROLLBACK TO sp_55;")?;
    conn.execute("RELEASE sp_55;")?;
    conn.execute("COMMIT;")?;

    let res = execute_and_get_strings(&conn, "PRAGMA integrity_check;")?;
    assert_eq!(res, vec!["ok"]);
    let count = execute_and_get_ints(&conn, "SELECT COUNT(*) FROM shy_rain_806;")?;
    assert_eq!(count, vec![260]);
    let updated = execute_and_get_ints(
        &conn,
        "SELECT COUNT(*) FROM shy_rain_806 WHERE hard_star_751 >= 110000;",
    )?;
    assert_eq!(updated, vec![61]);

    Ok(())
}

#[test]
#[ignore = "ignored for now because it's flaky"]
fn test_wal_1_writer_1_reader() -> Result<()> {
    maybe_setup_tracing();
    let tmp_db = Arc::new(Mutex::new(TempDatabase::new("test_wal.db")));
    let db = tmp_db.lock().unwrap().limbo_database();

    {
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        match conn.query("CREATE TABLE t (id)")? {
            Some(ref mut rows) => {
                rows.run_with_row_callback(|_| Ok(())).unwrap();
            }
            None => todo!(),
        }
        do_flush(&conn, tmp_db.lock().unwrap().deref()).unwrap();
    }
    let rows = Arc::new(std::sync::Mutex::new(0));
    let rows_ = rows.clone();
    const ROWS_WRITE: usize = 1000;
    let tmp_db_w = db.clone();
    let writer_thread = std::thread::spawn(move || {
        let conn = tmp_db_w
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        for i in 0..ROWS_WRITE {
            conn.execute(format!("INSERT INTO t values({i})").as_str())
                .unwrap();
            let mut rows = rows_.lock().unwrap();
            *rows += 1;
        }
    });
    let rows_ = rows.clone();
    let reader_thread = std::thread::spawn(move || {
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        loop {
            let rows = *rows_.lock().unwrap();
            let mut i = 0;
            match conn.query("SELECT * FROM t") {
                Ok(Some(ref mut rows)) => {
                    rows.run_with_row_callback(|row| {
                        let id = row.get::<i64>(0).unwrap();
                        assert_eq!(id, i);
                        i += 1;
                        Ok(())
                    })
                    .unwrap();
                }
                Ok(None) => {}
                Err(err) => {
                    eprintln!("{err}");
                }
            }
            if rows == ROWS_WRITE {
                break;
            }
        }
    });

    writer_thread.join().unwrap();
    reader_thread.join().unwrap();
    Ok(())
}

/// Execute a statement and get strings result
pub(crate) fn execute_and_get_strings(conn: &Arc<Connection>, sql: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut result = Vec::new();

    stmt.run_with_row_callback(|row| {
        for el in row.get_values() {
            result.push(format!("{el}"));
        }
        Ok(())
    })?;
    Ok(result)
}

/// Execute a statement and get integers
pub(crate) fn execute_and_get_ints(conn: &Arc<Connection>, sql: &str) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare(sql)?;
    let mut result = Vec::new();

    stmt.run_with_row_callback(|row| {
        for value in row.get_values() {
            let out = match value {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => i,
                _ => {
                    return Err(LimboError::ConversionError(format!(
                        "cannot convert {value} to int"
                    )))
                }
            };
            result.push(*out);
        }
        Ok(())
    })?;

    Ok(result)
}

#[test]
fn test_wal_read_lock_released_on_conn_drop() {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal_read_lock_released.db");
    let db = tmp_db.limbo_database();

    // Setup: create table and insert data so WAL has content
    let setup_conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    setup_conn
        .execute("CREATE TABLE t (id integer primary key)")
        .unwrap();
    setup_conn.execute("INSERT INTO t VALUES (1)").unwrap();

    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // conn1 starts a read transaction and panics while holding the read lock
    let join_result = std::thread::spawn(move || {
        conn1.execute("BEGIN").unwrap();
        conn1.execute("SELECT * FROM t").unwrap();
        panic!("intentional panic while holding read tx");
    })
    .join();
    assert!(join_result.is_err(), "conn1 thread should panic");

    // TRUNCATE checkpoint requires that there be no readers - this would hang/fail if read lock wasn't released
    let res = conn2.pragma_update("wal_checkpoint", "TRUNCATE").unwrap();
    let row = res.first().unwrap();
    let truncate_succeeded = row.first().unwrap() == &turso_core::Value::from_i64(0)
        && row.get(1).unwrap() == &turso_core::Value::from_i64(0)
        && row.get(2).unwrap() == &turso_core::Value::from_i64(0);

    // Expect full truncate, i.e. 0 0 0 result.
    assert!(
        truncate_succeeded,
        "truncate should have succeeded, got checkpoint result: {res:?}"
    );
}

#[test]
fn test_deferred_transaction_first_write_retries_stale_snapshot() {
    maybe_setup_tracing();
    use std::sync::atomic::{AtomicBool, Ordering};

    let tmp_db = TempDatabase::new("test_deferred_first_write_stale_snapshot.db");
    let db = tmp_db.limbo_database();
    let setup = tmp_db.connect_limbo();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT)")
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || {
        let conn = writer_db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        while !writer_stop.load(Ordering::Relaxed) {
            match conn.execute("INSERT INTO t(source) VALUES ('writer')") {
                Ok(()) | Err(LimboError::Busy) => {}
                Err(err) => panic!("writer failed: {err:?}"),
            }
        }
    });

    let conn = tmp_db.connect_limbo();
    let mut result = Ok(());
    for _ in 0..20_000 {
        conn.execute("BEGIN DEFERRED").unwrap();
        result = match conn.execute("INSERT INTO t(source) VALUES ('deferred')") {
            Ok(()) | Err(LimboError::Busy) => Ok(()),
            Err(err) => Err(err),
        };
        conn.execute("ROLLBACK").unwrap();
        if result.is_err() {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        result.is_ok(),
        "first write after BEGIN DEFERRED must not fail on a stale snapshot, got {result:?}"
    );
}

#[test]
fn test_wal_write_lock_released_on_conn_drop() {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal_write_lock_released.db");
    let db = tmp_db.limbo_database();

    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let join_result = std::thread::spawn(move || {
        conn1.execute("BEGIN IMMEDIATE").unwrap();
        panic!("intentional panic while holding write tx");
    })
    .join();
    assert!(join_result.is_err(), "conn1 thread should panic");

    conn2.set_busy_handler(Some(Box::new(move |_| {
        panic!("Got busy, this should not happen");
    })));

    conn2
        .execute("CREATE TABLE t (id integer primary key)")
        .unwrap();
}

/// A WAL whose header checksum is wrong must be ignored, the way SQLite's
/// `walIndexRecover()` ignores it. Its page size must not be used either: a
/// header claiming 512-byte pages next to a 4096-byte-page database used to
/// make the next write fail and truncate the main file on close.
#[test]
fn test_wal_with_bad_header_checksum_is_ignored() -> anyhow::Result<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let source_path = temp_dir.path().join("source.db");
    let db_path = temp_dir.path().join("bad_wal_header.db");
    let wal_path = db_path.with_extension("db-wal");

    // Copy the pair aside while the WAL still holds uncheckpointed rows,
    // because SQLite checkpoints and removes the WAL when it closes.
    let sqlite = rusqlite::Connection::open(&source_path)?;
    sqlite.pragma_update(None, "journal_mode", "WAL")?;
    sqlite.execute_batch(
        "CREATE TABLE t(x TEXT);
         WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<50)
         INSERT INTO t SELECT printf('%0100d', i) FROM c;
         PRAGMA wal_checkpoint(TRUNCATE);
         WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<20)
         INSERT INTO t SELECT printf('%0100d', i) FROM c;",
    )?;
    std::fs::copy(&source_path, &db_path)?;
    std::fs::copy(source_path.with_extension("db-wal"), &wal_path)?;
    drop(sqlite);

    let mut wal = std::fs::read(&wal_path)?;
    wal[8..12].copy_from_slice(&512u32.to_be_bytes());
    wal[24..32].fill(0xab);
    std::fs::write(&wal_path, wal)?;

    {
        let tmp_db = TempDatabase::new_with_existent(&db_path);
        let conn = tmp_db.connect_limbo();
        assert_eq!(
            execute_and_get_ints(&conn, "SELECT count(*) FROM t")?,
            vec![50]
        );
        conn.execute("INSERT INTO t VALUES ('after')")?;
        assert_eq!(
            execute_and_get_ints(&conn, "SELECT count(*) FROM t")?,
            vec![51]
        );
        conn.close()?;
    }

    let sqlite = rusqlite::Connection::open(&db_path)?;
    let quick_check: String = sqlite.query_row("PRAGMA quick_check", (), |row| row.get(0))?;
    assert_eq!(quick_check, "ok");
    let count: i64 = sqlite.query_row("SELECT count(*) FROM t", (), |row| row.get(0))?;
    assert_eq!(count, 51);
    Ok(())
}

fn bulk_inserts(conn: &Arc<Connection>, n_txns: usize, rows_per_txn: usize) {
    for _ in 0..n_txns {
        conn.execute("begin transaction").unwrap();
        for i in 0..rows_per_txn {
            conn.execute(format!("insert into test(value) values ('v{i}')"))
                .unwrap();
        }
        conn.execute("commit").unwrap();
    }
}

fn count_test_table(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("select count(*) from test").unwrap();
    let mut count: i64 = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    count
}

fn run_checkpoint_until_done(pager: &turso_core::Pager, mode: CheckpointMode) -> CheckpointResult {
    // Use pager.checkpoint() instead of wal.checkpoint() directly because
    // WAL truncation (for TRUNCATE mode) now happens in pager's TruncateWalFile phase.
    pager
        .io
        .block(|| pager.checkpoint(mode, SyncMode::Full, true))
        .unwrap()
}

fn check_read_lock_slot(conn: &Arc<Connection>, _expected_slot: usize) -> bool {
    let pager = conn.pager.load();
    let _wal = pager.wal().unwrap();
    #[cfg(debug_assertions)]
    {
        let wal_any = _wal.as_any();
        if let Some(wal_file) = wal_any.downcast_ref::<turso_core::WalFile>() {
            return wal_file
                .max_frame_read_lock_index
                .load(std::sync::atomic::Ordering::Acquire)
                == _expected_slot;
        }
    }

    false
}

/// A reader of a fully backfilled WAL reads the database file alone, under read mark 0, so a
/// writer of another connection restarts the WAL beside it, as beside a reader in another
/// process (`subprocess_db_file_reader_allows_wal_restart_but_stops_backfill`). The reader
/// keeps its snapshot, and no frame of the restarted WAL reaches the database file while it
/// reads.
#[test]
fn test_db_file_reader_of_another_connection_allows_wal_restart_but_stops_backfill() {
    let (db, _path) = get_database();
    let writer = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&writer, 10, 5);
    let passive = CheckpointMode::Passive {
        upper_bound_inclusive: None,
    };
    let backfilled = run_checkpoint_until_done(&writer.pager.load(), passive);
    assert!(backfilled.everything_backfilled());

    let reader = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    reader.execute("begin").unwrap();
    assert_eq!(count_test_table(&reader), 50);
    assert!(
        check_read_lock_slot(&reader, 0),
        "the reader reads the database file"
    );

    writer.execute("update test set value = 'new'").unwrap();
    let max_frame = writer.pager.load().wal().unwrap().get_max_frame_in_wal();
    assert!(
        max_frame < backfilled.wal_max_frame,
        "a database file reader of another connection must not stop the WAL restart, \
         but the write appended at frame {max_frame} after frame {}",
        backfilled.wal_max_frame
    );
    let pager = writer.pager.load();
    let blocked = pager
        .io
        .block(|| pager.checkpoint(passive, SyncMode::Full, true));
    assert!(
        matches!(blocked, Err(LimboError::Busy)),
        "no frame of the restarted WAL may reach the database file while the reader reads: \
         {blocked:?}"
    );
    let mut stmt = reader
        .prepare("select count(*) from test where value = 'new'")
        .unwrap();
    let mut updated: i64 = -1;
    stmt.run_with_row_callback(|row| {
        updated = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    assert_eq!(updated, 0, "the reader's snapshot must not see the update");
    drop(stmt);
    reader.execute("commit").unwrap();

    let checkpoint = run_checkpoint_until_done(&writer.pager.load(), passive);
    assert!(checkpoint.everything_backfilled());
    let mut stmt = writer
        .prepare("select count(*) from test where value = 'new'")
        .unwrap();
    stmt.run_with_row_callback(|row| {
        updated = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    assert_eq!(updated, 50);
}

/// A commit's automatic checkpoint waits while another connection holds a read snapshot: the
/// commit still succeeds, however far the WAL grows past the automatic checkpoint's threshold,
/// and the frames the reader could still see stay in the WAL until it ends. WorkReef Cognition
/// relies on this (its writes must not fail because a recall is reading).
#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn an_automatic_checkpoint_waits_for_a_reader_and_never_fails_the_commit(
    tmp_db: TempDatabase,
) -> Result<()> {
    maybe_setup_tracing();
    let writer = tmp_db.connect_limbo();
    writer.execute("CREATE TABLE t(x)")?;
    let reader = tmp_db.connect_limbo();
    reader.execute("BEGIN")?;
    assert_eq!(
        execute_and_get_ints(&reader, "SELECT count(*) FROM t")?,
        vec![0]
    );

    // Far past the default automatic checkpoint threshold (1,000 frames): each insert is a
    // commit, and none may fail while the reader holds its snapshot.
    for _ in 0..700 {
        writer.execute("INSERT INTO t VALUES (randomblob(4096))")?;
    }
    let held = execute_and_get_ints(&writer, "PRAGMA wal_checkpoint(PASSIVE)")?;
    assert!(
        held[1] > 1000,
        "the WAL must have passed the automatic checkpoint's threshold: {held:?}"
    );
    assert!(
        held[2] < held[1],
        "frames after the reader's snapshot cannot be backfilled while it is open: {held:?}"
    );

    reader.execute("COMMIT")?;
    writer.execute("INSERT INTO t VALUES (randomblob(4096))")?;
    let after = execute_and_get_ints(&writer, "PRAGMA wal_checkpoint(PASSIVE)")?;
    assert_eq!(after[0], 0, "not busy once the reader has ended: {after:?}");
    assert_eq!(
        after[1], after[2],
        "every frame is backfilled once the reader has ended: {after:?}"
    );
    Ok(())
}

/// An explicit `PRAGMA wal_checkpoint(TRUNCATE)` while another connection holds a read snapshot is
/// refused (busy, "database is locked") and leaves the WAL as it was; once the reader ends it
/// succeeds. The forge's purge and WorkReef Cognition's erasure both depend on this: a truncate
/// must never pretend to have emptied a WAL a reader still needs.
#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn a_truncate_checkpoint_is_refused_while_a_reader_holds_a_snapshot(
    tmp_db: TempDatabase,
) -> Result<()> {
    maybe_setup_tracing();
    let writer = tmp_db.connect_limbo();
    writer.execute("CREATE TABLE t(x)")?;
    writer.execute("INSERT INTO t VALUES (1)")?;
    let reader = tmp_db.connect_limbo();
    reader.execute("BEGIN")?;
    assert_eq!(execute_and_get_ints(&reader, "SELECT x FROM t")?, vec![1]);
    writer.execute("INSERT INTO t VALUES (2)")?;

    // Refused is an error (busy, "database is locked") or a row whose busy column is not 0
    // (1, or NULL when no frames were counted).
    match first_column_text(&writer, "PRAGMA wal_checkpoint(TRUNCATE)") {
        Err(LimboError::Busy) => {}
        Ok(busy) => assert!(
            busy != "0",
            "a truncate beside a reader must be refused, but it reported not busy"
        ),
        Err(other) => panic!("a truncate beside a reader must be busy, not {other:?}"),
    }
    assert_eq!(
        execute_and_get_ints(&reader, "SELECT x FROM t")?,
        vec![1],
        "the reader's snapshot is untouched"
    );

    reader.execute("COMMIT")?;
    let done = execute_and_get_ints(&writer, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(done[0], 0, "the truncate succeeds once the reader ends: {done:?}");
    Ok(())
}

/// The first column of the first row `sql` returns, as text (`""` for NULL).
fn first_column_text(conn: &Arc<Connection>, sql: &str) -> Result<String> {
    let mut stmt = conn.prepare(sql)?;
    let mut first = None;
    stmt.run_with_row_callback(|row| {
        if first.is_none() {
            first = row.get_values().next().map(|value| format!("{value}"));
        }
        Ok(())
    })?;
    Ok(first.unwrap_or_default())
}

/// The frames in a WAL file of 512-byte pages: its size less the 32-byte header, over a
/// frame's 24-byte header and a page.
fn frames_in_wal_file(db_path: &std::path::Path) -> u64 {
    let wal = std::fs::metadata(format!("{}-wal", db_path.display()))
        .map(|meta| meta.len())
        .unwrap_or(0);
    wal.saturating_sub(32) / (512 + 24)
}

/// Copies the database and its WAL as they are on disk now, as a crash would leave them.
fn copy_database_files(db_path: &std::path::Path, to: &std::path::Path) {
    std::fs::copy(db_path, to).unwrap();
    let wal = format!("{}-wal", db_path.display());
    if std::path::Path::new(&wal).exists() {
        std::fs::copy(&wal, format!("{}-wal", to.display())).unwrap();
    }
}

/// SQLite reading a copy of the files: the oracle for the WAL's checksum chain, which it
/// verifies frame by frame and stops at the first frame that fails.
fn sqlite_counts(db_path: &std::path::Path) -> (String, i64) {
    let sqlite = rusqlite::Connection::open(db_path).unwrap();
    let integrity: String = sqlite
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    let rows: i64 = sqlite
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    (integrity, rows)
}

const SPILLING_ROWS: u32 = 3000;

fn spilling_table_and_cache(conn: &Arc<Connection>) -> Result<()> {
    conn.execute("PRAGMA page_size=512;")?;
    conn.execute("PRAGMA journal_mode=WAL;")?;
    conn.execute("PRAGMA wal_autocheckpoint=0;")?;
    conn.execute("PRAGMA cache_size=20;")?;
    conn.execute(
        "CREATE TABLE t(k INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d INTEGER);",
    )?;
    for column in ["a", "b", "c", "d"] {
        conn.execute(format!("CREATE INDEX t_{column} ON t({column});"))?;
    }
    Ok(())
}

fn spilling_fill(conn: &Arc<Connection>) -> Result<()> {
    conn.execute(format!(
        "INSERT INTO t(a, b, c, d)
         SELECT random(), random(), random(), random()
         FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {SPILLING_ROWS})
               SELECT x FROM c);"
    ))?;
    Ok(())
}

/// A transaction whose dirty pages do not fit the cache writes each page to the WAL once,
/// as SQLite does, and not once for every time the cache let it go.
#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_a_spilling_transaction_writes_each_page_to_the_wal_once(
    tmp_db: TempDatabase,
) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();
    spilling_table_and_cache(&conn)?;
    let before = frames_in_wal_file(&tmp_db.path);

    conn.execute("BEGIN IMMEDIATE;")?;
    spilling_fill(&conn)?;
    conn.execute("COMMIT;")?;

    let written = frames_in_wal_file(&tmp_db.path) - before;
    let pages = execute_and_get_ints(&conn, "PRAGMA page_count;")?[0] as u64;
    assert!(
        pages > 100,
        "the test must overflow a 20-page cache many times over: {pages} pages"
    );
    assert!(
        written <= pages + 2,
        "the transaction wrote {written} frames for a database of {pages} pages"
    );
    assert_eq!(
        execute_and_get_strings(&conn, "PRAGMA integrity_check;")?,
        vec!["ok"]
    );
    assert_eq!(
        execute_and_get_ints(&conn, "SELECT count(*) FROM t;")?,
        vec![SPILLING_ROWS as i64]
    );
    Ok(())
}

/// What the files hold at the moment of the commit is a WAL SQLite accepts whole: the
/// checksum chain, which the commit rewrote after the pages written over their frames, holds
/// to the last frame, so SQLite sees every row.
#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_sqlite_reads_the_wal_of_a_spilling_commit(tmp_db: TempDatabase) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();
    spilling_table_and_cache(&conn)?;
    conn.execute("BEGIN IMMEDIATE;")?;
    spilling_fill(&conn)?;
    conn.execute("COMMIT;")?;

    let image = tmp_db.path.with_file_name("after-commit.db");
    copy_database_files(&tmp_db.path, &image);
    let (integrity, rows) = sqlite_counts(&image);
    assert_eq!(integrity, "ok");
    assert_eq!(
        rows, SPILLING_ROWS as i64,
        "SQLite must accept every frame of the commit"
    );
    Ok(())
}

/// A crash in the middle of a spilling transaction leaves frames written over, with stale
/// checksums, above the last commit: the next open and SQLite both see the database as it
/// was before the transaction.
#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_a_crash_image_of_a_spilling_transaction_is_the_database_before_it(
    tmp_db: TempDatabase,
) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();
    spilling_table_and_cache(&conn)?;
    conn.execute("INSERT INTO t(a, b, c, d) VALUES (1, 2, 3, 4);")?;
    let committed_frames = frames_in_wal_file(&tmp_db.path);

    conn.execute("BEGIN IMMEDIATE;")?;
    spilling_fill(&conn)?;
    // Update the rows again so pages the cache already gave up are written over.
    conn.execute("UPDATE t SET a = a + 1, b = b + 1, c = c + 1, d = d + 1;")?;
    for completion in conn.cacheflush()? {
        tmp_db.io.wait_for_completion(completion)?;
    }
    assert!(
        frames_in_wal_file(&tmp_db.path) > committed_frames,
        "the transaction must have spilled frames by now"
    );

    let image = tmp_db.path.with_file_name("crash-image.db");
    copy_database_files(&tmp_db.path, &image);
    let sqlite_image = tmp_db.path.with_file_name("crash-image-sqlite.db");
    copy_database_files(&tmp_db.path, &sqlite_image);
    conn.execute("ROLLBACK;")?;

    let reopened = TempDatabase::new_with_existent(&image);
    let reopened_conn = reopened.connect_limbo();
    assert_eq!(
        execute_and_get_ints(&reopened_conn, "SELECT count(*) FROM t;")?,
        vec![1],
        "an uncommitted transaction is not there after the crash"
    );
    assert_eq!(
        execute_and_get_strings(&reopened_conn, "PRAGMA integrity_check;")?,
        vec!["ok"]
    );
    let (integrity, rows) = sqlite_counts(&sqlite_image);
    assert_eq!(integrity, "ok");
    assert_eq!(rows, 1);
    Ok(())
}
