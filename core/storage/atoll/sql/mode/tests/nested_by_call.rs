//! ONE-TXN 4.1 (rows 69 and 70): a helper statement marks its connection nested only while one of
//! its own calls runs. Between its calls its parent may return to the caller with the helper kept,
//! a pragma's table-valued form at a row or a schema parse waiting for a read, and a statement of
//! the connection the caller steps then is a top-level statement: refused, begun and ended as one.
//! Each test records what it sees, drops every connection, and asserts after, in a child of its
//! own (`testing::cleanly`).

use super::*;

/// Steps `stmt` until it returns something other than IO, a yield or a row, waiting for the IO it
/// stops on.
fn stepped(stmt: &mut crate::Statement) -> String {
    loop {
        match stmt.step() {
            Ok(crate::StepResult::IO) => {
                if let Err(e) = stmt.wait_io() {
                    return format!("io error: {e}");
                }
            }
            Ok(crate::StepResult::Row | crate::StepResult::Yield) => {}
            Ok(other) => return format!("{other:?}"),
            Err(e) => return format!("error: {e}"),
        }
    }
}

fn ran(conn: &Arc<Connection>, sql: &str) -> String {
    match conn
        .prepare(sql)
        .and_then(|mut stmt| stmt.run_collect_rows())
    {
        Ok(rows) => format!("{rows:?}"),
        Err(e) => format!("error: {e}"),
    }
}

/// Evicts every page nothing holds or keeps, the pool's and the metadata cache's, so the next
/// read of each is cold, once every request has retired: a page still being written back is not
/// yet clean, so it would stay resident. Whether every request retired within 20 seconds.
fn chill(db: &Database) -> bool {
    let atoll = db.atoll.as_ref().unwrap().atoll();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while atoll.outstanding_requests() != 0 {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    let atoll = db.atoll.as_ref().unwrap().atoll();
    atoll.pool().evict_all_clean();
    atoll.store().meta().evict_all();
    true
}

/// One call of `stmt` in a cooperative frame of the default budget, with the database's IO thread
/// held where it would mark itself idle until the call returns: a cold read the call queues cannot
/// finish before the statement parks on it, so the call stops there whatever the machine's load.
/// `None` if the IO thread was not held within a minute.
fn framed_with_the_io_thread_held(
    db: &Database,
    stmt: &mut crate::Statement,
) -> Option<crate::Result<crate::StepResult>> {
    let atoll = db.atoll.as_ref().unwrap().atoll();
    let hold = crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
        atoll.requests_for_test(),
    )?;
    let step = {
        let _frame = crate::storage::atoll::cooperative::Frame::with_budget(
            crate::storage::atoll::cooperative::Frame::DEFAULT_BUDGET,
        );
        stmt.step_in_frame(std::task::Waker::noop())
    };
    drop(hold);
    Some(step)
}

/// Row 70's shape: a pragma's table-valued form stands at a row, its helper kept. The connection
/// is not nested meanwhile, and a write of the connection beside it is a top-level statement: it
/// takes the write gate, commits at its halt, and another connection reads it.
#[test]
fn a_write_beside_a_pragma_scan_standing_at_a_row_is_committed() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_write_beside_a_pragma_scan_standing_at_a_row_is_committed"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = open(&io, "/db/nested-beside-pragma-scan");
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            let set = ran(&conn, "CREATE TABLE t(a INTEGER, b TEXT)");
            let mut scan = conn
                .prepare("SELECT name FROM pragma_table_info('t')")
                .unwrap();
            let first = scan
                .step()
                .map(|r| format!("{r:?}"))
                .map_err(|e| e.to_string());
            let nested = conn.is_nested_stmt();
            let autocommit = conn.get_auto_commit();
            let beside = ran(&conn, "INSERT INTO t VALUES (2, 'beside a pragma scan')");
            let rest = stepped(&mut scan);
            drop(scan);
            let seen = ran(&other, "SELECT a FROM t");
            let checked = ran(&other, "PRAGMA integrity_check");
            drop((conn, other, db));
            assert_eq!(set, "[]");
            assert_eq!(first, Ok("Row".to_string()));
            assert!(
                !nested,
                "a pragma scan standing at a row leaves its connection nested"
            );
            assert!(autocommit);
            assert_eq!(beside, "[]");
            assert_eq!(rest, "Done");
            assert_eq!(
                seen, "[[Numeric(Integer(2))]]",
                "the write beside the pragma scan was not committed"
            );
            assert_eq!(checked, "[[Text(Text { value: \"ok\", subtype: Text })]]");
        },
    );
}

/// Row 69's shape, in cooperative frames with every page made cold before the first step and
/// the IO thread held through each call: a CREATE TABLE stops at a cold read inside its schema
/// parse, the parse's helper kept. At every stop inside the parse, a wait for a read or a yield
/// (a look-again yield made inside the parse reaches the caller as a yield), its connection is
/// not nested and is in autocommit, and a second write of the connection is refused as one beside
/// an active writer. The CREATE TABLE then finishes outside the frames (its commit needs the
/// log's preparer, which no worker drives for a frame yet), and another connection reads the
/// table.
#[test]
fn a_schema_parse_waiting_for_a_read_leaves_its_connection_top_level() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_schema_parse_waiting_for_a_read_leaves_its_connection_top_level"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = crate::storage::atoll::requests::opened_async(|| {
                open(&io, "/db/nested-schema-parse-waiting")
            });
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            // Enough tables that sqlite_schema spans many pages, so the parse's scan meets cold ones.
            let filler = "x".repeat(200);
            let set: Vec<String> = (0..120)
                .map(|i| {
                    ran(
                        &conn,
                        &format!("CREATE TABLE f{i}(a INTEGER, note TEXT DEFAULT '{filler}')"),
                    )
                })
                .filter(|r| r != "[]")
                .collect();
            let mut x = conn
                .prepare("CREATE TABLE c(k INTEGER PRIMARY KEY, w TEXT)")
                .unwrap();
            let (mut stops, mut inside, mut waits) = (0, 0, 0);
            let mut at_stops = Vec::new();
            let mut beside = None;
            let mut framed = String::new();
            // Every page cold once, before the first step: the parse's scan of sqlite_schema then
            // meets cold leaves. Evicting again between steps would drop the page each wait loaded.
            let chilled = chill(&db);
            loop {
                let Some(step) = framed_with_the_io_thread_held(&db, &mut x) else {
                    framed = "the IO thread was not held".to_string();
                    break;
                };
                match step {
                    Ok(stop @ (crate::StepResult::IO | crate::StepResult::Yield)) => {
                        stops += 1;
                        if stops > 10_000 {
                            framed = "no end after 10,000 stops".to_string();
                            break;
                        }
                        if x.parse_schema_kept_for_test() {
                            inside += 1;
                            waits += usize::from(matches!(stop, crate::StepResult::IO));
                            at_stops.push((conn.is_nested_stmt(), conn.get_auto_commit()));
                            if beside.is_none() {
                                beside = Some(ran(&conn, "INSERT INTO f0 VALUES (1, 'beside')"));
                            }
                        } else if inside > 0 {
                            break;
                        }
                        if !matches!(stop, crate::StepResult::IO) {
                            continue;
                        }
                        if let Err(e) = x.wait_io() {
                            framed = format!("io error: {e}");
                            break;
                        }
                    }
                    Ok(other) => {
                        framed = format!("{other:?}");
                        break;
                    }
                    Err(e) => {
                        framed = format!("error: {e}");
                        break;
                    }
                }
            }
            let finished = if framed.is_empty() {
                stepped(&mut x)
            } else {
                framed
            };
            drop(x);
            let after = (conn.is_nested_stmt(), conn.get_auto_commit());
            let seen = ran(
                &other,
                "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
            );
            let f0 = ran(&other, "SELECT count(*) FROM f0");
            let checked = ran(&other, "PRAGMA integrity_check");
            drop((conn, other, db));
            assert!(set.is_empty(), "{set:?}");
            assert!(chilled, "a request still outstanding before the eviction");
            assert!(
                inside > 0,
                "the CREATE TABLE never stopped inside its schema parse ({stops} stops, {finished})"
            );
            assert!(
                waits > 0,
                "the CREATE TABLE never waited for a read inside its schema parse ({inside} stops inside it)"
            );
            assert!(
                at_stops.iter().all(|s| *s == (false, true)),
                "a schema parse waiting for a read left its connection nested or out of autocommit: {at_stops:?}"
            );
            assert!(
                beside
                    .as_deref()
                    .is_some_and(|b| b.contains("cannot start a write statement")),
                "a write beside the parse was not refused as one beside an active writer: {beside:?}"
            );
            assert_eq!(finished, "Done");
            assert_eq!(after, (false, true));
            assert_eq!(seen, "[[Numeric(Integer(1))]]");
            assert_eq!(f0, "[[Numeric(Integer(0))]]");
            assert_eq!(checked, "[[Text(Text { value: \"ok\", subtype: Text })]]");
        },
    );
}

/// The same CREATE TABLE dropped at its first stop inside its schema parse, the IO thread held
/// through each call so the stop is made: its drop ends what it began, the parse's helper dropped
/// with it, and the database is as before it; the connection then creates the table and another
/// connection reads it.
#[test]
fn a_create_table_dropped_inside_its_schema_parse_leaves_the_database_as_before() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_create_table_dropped_inside_its_schema_parse_leaves_the_database_as_before"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = crate::storage::atoll::requests::opened_async(|| {
                open(&io, "/db/nested-schema-parse-dropped")
            });
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            let filler = "x".repeat(200);
            let set: Vec<String> = (0..120)
                .map(|i| {
                    ran(
                        &conn,
                        &format!("CREATE TABLE f{i}(a INTEGER, note TEXT DEFAULT '{filler}')"),
                    )
                })
                .filter(|r| r != "[]")
                .collect();
            let mut x = conn
                .prepare("CREATE TABLE c(k INTEGER PRIMARY KEY, w TEXT)")
                .unwrap();
            let mut stops = 0;
            let mut reached = false;
            let mut framed = String::new();
            let chilled = chill(&db);
            while !reached && stops <= 10_000 {
                let Some(step) = framed_with_the_io_thread_held(&db, &mut x) else {
                    framed = "the IO thread was not held".to_string();
                    break;
                };
                match step {
                    Ok(stop @ (crate::StepResult::IO | crate::StepResult::Yield)) => {
                        stops += 1;
                        reached = x.parse_schema_kept_for_test();
                        if !matches!(stop, crate::StepResult::IO) {
                            continue;
                        }
                        if let Err(e) = x.wait_io() {
                            framed = format!("io error: {e}");
                            break;
                        }
                    }
                    Ok(other) => {
                        framed = format!("{other:?}");
                        break;
                    }
                    Err(e) => {
                        framed = format!("error: {e}");
                        break;
                    }
                }
            }
            drop(x);
            let after = (conn.is_nested_stmt(), conn.get_auto_commit());
            let gone = ran(
                &other,
                "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
            );
            let again = ran(&conn, "CREATE TABLE c(k INTEGER PRIMARY KEY, w TEXT)");
            let seen = ran(
                &other,
                "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
            );
            let checked = ran(&other, "PRAGMA integrity_check");
            drop((conn, other, db));
            assert!(set.is_empty(), "{set:?}");
            assert!(chilled, "a request still outstanding before the eviction");
            assert!(
                reached,
                "the CREATE TABLE never stopped inside its schema parse ({stops} stops, {framed})"
            );
            assert_eq!(after, (false, true));
            assert_eq!(gone, "[[Numeric(Integer(0))]]");
            assert_eq!(again, "[]");
            assert_eq!(seen, "[[Numeric(Integer(1))]]");
            assert_eq!(checked, "[[Text(Text { value: \"ok\", subtype: Text })]]");
        },
    );
}
