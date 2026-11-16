//! ONE-TXN rule 1 (4.2a, row 67): a connection has one top-level writer, registered at its
//! Transaction instruction's first entry, before it can return to its caller, and let go at its
//! last return: Done, an error, a reset, a drop, an interrupt, and a Busy that reaches the caller
//! before its write began (rule 2: it has not begun). A second writer of the connection is
//! refused meanwhile, with "cannot start a write statement". Each test records what it sees, drops
//! every connection, and asserts after, in a child of its own (`testing::cleanly`).

use super::*;

const REFUSED: &str = "cannot start a write statement";

/// Steps `stmt` until it returns something other than IO or a row, driving the IO it waits on;
/// with a `bound`, it sleeps a millisecond between steps so a wait with a busy timeout can pass
/// its bound.
fn stepped(stmt: &mut crate::Statement, bound: Option<std::time::Duration>) -> String {
    let start = std::time::Instant::now();
    loop {
        match stmt.step() {
            Ok(crate::StepResult::IO) => {
                if let Some(bound) = bound {
                    if start.elapsed() < bound * 4 {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
                if let Err(e) = stmt._io().step() {
                    return format!("io error: {e}");
                }
            }
            Ok(crate::StepResult::Row) => {}
            Ok(other) => return format!("{other:?}"),
            Err(e) => return format!("error: {e}"),
        }
    }
}

fn once(stmt: &mut crate::Statement) -> String {
    match stmt.step() {
        Ok(r) => format!("{r:?}"),
        Err(e) => format!("error: {e}"),
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

/// A database with a table `t`, this connection and another, which holds the write gate shared
/// with an open write: a schema change of this connection parks at its exclusive take.
fn holding(path: &str) -> (Arc<Database>, Arc<Connection>, Arc<Connection>, String) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, path);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    let set = [
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, w TEXT)"),
        other.execute("BEGIN"),
        other.execute("INSERT INTO t VALUES (1000, 'w')"),
    ]
    .into_iter()
    .map(|r| r.map_err(|e| e.to_string()))
    .collect::<std::result::Result<Vec<_>, _>>();
    (db, conn, other, format!("{set:?}"))
}

/// What a connection shows of its transaction: whether Atoll has one open, and autocommit.
fn shows(conn: &Arc<Connection>) -> (bool, bool) {
    (conn.atoll().unwrap().is_open(), conn.get_auto_commit())
}

const CREATE_C: &str = "CREATE TABLE c(k INTEGER PRIMARY KEY, w TEXT)";

/// Evicts every page nothing holds or keeps, the pool's and the metadata cache's, so the next
/// read of each is cold, once every request has retired. Whether every request retired within 20
/// seconds.
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
    atoll.pool().evict_all_clean();
    atoll.store().meta().evict_all();
    true
}

/// One call of `stmt` in a cooperative frame of the default budget, with the database's IO thread
/// held where it would mark itself idle until the call returns: a cold read the call queues cannot
/// finish before the statement parks on it. `None` if the IO thread was not held within a minute.
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

/// What a writer shows at a stop: whether the connection has a registered writer, whether the
/// statement's write has begun, and whether the connection's transaction state is open.
fn at_stop(conn: &Arc<Connection>, stmt: &crate::Statement) -> (bool, bool, bool) {
    (
        conn.n_active_writes
            .load(std::sync::atomic::Ordering::SeqCst)
            == 1,
        stmt.write_begun_for_test(),
        !matches!(conn.get_tx_state(), crate::TransactionState::None),
    )
}

/// Row 67's schedule: X, a schema change, stands at its Transaction instruction's IO return,
/// parked at its exclusive take, with no busy timeout bound to end the wait. A second writer of
/// its connection is refused at once, and X, stepped after the other connection commits, ends
/// Done and committed.
#[test]
fn a_writer_parked_at_its_take_refuses_a_second_writer_of_its_connection() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_parked_at_its_take_refuses_a_second_writer_of_its_connection"
        ),
        || {
            let (db, conn, other, set) = holding("/db/one-writer-parked");
            conn.set_busy_timeout(std::time::Duration::from_secs(30));
            let mut x = conn.prepare(CREATE_C).unwrap();
            let parked = once(&mut x);
            let second = ran(&conn, "INSERT INTO t VALUES (5, 'second')");
            let committed = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = stepped(&mut x, None);
            drop(x);
            let after = shows(&conn);
            let third = ran(&conn, "INSERT INTO t VALUES (6, 'after')");
            let seen = [
                ran(
                    &other,
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
                ),
                ran(&other, "SELECT id FROM t WHERE id < 1000 ORDER BY id"),
            ];
            drop((conn, other, db));
            assert_eq!(set, "Ok([(), (), ()])");
            assert_eq!(parked, "IO", "the schema change did not park at its take");
            assert!(
                second.contains(REFUSED),
                "a second writer beside one parked at its take was not refused: {second}"
            );
            assert_eq!(committed, Ok(()));
            assert_eq!(finished, "Done");
            assert_eq!(after, (false, true));
            assert_eq!(third, "[]");
            assert_eq!(
                seen,
                [
                    "[[Numeric(Integer(1))]]".to_string(),
                    "[[Numeric(Integer(6))]]".to_string()
                ]
            );
        },
    );
}

/// The same inside BEGIN: the second writer is refused, and so is a COMMIT beside the writer
/// parked at its take, which would end the transaction under it. After X ends, the COMMIT
/// commits what X wrote.
#[test]
fn inside_begin_a_writer_parked_at_its_take_refuses_a_second_writer_and_a_commit() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::inside_begin_a_writer_parked_at_its_take_refuses_a_second_writer_and_a_commit"
        ),
        || {
            let (db, conn, other, set) = holding("/db/one-writer-parked-in-begin");
            conn.set_busy_timeout(std::time::Duration::from_secs(30));
            let begun = ran(&conn, "BEGIN");
            let mut x = conn.prepare(CREATE_C).unwrap();
            let parked = once(&mut x);
            let second = ran(&conn, "INSERT INTO t VALUES (5, 'second')");
            let commit_beside = ran(&conn, "COMMIT");
            let still = shows(&conn);
            let committed = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = stepped(&mut x, None);
            drop(x);
            let commit_after = ran(&conn, "COMMIT");
            let seen = [
                ran(
                    &other,
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
                ),
                ran(&other, "SELECT count(*) FROM t WHERE id < 1000"),
            ];
            drop((conn, other, db));
            assert_eq!(set, "Ok([(), (), ()])");
            assert_eq!(begun, "[]");
            assert_eq!(parked, "IO", "the schema change did not park at its take");
            assert!(
                second.contains(REFUSED),
                "inside BEGIN, a second writer beside one parked at its take was not refused: {second}"
            );
            assert!(
                commit_beside.contains("cannot commit transaction"),
                "a COMMIT beside a writer parked at its take was not refused: {commit_beside}"
            );
            assert_eq!(still, (true, false));
            assert_eq!(committed, Ok(()));
            assert_eq!(finished, "Done");
            assert_eq!(commit_after, "[]");
            assert_eq!(
                seen,
                [
                    "[[Numeric(Integer(1))]]".to_string(),
                    "[[Numeric(Integer(0))]]".to_string()
                ]
            );
        },
    );
}

/// Rule 2: a writer that came back Busy to its caller has not begun, and is not the
/// connection's writer any more. A row write of its connection then runs, takes the gate shared
/// beside the other connection's, and commits; the Busy statement, stepped again after the other
/// commits, registers again and ends Done and committed.
#[test]
fn a_writer_that_came_back_busy_lets_a_second_writer_of_its_connection_run() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_that_came_back_busy_lets_a_second_writer_of_its_connection_run"
        ),
        || {
            let (db, conn, other, set) = holding("/db/one-writer-busy-lets-go");
            let bound = std::time::Duration::from_millis(5);
            conn.set_busy_timeout(bound);
            let mut x = conn.prepare(CREATE_C).unwrap();
            let busy = stepped(&mut x, Some(bound));
            let run_after_busy = format!("{:?}", x.execution_state());
            let second = ran(&conn, "INSERT INTO t VALUES (5, 'second')");
            let after_second = shows(&conn);
            let committed = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = stepped(&mut x, None);
            drop(x);
            let after = shows(&conn);
            let seen = [
                ran(
                    &other,
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
                ),
                ran(&other, "SELECT id FROM t WHERE id < 1000"),
            ];
            drop((conn, other, db));
            assert_eq!(set, "Ok([(), (), ()])");
            assert_eq!(busy, "Busy");
            assert_eq!(
                run_after_busy, "Init",
                "a statement that came back Busy kept its run's state"
            );
            assert_eq!(
                second, "[]",
                "a writer that came back Busy stayed its connection's writer"
            );
            assert_eq!(after_second, (false, true));
            assert_eq!(committed, Ok(()));
            assert_eq!(finished, "Done");
            assert_eq!(after, (false, true));
            assert_eq!(
                seen,
                [
                    "[[Numeric(Integer(1))]]".to_string(),
                    "[[Numeric(Integer(5))]]".to_string()
                ]
            );
        },
    );
}

/// The first matrix's shape (#5955): X Busy; a SELECT of its connection, which reads in the
/// transaction X's begin left in the cell and ends it at its halt; the other commits; X stepped
/// again begins afresh, ends Done, and is committed: the other connection reads its table.
#[test]
fn a_writer_busy_then_a_select_then_stepped_again_is_done_and_committed() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_busy_then_a_select_then_stepped_again_is_done_and_committed"
        ),
        || {
            let (db, conn, other, set) = holding("/db/one-writer-busy-select-again");
            let bound = std::time::Duration::from_millis(5);
            conn.set_busy_timeout(bound);
            let mut x = conn.prepare(CREATE_C).unwrap();
            let busy = stepped(&mut x, Some(bound));
            let after_busy = shows(&conn);
            let read = ran(&conn, "SELECT count(*) FROM t");
            let after_read = shows(&conn);
            let committed = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = stepped(&mut x, None);
            drop(x);
            let after = shows(&conn);
            let seen = ran(
                &other,
                "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
            );
            let wrote = ran(&other, "INSERT INTO t VALUES (7, 'other')");
            let checked = ran(&conn, "PRAGMA integrity_check");
            drop((conn, other, db));
            assert_eq!(set, "Ok([(), (), ()])");
            assert_eq!(busy, "Busy");
            // Until each autocommit statement has a transaction of its own (E1b), the transaction
            // the Busy statement's begin opened stays in the cell with nobody in it.
            assert_eq!(after_busy, (true, true));
            assert_eq!(read, "[[Numeric(Integer(0))]]");
            assert_eq!(
                after_read,
                (false, true),
                "the SELECT did not end the transaction the Busy statement left"
            );
            assert_eq!(committed, Ok(()));
            assert_eq!(finished, "Done");
            assert_eq!(after, (false, true));
            assert_eq!(
                seen, "[[Numeric(Integer(1))]]",
                "the statement stepped again after its Busy was not committed"
            );
            assert_eq!(wrote, "[]", "the write gate was left held");
            assert_eq!(checked, "[[Text(Text { value: \"ok\", subtype: Text })]]");
        },
    );
}

/// The writer's other last returns from the same stop, parked at its take: a reset and a drop
/// each let the registration go, and a second writer of the connection then runs and commits.
#[test]
fn a_writer_reset_or_dropped_at_its_take_is_the_writer_no_more() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_reset_or_dropped_at_its_take_is_the_writer_no_more"
        ),
        || {
            let mut seen = Vec::new();
            for ending in ["reset", "drop"] {
                let (db, conn, other, set) = holding(&format!("/db/one-writer-{ending}"));
                conn.set_busy_timeout(std::time::Duration::from_secs(30));
                let mut x = conn.prepare(CREATE_C).unwrap();
                let parked = once(&mut x);
                let refused = ran(&conn, "INSERT INTO t VALUES (4, 'beside')");
                let ended = match ending {
                    "reset" => format!("{:?}", x.reset().map_err(|e| e.to_string())),
                    _ => String::new(),
                };
                drop(x);
                let second = ran(&conn, "INSERT INTO t VALUES (5, 'second')");
                let after = shows(&conn);
                let committed = other.execute("COMMIT").map_err(|e| e.to_string());
                let rows = ran(&other, "SELECT id FROM t WHERE id < 1000");
                let made = ran(
                    &other,
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
                );
                drop((conn, other, db));
                seen.push(format!(
                    "{ending}: {set} parked={parked} refused={} ended={ended} second={second} after={after:?} committed={committed:?} rows={rows} made={made}",
                    refused.contains(REFUSED)
                ));
            }
            let want = |ending: &str, ended: &str| {
                format!(
                    "{ending}: Ok([(), (), ()]) parked=IO refused=true ended={ended} second=[] after=(false, true) committed=Ok(()) rows=[[Numeric(Integer(5))]] made=[[Numeric(Integer(0))]]"
                )
            };
            assert_eq!(
                seen,
                [want("reset", "Ok(())"), want("drop", "")],
                "a writer ended at its take stayed its connection's writer"
            );
        },
    );
}

/// Inside BEGIN, a writer reset or dropped at its take is registered and has written nothing, so
/// the transaction is whole: it still reads its earlier write, and COMMIT commits it. A writer
/// whose write had begun with no statement savepoint poisons the transaction when it is reset;
/// read as "registered", that test poisoned this one too (mutant `poison-reads-registered`).
#[test]
fn inside_begin_a_writer_reset_or_dropped_at_its_take_leaves_the_transaction_whole() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::inside_begin_a_writer_reset_or_dropped_at_its_take_leaves_the_transaction_whole"
        ),
        || {
            let mut seen = Vec::new();
            for ending in ["reset", "drop"] {
                let (db, conn, other, set) = holding(&format!("/db/one-writer-begin-{ending}"));
                conn.set_busy_timeout(std::time::Duration::from_secs(30));
                let begun = [
                    ran(&conn, "BEGIN"),
                    ran(&conn, "INSERT INTO t VALUES (7, 'mine')"),
                ];
                let mut x = conn.prepare(CREATE_C).unwrap();
                let parked = once(&mut x);
                let ended = match ending {
                    "reset" => format!("{:?}", x.reset().map_err(|e| e.to_string())),
                    _ => String::new(),
                };
                drop(x);
                let still = shows(&conn);
                let own = ran(&conn, "SELECT count(*) FROM t WHERE id = 7");
                let commit = ran(&conn, "COMMIT");
                let after = shows(&conn);
                let committed = other.execute("COMMIT").map_err(|e| e.to_string());
                let rows = ran(&other, "SELECT id FROM t WHERE id < 1000");
                drop((conn, other, db));
                seen.push(format!(
                    "{ending}: {set} begun={begun:?} parked={parked} ended={ended} still={still:?} own={own} commit={commit} after={after:?} committed={committed:?} rows={rows}"
                ));
            }
            let want = |ending: &str, ended: &str| {
                format!(
                    "{ending}: Ok([(), (), ()]) begun=[\"[]\", \"[]\"] parked=IO ended={ended} still=(true, false) own=[[Numeric(Integer(1))]] commit=[] after=(false, true) committed=Ok(()) rows=[[Numeric(Integer(7))]]"
                )
            };
            assert_eq!(
                seen,
                [want("reset", "Ok(())"), want("drop", "")],
                "inside BEGIN, a writer ended at its take poisoned the transaction or lost its write"
            );
        },
    );
}

/// The Interrupt return: a writer standing at a row it returns, its write done, is interrupted;
/// its next step returns Interrupt and undoes its write, and it is the writer no more: a second
/// writer of the connection runs and commits.
#[test]
fn a_writer_interrupted_at_a_row_is_the_writer_no_more() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_interrupted_at_a_row_is_the_writer_no_more"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = open(&io, "/db/one-writer-interrupted");
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            let mut set = vec![
                ran(&conn, "CREATE TABLE src(id INTEGER PRIMARY KEY, w TEXT)"),
                ran(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, w TEXT)"),
            ];
            for i in 0..300 {
                set.push(ran(&conn, &format!("INSERT INTO src VALUES ({i}, 'w')")));
            }
            set.retain(|r| r != "[]");
            let mut x = conn
                .prepare("INSERT INTO t SELECT id, w FROM src RETURNING id")
                .unwrap();
            let first = once(&mut x);
            let refused = ran(&conn, "INSERT INTO t VALUES (5000, 'beside')");
            conn.interrupt();
            let mut ended = String::new();
            for _ in 0..400 {
                ended = once(&mut x);
                if ended != "Row" {
                    break;
                }
            }
            drop(x);
            let second = ran(&conn, "INSERT INTO t VALUES (5001, 'second')");
            let after = shows(&conn);
            let seen = ran(&other, "SELECT id FROM t");
            drop((conn, other, db));
            assert!(set.is_empty(), "{set:?}");
            assert_eq!(first, "Row");
            assert!(
                refused.contains(REFUSED),
                "a second writer beside one standing at a row was not refused: {refused}"
            );
            assert_eq!(ended, "Interrupt");
            assert_eq!(
                second, "[]",
                "an interrupted writer stayed its connection's writer"
            );
            assert_eq!(after, (false, true));
            assert_eq!(seen, "[[Numeric(Integer(5001))]]");
        },
    );
}

/// A return that is not one to the caller: a prepared write meets a schema change another
/// connection committed at its Transaction instruction, after its admission, and is prepared
/// again and run again inside the same step. It is let go and admitted again, never refused by
/// its own registration; it ends Done and its row is written once.
#[test]
fn a_prepared_write_prepared_again_for_a_schema_change_is_admitted_again() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_prepared_write_prepared_again_for_a_schema_change_is_admitted_again"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = open(&io, "/db/one-writer-reprepared");
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            let set = ran(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, w TEXT)");
            let mut x = conn.prepare("INSERT INTO t VALUES (1, 'x')").unwrap();
            let changed = ran(&other, "CREATE TABLE u(a INTEGER)");
            let reprepares_before = x.metrics().reprepares;
            let finished = stepped(&mut x, None);
            let reprepared = x.metrics().reprepares - reprepares_before;
            drop(x);
            let writers = conn
                .n_active_writes
                .load(std::sync::atomic::Ordering::SeqCst);
            let second = ran(&conn, "INSERT INTO t VALUES (2, 'second')");
            let seen = ran(&other, "SELECT id FROM t ORDER BY id");
            drop((conn, other, db));
            assert_eq!(set, "[]");
            assert_eq!(changed, "[]");
            assert_eq!(
                finished, "Done",
                "the write prepared again did not end Done"
            );
            assert!(reprepared > 0, "the write was not prepared again");
            assert_eq!(writers, 0, "the writer's registration was not let go");
            assert_eq!(second, "[]");
            assert_eq!(seen, "[[Numeric(Integer(1))], [Numeric(Integer(2))]]");
        },
    );
}

/// The other such return: a sole autocommit write that meets a write-write conflict is run
/// again from its first instruction with a new snapshot. It is the writer through the restart,
/// ends Done with its row written once, and is the writer no more.
#[test]
fn a_write_restarted_after_a_conflict_ends_done_and_is_the_writer_no_more() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_write_restarted_after_a_conflict_ends_done_and_is_the_writer_no_more"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = open(&io, "/db/one-writer-restarted");
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            let set = [
                ran(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, w TEXT)"),
                ran(&conn, "INSERT INTO t VALUES (1, 'first')"),
                ran(&other, "BEGIN"),
                ran(&other, "UPDATE t SET w = 'other' WHERE id = 1"),
            ];
            conn.set_busy_timeout(std::time::Duration::from_secs(30));
            let mut x = conn
                .prepare("UPDATE t SET w = 'mine' WHERE id = 1")
                .unwrap();
            let waiting = once(&mut x);
            let beside = ran(&conn, "INSERT INTO t VALUES (5, 'beside')");
            let committed = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = stepped(&mut x, None);
            drop(x);
            let writers = conn
                .n_active_writes
                .load(std::sync::atomic::Ordering::SeqCst);
            let second = ran(&conn, "INSERT INTO t VALUES (2, 'second')");
            let seen = ran(&other, "SELECT id, w FROM t ORDER BY id");
            drop((conn, other, db));
            assert_eq!(set, ["[]", "[]", "[]", "[]"]);
            assert_eq!(waiting, "IO", "the write did not wait for the other's row");
            assert!(
                beside.contains(REFUSED),
                "a second writer beside one waiting for a row was not refused: {beside}"
            );
            assert_eq!(committed, Ok(()));
            assert_eq!(finished, "Done");
            assert_eq!(writers, 0, "the writer's registration was not let go");
            assert_eq!(second, "[]");
            assert_eq!(
                seen,
                "[[Numeric(Integer(1)), Text(Text { value: \"mine\", subtype: Text })], [Numeric(Integer(2)), Text(Text { value: \"second\", subtype: Text })]]"
            );
        },
    );
}

/// What 4.2a brings a caller (#5962): inside BEGIN, a COMMIT or a ROLLBACK beside a writer parked
/// at its take is refused, since the writer is registered from its first entry, and the
/// transaction is whole after each refusal. Once the writer has come back Busy it has not begun:
/// the COMMIT then commits what the transaction wrote before.
#[test]
fn inside_begin_a_commit_or_rollback_beside_a_parked_writer_leaves_the_transaction_whole() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::inside_begin_a_commit_or_rollback_beside_a_parked_writer_leaves_the_transaction_whole"
        ),
        || {
            let (db, conn, other, set) = holding("/db/one-writer-commit-beside");
            let bound = std::time::Duration::from_millis(40);
            conn.set_busy_timeout(bound);
            let begun = [
                ran(&conn, "BEGIN"),
                ran(&conn, "INSERT INTO t VALUES (7, 'mine')"),
            ];
            let mut x = conn.prepare(CREATE_C).unwrap();
            let parked = once(&mut x);
            let commit_beside = ran(&conn, "COMMIT");
            let rollback_beside = ran(&conn, "ROLLBACK");
            let savepoint_beside = ran(&conn, "SAVEPOINT s");
            let still = shows(&conn);
            let own = ran(&conn, "SELECT count(*) FROM t WHERE id = 7");
            let busy = stepped(&mut x, Some(bound));
            let commit_after_busy = ran(&conn, "COMMIT");
            drop(x);
            let after = shows(&conn);
            let committed = other.execute("COMMIT").map_err(|e| e.to_string());
            let seen = [
                ran(&other, "SELECT id FROM t WHERE id < 1000"),
                ran(&other, "SELECT count(*) FROM sqlite_schema WHERE name = 'c'"),
            ];
            drop((conn, other, db));
            assert_eq!(set, "Ok([(), (), ()])");
            assert_eq!(begun, ["[]", "[]"]);
            assert_eq!(parked, "IO", "the schema change did not park at its take");
            assert!(
                commit_beside.contains("cannot commit transaction"),
                "a COMMIT beside a writer parked at its take was not refused: {commit_beside}"
            );
            assert!(
                rollback_beside.contains("cannot rollback transaction"),
                "a ROLLBACK beside a writer parked at its take was not refused: {rollback_beside}"
            );
            assert!(
                savepoint_beside.contains("cannot open savepoint"),
                "a SAVEPOINT beside a writer parked at its take was not refused: {savepoint_beside}"
            );
            assert_eq!(still, (true, false), "a refused COMMIT or ROLLBACK ended the transaction");
            assert_eq!(
                own, "[[Numeric(Integer(1))]]",
                "the transaction lost its write at a refused COMMIT or ROLLBACK"
            );
            assert_eq!(busy, "Busy");
            assert_eq!(
                commit_after_busy, "[]",
                "a COMMIT after the writer came back Busy was refused"
            );
            assert_eq!(after, (false, true));
            assert_eq!(committed, Ok(()));
            assert_eq!(
                seen,
                [
                    "[[Numeric(Integer(7))]]".to_string(),
                    "[[Numeric(Integer(0))]]".to_string()
                ]
            );
        },
    );
}

/// A writer reset at a stop inside its Transaction instruction, after its transaction began and
/// before its write did (its cold read of the schema cookie, the IO thread held), ends the
/// transaction it began. The connection then shows none open, and its next write is a statement
/// of its own: committed at its halt, and read by another connection. A registration counted as
/// another writer's left the transaction open there, and every later write of the connection
/// joined it (mutant `own-registration-counted-as-another`).
#[test]
fn a_writer_reset_between_its_begin_and_its_write_ends_the_transaction_it_began() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_reset_between_its_begin_and_its_write_ends_the_transaction_it_began"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = crate::storage::atoll::requests::opened_async(|| {
                open(&io, "/db/one-writer-reset-in-transaction")
            });
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            let set = [
                ran(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, w TEXT)"),
                ran(&conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')"),
            ];
            // Every page written back and clean, so each can be evicted.
            let checkpointed = [
                ran(&conn, "PRAGMA wal_checkpoint(TRUNCATE)"),
                ran(&conn, "PRAGMA wal_checkpoint(TRUNCATE)"),
            ];
            let mut x = conn.prepare("INSERT INTO t VALUES (10, 'reset')").unwrap();
            // Cold after the prepare, which reads the schema cookie itself.
            let chilled = chill(&db);
            let mut stops = Vec::new();
            let mut reset_at = None;
            let mut ended = String::new();
            while ended.is_empty() && stops.len() < 1_000 {
                let Some(step) = framed_with_the_io_thread_held(&db, &mut x) else {
                    ended = "the IO thread was not held".to_string();
                    break;
                };
                match step {
                    Ok(stop @ (crate::StepResult::IO | crate::StepResult::Yield)) => {
                        let shown = at_stop(&conn, &x);
                        stops.push(shown);
                        if shown == (true, false, true) {
                            reset_at = Some(x.reset().map_err(|e| e.to_string()));
                            break;
                        }
                        if matches!(stop, crate::StepResult::IO) {
                            if let Err(e) = x.wait_io() {
                                ended = format!("io error: {e}");
                            }
                        }
                    }
                    Ok(other) => ended = format!("{other:?}"),
                    Err(e) => ended = format!("error: {e}"),
                }
            }
            let after_reset = (shows(&conn), at_stop(&conn, &x));
            let next = ran(&conn, "INSERT INTO t VALUES (11, 'next')");
            let after_next = shows(&conn);
            let seen = ran(&other, "SELECT id FROM t ORDER BY id");
            drop(x);
            let checked = ran(&other, "PRAGMA integrity_check");
            drop((conn, other, db));
            assert_eq!(set, ["[]", "[]"]);
            assert!(
                checkpointed.iter().all(|c| !c.starts_with("error")),
                "{checkpointed:?}"
            );
            assert!(chilled, "a request still outstanding before the eviction");
            assert_eq!(
                reset_at,
                Some(Ok(())),
                "the writer never stopped between its begin and its write: its stops {stops:?}, then {ended}"
            );
            assert_eq!(
                after_reset,
                ((false, true), (false, false, false)),
                "a writer reset between its begin and its write left its transaction open"
            );
            assert_eq!(next, "[]");
            assert_eq!(after_next, (false, true));
            assert_eq!(
                seen, "[[Numeric(Integer(1))], [Numeric(Integer(2))], [Numeric(Integer(11))]]",
                "the write after the reset was not committed, or the reset write was"
            );
            assert_eq!(checked, "[[Text(Text { value: \"ok\", subtype: Text })]]");
        },
    );
}

/// The same reset with a reader of the connection standing at a row in the writer's transaction:
/// the writer, which has changed nothing, leaves the transaction to the reader, which reads on
/// to its end and ends it there. The connection's next write is committed at its halt, and
/// another connection reads it. Ended under the reader, the reader's next step failed with "an
/// Atoll cursor used outside a transaction" (mutant `unbegun-writer-ends-under-a-reader`).
#[test]
fn a_writer_reset_between_its_begin_and_its_write_leaves_a_reader_its_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_reset_between_its_begin_and_its_write_leaves_a_reader_its_transaction"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = crate::storage::atoll::requests::opened_async(|| {
                open(&io, "/db/one-writer-reset-beside-a-reader")
            });
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            let set = [
                ran(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, w TEXT)"),
                ran(&conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')"),
            ];
            let checkpointed = [
                ran(&conn, "PRAGMA wal_checkpoint(TRUNCATE)"),
                ran(&conn, "PRAGMA wal_checkpoint(TRUNCATE)"),
            ];
            let mut x = conn.prepare("INSERT INTO t VALUES (10, 'reset')").unwrap();
            let mut y = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let chilled = chill(&db);
            let mut stops = Vec::new();
            let mut beside = None;
            let mut ended = String::new();
            while ended.is_empty() && stops.len() < 1_000 {
                let Some(step) = framed_with_the_io_thread_held(&db, &mut x) else {
                    ended = "the IO thread was not held".to_string();
                    break;
                };
                match step {
                    Ok(stop @ (crate::StepResult::IO | crate::StepResult::Yield)) => {
                        let shown = at_stop(&conn, &x);
                        stops.push(shown);
                        if shown == (true, false, true) {
                            // The reader joins the writer's transaction and stands at its first row.
                            let first = loop {
                                match y.step() {
                                    Ok(crate::StepResult::IO) => {
                                        if let Err(e) = y.wait_io() {
                                            break format!("io error: {e}");
                                        }
                                    }
                                    Ok(other) => break format!("{other:?}"),
                                    Err(e) => break format!("error: {e}"),
                                }
                            };
                            let reset = x.reset().map_err(|e| e.to_string());
                            let open_after =
                                !matches!(conn.get_tx_state(), crate::TransactionState::None);
                            beside = Some((first, reset, open_after, at_stop(&conn, &x)));
                            break;
                        }
                        if matches!(stop, crate::StepResult::IO) {
                            if let Err(e) = x.wait_io() {
                                ended = format!("io error: {e}");
                            }
                        }
                    }
                    Ok(other) => ended = format!("{other:?}"),
                    Err(e) => ended = format!("error: {e}"),
                }
            }
            let rest = stepped(&mut y, None);
            drop(y);
            let after_reader = shows(&conn);
            let next = ran(&conn, "INSERT INTO t VALUES (11, 'next')");
            let seen = ran(&other, "SELECT id FROM t ORDER BY id");
            drop(x);
            let checked = ran(&other, "PRAGMA integrity_check");
            drop((conn, other, db));
            assert_eq!(set, ["[]", "[]"]);
            assert!(
                checkpointed.iter().all(|c| !c.starts_with("error")),
                "{checkpointed:?}"
            );
            assert!(chilled, "a request still outstanding before the eviction");
            assert_eq!(
                beside,
                Some(("Row".to_string(), Ok(()), true, (false, false, true))),
                "a writer reset between its begin and its write ended the transaction under a reader \
                 at a row, or stayed registered (its stops {stops:?}, then {ended})"
            );
            assert_eq!(
                rest, "Done",
                "the reader did not read on to its end after the writer's reset"
            );
            assert_eq!(after_reader, (false, true));
            assert_eq!(next, "[]");
            assert_eq!(
                seen, "[[Numeric(Integer(1))], [Numeric(Integer(2))], [Numeric(Integer(11))]]",
                "the write after the reset was not committed, or the reset write was"
            );
            assert_eq!(checked, "[[Text(Text { value: \"ok\", subtype: Text })]]");
        },
    );
}

/// Row 67 with a reader (the auditor's #5968): a schema change stepped cold in cooperative frames
/// with the IO thread held, its first stop inside its Transaction instruction, and at every stop
/// a SELECT of the connection run whole. The schema change is the connection's registered writer from its first
/// stop, so the reader's halt does not end the transaction the writer began: the writer, stepped
/// outside the frames from the first stop past its begin, ends Done and committed, and every
/// reader returned the committed count. Registered only where its statement begins,
/// the writer went on in a transaction its statement never started, and the process ended at "a
/// tree created before the first statement" (mutant `admitted-at-begin-statement`).
#[test]
fn a_reader_run_whole_at_every_stop_of_a_writer_stepped_cold_leaves_it_its_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_run_whole_at_every_stop_of_a_writer_stepped_cold_leaves_it_its_transaction"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = crate::storage::atoll::requests::opened_async(|| {
                open(&io, "/db/one-writer-reader-at-every-stop")
            });
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let other = db.connect(ConnectOpts::new()).unwrap();
            let set = [
                ran(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, w TEXT)"),
                ran(&conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
            ];
            // Every page written back and clean, so each can be evicted.
            let checkpointed = [
                ran(&conn, "PRAGMA wal_checkpoint(TRUNCATE)"),
                ran(&conn, "PRAGMA wal_checkpoint(TRUNCATE)"),
            ];
            let mut x = conn.prepare(CREATE_C).unwrap();
            let chilled = chill(&db);
            let mut stops = Vec::new();
            let mut readers = Vec::new();
            let mut ended = String::new();
            while ended.is_empty() && stops.len() < 1_000 {
                let Some(step) = framed_with_the_io_thread_held(&db, &mut x) else {
                    ended = "the IO thread was not held".to_string();
                    break;
                };
                match step {
                    Ok(stop @ (crate::StepResult::IO | crate::StepResult::Yield)) => {
                        let shown = at_stop(&conn, &x);
                        stops.push(shown);
                        readers.push(ran(&conn, "SELECT count(*) FROM t"));
                        if shown.1 {
                            // Its write begun, it finishes outside the frames: its commit needs
                            // the log's preparer, which no worker drives for a frame yet.
                            ended = stepped(&mut x, None);
                        } else if matches!(stop, crate::StepResult::IO) {
                            if let Err(e) = x.wait_io() {
                                ended = format!("io error: {e}");
                            }
                        }
                    }
                    Ok(other) => ended = format!("{other:?}"),
                    Err(e) => ended = format!("error: {e}"),
                }
            }
            drop(x);
            let after = shows(&conn);
            let seen = [
                ran(
                    &other,
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
                ),
                ran(&other, "SELECT count(*) FROM t"),
            ];
            let checked = ran(&other, "PRAGMA integrity_check");
            drop((conn, other, db));
            assert_eq!(set, ["[]", "[]"]);
            assert!(
                checkpointed.iter().all(|c| !c.starts_with("error")),
                "{checkpointed:?}"
            );
            assert!(chilled, "a request still outstanding before the eviction");
            assert!(
                stops.first().is_some_and(|s| s.0 && !s.1),
                "the writer's first stop was not inside its Transaction instruction, registered: {stops:?}"
            );
            assert!(
                stops.iter().all(|s| s.0),
                "the writer stood at a stop not registered: {stops:?}"
            );
            assert_eq!(ended, "Done", "after {} stops", stops.len());
            assert!(
                readers.iter().all(|r| r == "[[Numeric(Integer(3))]]"),
                "a reader beside the writer did not return the committed count: {readers:?}"
            );
            assert_eq!(after, (false, true));
            assert_eq!(
                seen,
                [
                    "[[Numeric(Integer(1))]]".to_string(),
                    "[[Numeric(Integer(3))]]".to_string()
                ]
            );
            assert_eq!(checked, "[[Text(Text { value: \"ok\", subtype: Text })]]");
        },
    );
}
