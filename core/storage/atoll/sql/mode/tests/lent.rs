//! A statement's lent transaction (ASYNC1 E1b): a connection lends a statement a transaction of
//! its own, which the statement's token names until the statement gives it back. Each test
//! records what it sees, drops every connection, and asserts after, in a child of its own
//! (`testing::cleanly`). The two that break an invariant end their child, and the parent reads
//! the child's words.

use super::*;
use crate::storage::atoll::drive;
use crate::storage::atoll::sql::mode::{Begin, ConnTxn, StatementTxn, SQL_TXNS_MADE};
use crate::storage::atoll::testing::in_child;

fn ct(conn: &Arc<Connection>) -> &ConnTxn {
    conn.atoll().expect("Atoll mode")
}

/// A database with a table `t`, and two connections to it.
fn two_connections(path: &str) -> (Arc<Database>, Arc<Connection>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, path);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    (db, conn, other)
}

/// Begins a read in the transaction `statement` names.
fn begin_read(conn: &Arc<Connection>, statement: &StatementTxn) -> String {
    let io = conn.db.io.clone();
    match drive(&io, || {
        ct(conn).begin(TxnOf::Statement(statement), Begin::Read)
    }) {
        Ok(()) => "begun".to_string(),
        Err(e) => format!("error: {e}"),
    }
}

/// Whether the transaction `statement` names is open, and whether the connection's own is.
fn open_in(conn: &Arc<Connection>, statement: &StatementTxn) -> (bool, bool) {
    (
        ct(conn).is_open(TxnOf::Statement(statement)),
        ct(conn).is_open(TxnOf::Connection),
    )
}

#[test]
fn a_lent_transaction_is_the_statements_own_and_is_lent_again_once_given_back() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_transaction_is_the_statements_own_and_is_lent_again_once_given_back"
        ),
        || {
            let (db, conn, other) = two_connections("/db/lent-own");
            let made_before = SQL_TXNS_MADE.with(|n| n.get());
            let mut x = StatementTxn::default();
            let unlent = (x.holds_lent(), ct(&conn).lendable());
            // With nothing lent the statement's name is the connection's own transaction.
            let begun_in_own = begin_read(&conn, &x);
            let in_own = open_in(&conn, &x);
            drop(ct(&conn).end(TxnOf::Statement(&x), false));
            ct(&conn).lend(&mut x);
            let lent = (x.holds_lent(), ct(&conn).lendable());
            let begun = begin_read(&conn, &x);
            let in_lent = open_in(&conn, &x);
            drop(ct(&conn).end(TxnOf::Statement(&x), false));
            let ended = open_in(&conn, &x);
            ct(&conn).take_back(&mut x);
            let back = (x.holds_lent(), ct(&conn).lendable());
            let mut y = StatementTxn::default();
            ct(&conn).lend(&mut y);
            let again = ct(&conn).lendable();
            let made = SQL_TXNS_MADE.with(|n| n.get()) - made_before;
            ct(&conn).take_back(&mut y);
            drop((conn, other, db));
            assert_eq!(unlent, (false, (0, 0)));
            assert_eq!(begun_in_own, "begun");
            assert_eq!(
                in_own,
                (true, true),
                "a statement lent nothing did not name the connection's own transaction"
            );
            assert_eq!(lent, (true, (1, 0)));
            assert_eq!(begun, "begun");
            assert_eq!(
                in_lent,
                (true, false),
                "a statement's lent transaction is not its own"
            );
            assert_eq!(ended, (false, false));
            assert_eq!(back, (false, (1, 1)));
            assert_eq!(
                again,
                (1, 0),
                "the transaction given back was not lent again"
            );
            assert_eq!(made, 1, "a transaction was made where one was free");
        },
    );
}

#[test]
fn two_statements_lent_at_once_hold_two_transactions() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::two_statements_lent_at_once_hold_two_transactions"
        ),
        || {
            let (db, conn, other) = two_connections("/db/lent-two");
            let (mut x, mut y) = (StatementTxn::default(), StatementTxn::default());
            ct(&conn).lend(&mut x);
            ct(&conn).lend(&mut y);
            let lent = ct(&conn).lendable();
            let begun_x = begin_read(&conn, &x);
            let committed = other
                .execute("INSERT INTO t VALUES (1, 'between')")
                .map_err(|e| e.to_string());
            let begun_y = begin_read(&conn, &y);
            let snapshots = (
                ct(&conn).snapshot(TxnOf::Statement(&x)),
                ct(&conn).snapshot(TxnOf::Statement(&y)),
            );
            drop(ct(&conn).end(TxnOf::Statement(&x), false));
            let after_x = (open_in(&conn, &x).0, open_in(&conn, &y).0);
            drop(ct(&conn).end(TxnOf::Statement(&y), false));
            ct(&conn).take_back(&mut x);
            ct(&conn).take_back(&mut y);
            let back = ct(&conn).lendable();
            drop((conn, other, db));
            assert_eq!(lent, (2, 0));
            assert_eq!((begun_x.as_str(), begun_y.as_str()), ("begun", "begun"));
            assert_eq!(committed, Ok(()));
            let (Some(sx), Some(sy)) = snapshots else {
                panic!("a lent transaction has no snapshot: {snapshots:?}");
            };
            assert!(
                sx < sy,
                "the second statement's snapshot {sy} is not after the first's {sx}: one transaction"
            );
            assert_eq!(
                after_x,
                (false, true),
                "one statement's end ended the other's transaction"
            );
            assert_eq!(back, (2, 2));
        },
    );
}

#[test]
fn a_close_ends_every_lent_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(module_path!(), "::a_close_ends_every_lent_transaction"),
        || {
            let (db, conn, other) = two_connections("/db/lent-close");
            let (mut x, mut y) = (StatementTxn::default(), StatementTxn::default());
            ct(&conn).lend(&mut x);
            ct(&conn).lend(&mut y);
            let begun = (begin_read(&conn, &x), begin_read(&conn, &y));
            let before = (open_in(&conn, &x).0, open_in(&conn, &y).0);
            ct(&conn).end_every();
            let after = (open_in(&conn, &x).0, open_in(&conn, &y).0);
            ct(&conn).take_back(&mut x);
            ct(&conn).take_back(&mut y);
            drop((conn, other, db));
            assert_eq!(begun, ("begun".to_string(), "begun".to_string()));
            assert_eq!(before, (true, true));
            assert_eq!(
                after,
                (false, false),
                "a close left a lent transaction open"
            );
        },
    );
}

#[test]
fn a_lent_transaction_given_back_open_ends_the_process() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_lent_transaction_given_back_open_ends_the_process"
        ),
        || {
            let (_db, conn, _other) = two_connections("/db/lent-back-open");
            let mut x = StatementTxn::default();
            ct(&conn).lend(&mut x);
            assert_eq!(begin_read(&conn, &x), "begun");
            ct(&conn).take_back(&mut x);
        },
    );
    let Some(ended) = ended else { return };
    assert!(
        ended.aborted
            && ended.stderr.contains(
                "a lent transaction is given back with something of a transaction left in it"
            ),
        "{:?} aborted {}\n{}",
        ended.code,
        ended.aborted,
        ended.stderr
    );
}

#[test]
fn a_statement_that_holds_a_lent_transaction_is_not_lent_another() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_statement_that_holds_a_lent_transaction_is_not_lent_another"
        ),
        || {
            let (_db, conn, _other) = two_connections("/db/lent-twice");
            let mut x = StatementTxn::default();
            ct(&conn).lend(&mut x);
            ct(&conn).lend(&mut x);
        },
    );
    let Some(ended) = ended else { return };
    assert!(
        ended.aborted
            && ended
                .stderr
                .contains("a statement that holds a lent transaction is lent another"),
        "{:?} aborted {}\n{}",
        ended.code,
        ended.aborted,
        ended.stderr
    );
}

/// Steps `stmt` to its next row, driving the IO it waits on: the row's first column, or what
/// ended the statement.
fn next_row(stmt: &mut crate::Statement) -> Result<Option<i64>, String> {
    loop {
        match stmt.step() {
            Ok(crate::StepResult::IO) => stmt._io().step().map_err(|e| e.to_string())?,
            Ok(crate::StepResult::Row) => {
                let row = stmt.row().expect("a row");
                return match row.get_value(0) {
                    crate::Value::Numeric(crate::Numeric::Integer(i)) => Ok(Some(*i)),
                    other => Err(format!("not an integer: {other:?}")),
                };
            }
            Ok(crate::StepResult::Done) => return Ok(None),
            Ok(crate::StepResult::Yield) => stmt._io().step().map_err(|e| e.to_string())?,
            Ok(other) => return Err(format!("{other:?}")),
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// Every row's first column left in `stmt`.
fn rest(stmt: &mut crate::Statement) -> Result<Vec<i64>, String> {
    let mut rows = Vec::new();
    while let Some(row) = next_row(stmt)? {
        rows.push(row);
    }
    Ok(rows)
}

fn count(conn: &Arc<Connection>) -> Result<Option<i64>, String> {
    let mut stmt = conn
        .prepare("SELECT count(*) FROM t")
        .map_err(|e| e.to_string())?;
    next_row(&mut stmt)
}

/// A table `t` with rows 1 to 3.
fn three_rows(path: &str) -> (Arc<Database>, Arc<Connection>, Arc<Connection>) {
    let (db, conn, other) = two_connections(path);
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    (db, conn, other)
}

#[test]
fn two_readers_of_a_connection_read_in_two_transactions() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::two_readers_of_a_connection_read_in_two_transactions"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-readers");
            let mut a = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut a);
            let committed = other
                .execute("INSERT INTO t VALUES (4, 'd')")
                .map_err(|e| e.to_string());
            let b_count = count(&conn);
            let lent_beside = ct(&conn).lendable();
            let a_rest = rest(&mut a);
            let autocommit = conn.get_auto_commit();
            drop(a);
            let lent_after = ct(&conn).lendable();
            drop((conn, other, db));
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(committed, Ok(()));
            assert_eq!(
                b_count,
                Ok(Some(4)),
                "a reader begun after another connection's commit did not see it"
            );
            assert_eq!(
                a_rest,
                Ok(vec![2, 3]),
                "a reader saw a row committed after it began"
            );
            assert!(autocommit);
            assert_eq!(
                lent_beside.0, 2,
                "two readers open at once did not hold two lent transactions"
            );
            assert_eq!(
                lent_after,
                (2, 2),
                "a dropped reader's lease was not given back"
            );
        },
    );
}

#[test]
fn a_reader_beside_a_writer_of_its_connection_keeps_its_snapshot() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_beside_a_writer_of_its_connection_keeps_its_snapshot"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-beside-writer");
            let mut a = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut a);
            let written = conn
                .execute("INSERT INTO t VALUES (4, 'd')")
                .map_err(|e| e.to_string());
            let seen_by_other = count(&other);
            let a_rest = rest(&mut a);
            let state = (conn.get_auto_commit(), conn.get_tx_state());
            drop(a);
            let after = count(&conn);
            drop((conn, other, db));
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(written, Ok(()));
            assert_eq!(
                seen_by_other,
                Ok(Some(4)),
                "the writer did not commit at its halt"
            );
            assert_eq!(
                a_rest,
                Ok(vec![2, 3]),
                "a reader saw what a writer of its connection committed after it began"
            );
            assert_eq!(state, (true, crate::TransactionState::None));
            assert_eq!(after, Ok(Some(4)));
        },
    );
}

#[test]
fn a_reader_reset_at_a_row_ends_its_transaction_and_keeps_its_lease() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_reset_at_a_row_ends_its_transaction_and_keeps_its_lease"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-reset");
            let mut a = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut a);
            let open_at_row = (
                ct(&conn).is_open(TxnOf::Statement(a.atoll_txn_for_test())),
                ct(&conn).is_open(TxnOf::Connection),
            );
            let reset = a.reset().map_err(|e| e.to_string());
            let after_reset = (
                a.atoll_txn_for_test().holds_lent(),
                ct(&conn).is_open(TxnOf::Statement(a.atoll_txn_for_test())),
                ct(&conn).lendable(),
            );
            let committed = other
                .execute("INSERT INTO t VALUES (4, 'd')")
                .map_err(|e| e.to_string());
            let again = rest(&mut a);
            let made = ct(&conn).lendable();
            drop(a);
            drop((conn, other, db));
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(open_at_row, (true, false));
            assert_eq!(reset, Ok(()));
            assert_eq!(
                after_reset,
                (true, false, (1, 0)),
                "a reset reader did not end its lent transaction and keep the lease"
            );
            assert_eq!(committed, Ok(()));
            assert_eq!(
                again,
                Ok(vec![1, 2, 3, 4]),
                "a reader run again read the old snapshot"
            );
            assert_eq!(
                made,
                (1, 0),
                "a reader run again was lent another transaction"
            );
        },
    );
}

#[test]
fn begin_beside_an_open_reader_leaves_the_reader_its_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::begin_beside_an_open_reader_leaves_the_reader_its_transaction"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-begin-beside");
            let mut a = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut a);
            let begun = conn.execute("BEGIN").map_err(|e| e.to_string());
            let written = conn
                .execute("INSERT INTO t VALUES (5, 'e')")
                .map_err(|e| e.to_string());
            drop(a);
            let state = (conn.get_auto_commit(), ct(&conn).is_open(TxnOf::Connection));
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            let seen = count(&other);
            drop((conn, other, db));
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(begun, Ok(()));
            assert_eq!(written, Ok(()));
            assert_eq!(
                state,
                (false, true),
                "a reader begun in autocommit, dropped inside BEGIN, ended the explicit transaction"
            );
            assert_eq!(committed, Ok(()));
            assert_eq!(seen, Ok(Some(4)), "the explicit transaction's row was lost");
        },
    );
}

#[test]
fn a_reader_run_inside_begin_gives_its_lease_back_and_reads_in_the_explicit_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_run_inside_begin_gives_its_lease_back_and_reads_in_the_explicit_transaction"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-then-begin");
            let mut a = conn.prepare("SELECT count(*) FROM t").unwrap();
            let lent_run = (next_row(&mut a), a.atoll_txn_for_test().holds_lent());
            a.reset().unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (4, 'd')").unwrap();
            let in_begin = (next_row(&mut a), a.atoll_txn_for_test().holds_lent());
            a.reset().unwrap();
            let rolled_back = conn.execute("ROLLBACK").map_err(|e| e.to_string());
            let lent_again = (next_row(&mut a), a.atoll_txn_for_test().holds_lent());
            let leases = ct(&conn).lendable();
            drop(a);
            drop((conn, other, db));
            assert_eq!(lent_run, (Ok(Some(3)), true));
            assert_eq!(
                in_begin,
                (Ok(Some(4)), false),
                "a reader run inside BEGIN did not read the explicit transaction's row in it"
            );
            assert_eq!(rolled_back, Ok(()));
            assert_eq!(lent_again, (Ok(Some(3)), true));
            assert_eq!(leases, (1, 0));
        },
    );
}

/// Steps `stmt` until it returns something other than IO, a yield or a row, driving its IO and
/// sleeping a millisecond between steps so a wait with a bound can pass it.
fn run_to_end(stmt: &mut crate::Statement) -> String {
    loop {
        match stmt.step() {
            Ok(crate::StepResult::IO) => {
                std::thread::sleep(std::time::Duration::from_millis(1));
                if let Err(e) = stmt._io().step() {
                    return format!("io error: {e}");
                }
            }
            Ok(crate::StepResult::Yield) => {
                std::thread::sleep(std::time::Duration::from_millis(1));
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

#[test]
fn a_writer_busy_at_its_take_ends_its_lent_transaction_and_runs_again_alone() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_busy_at_its_take_ends_its_lent_transaction_and_runs_again_alone"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-writer-busy");
            let held = other.execute("BEGIN IMMEDIATE").map_err(|e| e.to_string());
            conn.set_busy_timeout(std::time::Duration::from_millis(5));
            let mut x = conn.prepare("INSERT INTO t VALUES (9, 'x')").unwrap();
            let busy = run_to_end(&mut x);
            let after_busy = (
                x.holds_atoll_lease_for_test(),
                ct(&conn).is_open(TxnOf::Statement(x.atoll_txn_for_test())),
                ct(&conn).is_open(TxnOf::Connection),
            );
            let released = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = run_to_end(&mut x);
            let after = ct(&conn).is_open(TxnOf::Statement(x.atoll_txn_for_test()));
            drop(x);
            let seen = count(&other);
            drop((conn, other, db));
            assert_eq!(held, Ok(()));
            assert_eq!(busy, "Busy");
            assert_eq!(
                after_busy,
                (true, false, false),
                "a writer Busy at its take kept a transaction open, or lost its lease"
            );
            assert_eq!(released, Ok(()));
            assert_eq!(finished, "Done");
            assert!(!after, "a writer's halt left its lent transaction open");
            assert_eq!(
                seen,
                Ok(Some(4)),
                "the writer stepped again after its Busy did not commit"
            );
        },
    );
}

/// A statement with a trigger runs lent, then runs again inside BEGIN as a member, while another
/// statement of the connection holds a lent transaction open beside it: the trigger's writes in the
/// member run are the explicit transaction's, undone by its ROLLBACK, and no copy of the
/// statement's name made in its lent run (its trigger's statement) is read after the statement
/// gave that transaction back (the auditor's #6090; mutant stale-copy-kept).
#[test]
fn a_statement_with_a_trigger_lent_then_run_inside_begin_writes_its_trigger_in_the_users_transaction(
) {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_statement_with_a_trigger_lent_then_run_inside_begin_writes_its_trigger_in_the_users_transaction"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-trigger-then-begin");
            conn.execute("CREATE TABLE log (x INTEGER)").unwrap();
            conn.execute(
                "CREATE TRIGGER t_log AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.id); END",
            )
            .unwrap();
            let mut y = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first_row = matches!(y.step(), Ok(crate::StepResult::Row));
            let mut x = conn.prepare("INSERT INTO t VALUES (?1, 'x')").unwrap();
            x.bind_at(1.try_into().unwrap(), Value::from_i64(10)).unwrap();
            let lent_run = run_to_end(&mut x);
            let leased_then = x.holds_atoll_lease_for_test();
            let began = conn.execute("BEGIN").map_err(|e| e.to_string());
            x.reset().unwrap();
            x.bind_at(1.try_into().unwrap(), Value::from_i64(11)).unwrap();
            let member_run = run_to_end(&mut x);
            let leased_after = x.holds_atoll_lease_for_test();
            drop(x);
            let rolled_back = conn.execute("ROLLBACK").map_err(|e| e.to_string());
            let y_end = run_to_end(&mut y);
            drop(y);
            let rows = count(&other);
            let logged = text(&other, "SELECT group_concat(x) FROM log");
            drop((conn, other, db));
            assert!(first_row, "the open reader's first row");
            assert_eq!((lent_run.as_str(), leased_then), ("Done", true), "the lent run");
            assert_eq!(began, Ok(()));
            assert_eq!(
                (member_run.as_str(), leased_after),
                ("Done", false),
                "the member run, its lease given back"
            );
            assert_eq!((rolled_back, y_end.as_str()), (Ok(()), "Done"));
            assert_eq!(
                (rows, logged),
                (Ok(Some(4)), Ok("10".to_string())),
                "after the ROLLBACK, the rows of t and of the trigger's log"
            );
        },
    );
}

/// A statement's kept pointer to its transaction (`StatementTxn::resolved`) is set only where the
/// transaction its name resolves to changes (an adoption, a give-back, and `StatementTxn::hold`,
/// which a lend and the names a close's walk makes for each lent transaction go through), and read
/// only by `Resolves::txn`, after the debug check that compares it with the name's resolution on
/// every call. The field is private to mode.rs, so mode.rs and its test files are every place that
/// can name it (the auditor's #6087).
#[test]
fn a_statements_kept_transaction_pointer_is_set_and_read_only_where_reviewed() {
    fn names_the_field(line: &str) -> bool {
        let ident = |c: char| c.is_alphanumeric() || c == '_';
        line.match_indices("resolved").any(|(at, _)| {
            let before = line[..at].chars().next_back();
            let after = line[at + "resolved".len()..].chars().next();
            !before.is_some_and(ident) && !after.is_some_and(|c| ident(c) || c == '(')
        })
    }
    let sql = crate::storage::atoll::testing::atoll_dir().join("sql");
    let mode = crate::source_test::code_of(&std::fs::read_to_string(sql.join("mode.rs")).unwrap());
    let mut paths = vec![sql.join("mode")];
    let mut seen: Vec<String> = mode
        .lines()
        .filter(|l| names_the_field(l))
        .map(|l| l.trim().to_string())
        .collect();
    while let Some(path) = paths.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap() {
                paths.push(entry.unwrap().path());
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            let code = crate::source_test::code_of(&std::fs::read_to_string(&path).unwrap());
            seen.extend(
                code.lines()
                    .filter(|l| names_the_field(l))
                    .map(|l| l.trim().to_string()),
            );
        }
    }
    seen.sort();
    let mut want = vec![
        "resolved: Resolved,",
        "self.resolved.0 != std::ptr::NonNull::dangling()",
        "self.resolved.0.as_ptr(),",
        "Named(unsafe { self.resolved.0.as_ref() })",
        "statement.resolved = Resolved::to(&self.own);",
        "self.resolved = lent;",
        "statement.resolved = Resolved::to(&self.own);",
    ];
    want.sort_unstable();
    assert_eq!(
        seen, want,
        "the places that set or read a statement's kept pointer"
    );
    let check = mode.find("self.resolved.0.as_ptr()").unwrap();
    let read = mode.find("self.resolved.0.as_ref()").unwrap();
    let asserted = mode[..check].rfind("turso_debug_assert!").unwrap();
    assert!(
        asserted < check && check < read && !mode[asserted..read].contains("fn "),
        "the kept pointer is read without its debug check before it, in one function"
    );
}

/// Every statement for a subprogram is made by `Statement::new_subprogram` or
/// `Statement::new_view_maintenance`, which name its Atoll transaction before it runs: one made
/// any other way would run with a name that resolves to nothing, as a materialized view's
/// population once did (ASYNC1 E1b, #6084).
#[test]
fn every_subprogram_statement_is_made_with_its_transactions_name() {
    let core = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let (mut paths, mut made) = (vec![core.to_path_buf()], Vec::new());
    while let Some(path) = paths.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap() {
                paths.push(entry.unwrap().path());
            }
            continue;
        }
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let code = crate::source_test::code_of(&std::fs::read_to_string(&path).unwrap());
        let mut rest = code.as_str();
        while let Some(at) = rest.find("new_with_origin(") {
            let args = &rest[at + "new_with_origin(".len()..];
            let mut depth = 1;
            let end = args
                .char_indices()
                .find(|&(_, c)| {
                    depth += (c == '(') as i32 - (c == ')') as i32;
                    depth == 0
                })
                .map_or(args.len(), |(i, _)| i);
            if args[..end].contains("StatementOrigin::Subprogram") {
                made.push(path.strip_prefix(core).unwrap().display().to_string());
            }
            rest = &args[end..];
        }
    }
    made.sort();
    assert_eq!(
        made,
        vec!["statement.rs", "statement.rs"],
        "a subprogram statement made outside its two constructors"
    );
}

/// A writer that came back Busy at its take inside a BEGIN no statement had run in leaves no
/// transaction in its connection's own, and the BEGIN stays open: before, its begin's transaction
/// stayed there, the connection's state none, so the BEGIN's ROLLBACK ended nothing and the
/// connection's next statement joined it, with its wait group (the simulator's seed 4, #6084).
#[test]
fn a_writer_busy_inside_begin_leaves_no_transaction_for_its_end_to_miss() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_busy_inside_begin_leaves_no_transaction_for_its_end_to_miss"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-busy-inside-begin");
            let held = other.execute("BEGIN IMMEDIATE").map_err(|e| e.to_string());
            conn.set_busy_timeout(std::time::Duration::from_millis(5));
            let began = conn.execute("BEGIN").map_err(|e| e.to_string());
            let mut x = conn.prepare("INSERT INTO t VALUES (9, 'x')").unwrap();
            let busy = run_to_end(&mut x);
            drop(x);
            let after_busy = (ct(&conn).is_open(TxnOf::Connection), conn.get_auto_commit());
            let released = other.execute("COMMIT").map_err(|e| e.to_string());
            let rolled_back = conn.execute("ROLLBACK").map_err(|e| e.to_string());
            let after_rollback = ct(&conn).is_open(TxnOf::Connection);
            let written = conn
                .execute("INSERT INTO t VALUES (10, 'y')")
                .map_err(|e| e.to_string());
            let seen = count(&other);
            drop((conn, other, db));
            assert_eq!((held, began), (Ok(()), Ok(())));
            assert_eq!(busy, "Busy");
            assert_eq!(
                after_busy,
                (false, false),
                "a writer Busy inside BEGIN left a transaction in its connection's own, or ended \
                 the BEGIN"
            );
            assert_eq!((released, rolled_back), (Ok(()), Ok(())));
            assert!(
                !after_rollback,
                "the BEGIN's ROLLBACK left a transaction open"
            );
            assert_eq!(written, Ok(()));
            assert_eq!(
                seen,
                Ok(Some(4)),
                "the statement after the ROLLBACK did not commit"
            );
        },
    );
}

/// A row write that came back Busy in autocommit, kept and stepped again after a BEGIN, begins
/// again as a member of the user's transaction: a Busy return has not begun, so the run's choice
/// is made again at the next step, and its write is undone by the user's ROLLBACK and kept by the
/// user's COMMIT (the auditor's ruling, #6084 item 2).
#[test]
fn a_writer_busy_in_autocommit_then_stepped_inside_begin_writes_in_the_users_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_busy_in_autocommit_then_stepped_inside_begin_writes_in_the_users_transaction"
        ),
        || {
            let schedule = |path: &str, end: &str| {
                let (db, conn, other) = three_rows(path);
                let held = other.execute("BEGIN IMMEDIATE").map_err(|e| e.to_string());
                conn.set_busy_timeout(std::time::Duration::from_millis(5));
                let mut x = conn.prepare("INSERT INTO t VALUES (9, 'x')").unwrap();
                let busy = run_to_end(&mut x);
                let released = other.execute("COMMIT").map_err(|e| e.to_string());
                let began = conn.execute("BEGIN").map_err(|e| e.to_string());
                let finished = run_to_end(&mut x);
                let leased = x.holds_atoll_lease_for_test();
                drop(x);
                let ended = conn.execute(end).map_err(|e| e.to_string());
                let seen = count(&other);
                drop((conn, other, db));
                (held, busy, released, began, finished, leased, ended, seen)
            };
            let rolled_back = schedule("/db/lent-busy-then-begin-rollback", "ROLLBACK");
            let committed = schedule("/db/lent-busy-then-begin-commit", "COMMIT");
            let ok = || Ok::<(), String>(());
            assert_eq!(
                rolled_back,
                (
                    ok(),
                    "Busy".to_string(),
                    ok(),
                    ok(),
                    "Done".to_string(),
                    false,
                    ok(),
                    Ok(Some(3))
                ),
                "ROLLBACK: (held, busy, released, began, finished, leased, ended, rows seen)"
            );
            assert_eq!(
                committed,
                (
                    ok(),
                    "Busy".to_string(),
                    ok(),
                    ok(),
                    "Done".to_string(),
                    false,
                    ok(),
                    Ok(Some(4))
                ),
                "COMMIT: (held, busy, released, began, finished, leased, ended, rows seen)"
            );
        },
    );
}

#[test]
fn a_writer_and_a_reader_of_one_connection_each_end_their_own_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_and_a_reader_of_one_connection_each_end_their_own_transaction"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-writer-reader");
            let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut r);
            let mut w = conn.prepare("INSERT INTO t VALUES (4, 'd')").unwrap();
            let written = run_to_end(&mut w);
            let both = (
                ct(&conn).is_open(TxnOf::Statement(r.atoll_txn_for_test())),
                ct(&conn).is_open(TxnOf::Statement(w.atoll_txn_for_test())),
                ct(&conn).lendable(),
            );
            let seen_by_other = count(&other);
            let r_rest = rest(&mut r);
            drop((r, w));
            drop((conn, other, db));
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(written, "Done");
            assert_eq!(
                both,
                (true, false, (2, 0)),
                "the reader's transaction was ended by the writer's halt, or the writer's left open"
            );
            assert_eq!(seen_by_other, Ok(Some(4)));
            assert_eq!(r_rest, Ok(vec![2, 3]));
        },
    );
}

/// A reader over one of Turso's own virtual tables (`json_each`) is lent a transaction of its own,
/// as any reader is; one over a pragma's table, which runs its pragma, or an extension's
/// (`generate_series`), whose cursor is handed the connection, runs in the connection's own
/// (auditor #6065 item 3).
#[test]
fn a_reader_over_json_each_lends_and_one_over_a_pragma_or_extension_table_does_not() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_over_json_each_lends_and_one_over_a_pragma_or_extension_table_does_not"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-virtual-tables");
            let read = |sql: &str| {
                let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
                let first = next_row(&mut stmt)?;
                let lent = stmt.atoll_txn_for_test().holds_lent();
                let rest = rest(&mut stmt)?;
                Ok::<_, String>((first, lent, rest))
            };
            let json = read("SELECT t.id FROM t, json_each('[0]') ORDER BY t.id");
            let pragma =
                read("SELECT t.id FROM t, pragma_table_info('t') WHERE cid = 0 ORDER BY t.id");
            let series = read("SELECT t.id FROM t, generate_series(1, 1) ORDER BY t.id");
            drop((conn, other, db));
            let rows = (Some(1), vec![2, 3]);
            assert_eq!(json, Ok((rows.0, true, rows.1.clone())), "json_each");
            assert_eq!(
                pragma,
                Ok((rows.0, false, rows.1.clone())),
                "pragma_table_info"
            );
            assert_eq!(series, Ok((rows.0, false, rows.1)), "generate_series");
        },
    );
}

/// A root statement stepped while its connection runs another statement's step (an index method's
/// statements, `Connection::start_nested`) is lent nothing and is no member: it reads in the
/// transaction its connection's state names, here the connection's own, begun by a member that
/// stands open (ASYNC1 E1b, as at snap-d5). So is one that would be a member outside the nesting,
/// a reader over a pragma's table (mutant nested-takes-part).
#[test]
fn a_statement_stepped_inside_another_statements_step_is_lent_nothing() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_statement_stepped_inside_another_statements_step_is_lent_nothing"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-nested");
            let mut member = conn
                .prepare("SELECT t.id FROM t, pragma_table_info('t') WHERE cid = 0 ORDER BY t.id")
                .unwrap();
            let member_first = next_row(&mut member);
            let members_before = conn.atoll_members();
            conn.start_nested();
            let mut nested = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let nested_first = next_row(&mut nested);
            let nested_lent = nested.atoll_txn_for_test().holds_lent();
            let members_with_nested = conn.atoll_members();
            let nested_rest = rest(&mut nested);
            drop(nested);
            let mut nested_reader = conn
                .prepare("SELECT t.id FROM t, pragma_table_info('t') WHERE cid = 0 ORDER BY t.id")
                .unwrap();
            let nested_reader_first = next_row(&mut nested_reader);
            let members_with_nested_reader = conn.atoll_members();
            let nested_reader_rest = rest(&mut nested_reader);
            drop(nested_reader);
            conn.end_nested();
            let member_rest = rest(&mut member);
            drop(member);
            drop((conn, other, db));
            assert!(!nested_lent, "the nested statement was lent a transaction");
            assert_eq!(
                members_with_nested, members_before,
                "the nested statement was counted a member"
            );
            assert_eq!(
                members_with_nested_reader, members_before,
                "a nested statement that does not lend was counted a member"
            );
            assert_eq!(members_before, 1, "the member");
            assert_eq!(member_first, Ok(Some(1)));
            assert_eq!(nested_first, Ok(Some(1)));
            assert_eq!(nested_rest, Ok(vec![2, 3]));
            assert_eq!(nested_reader_first, Ok(Some(1)));
            assert_eq!(nested_reader_rest, Ok(vec![2, 3]));
            assert_eq!(member_rest, Ok(vec![2, 3]));
        },
    );
}

/// A schema reparse outside a transaction (`Connection::maybe_reparse_schema`, a schema retry's)
/// compares the schema with the committed cookie, whatever transaction of its connection is half
/// begun: here the connection's own, a writer whose slot is taken and whose take is behind another
/// connection's exclusive hold, stopped (parked, or Busy with its take kept pending, R15) with no
/// snapshot yet and the connection's state still none (the simulator's "a read before the
/// transaction's snapshot", the auditor's #6078).
#[test]
fn a_schema_reparse_beside_a_half_begun_transaction_reads_the_committed_cookie() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_schema_reparse_beside_a_half_begun_transaction_reads_the_committed_cookie"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-reparse-half-begun");
            let io = db.io.clone();
            let (c, o) = (ct(&conn), ct(&other));
            drive(&io, || {
                o.begin(TxnOf::Connection, Begin::Write { schema: true })
            })
            .unwrap();
            let parked = c.begin(TxnOf::Connection, Begin::Write { schema: false });
            let half_begun = !matches!(parked, Ok(crate::types::IOResult::Done(())))
                && c.own()
                    .cell
                    .observe()
                    .as_ref()
                    .is_some_and(|txn| !txn.snapped());
            let reparse = conn.maybe_reparse_schema().map_err(|e| e.to_string());
            c.give_up(TxnOf::Connection);
            if let Some(mut ending) = c.end(TxnOf::Connection, false) {
                ending.finish().unwrap();
            }
            o.end(TxnOf::Connection, false)
                .expect("the other connection's transaction")
                .finish()
                .unwrap();
            drop((conn, other, db));
            assert!(
                half_begun,
                "the writer's begin did not stop with its transaction in the cell and no snapshot"
            );
            assert_eq!(
                reparse,
                Ok(()),
                "the reparse beside the half-begun transaction"
            );
        },
    );
}

/// Two transactions of one connection writing at once, its own and one it lent, each keep their
/// own group in the wait graph: the second's begin leaves the first's group running, so the first's
/// next wait is not answered as a deadlock (the simulator's Deadlocks, the auditor's #6078).
#[test]
fn a_connections_two_writing_transactions_each_keep_their_wait_group() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_connections_two_writing_transactions_each_keep_their_wait_group"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-two-writers-groups");
            let io = db.io.clone();
            let c = ct(&conn);
            drive(&io, || {
                c.begin(TxnOf::Connection, Begin::Write { schema: false })
            })
            .unwrap();
            let mut lent = StatementTxn::default();
            c.adopt(&mut lent);
            c.lend(&mut lent);
            drive(&io, || {
                c.begin(TxnOf::Statement(&lent), Begin::Write { schema: false })
            })
            .unwrap();
            let live = (
                c.group_is_live_for_test(TxnOf::Connection),
                c.group_is_live_for_test(TxnOf::Statement(&lent)),
            );
            c.end(TxnOf::Statement(&lent), false)
                .expect("the lent transaction")
                .finish()
                .unwrap();
            c.take_back(&mut lent);
            c.end(TxnOf::Connection, false)
                .expect("the connection's own transaction")
                .finish()
                .unwrap();
            drop((conn, other, db));
            assert_eq!(
                live,
                (true, true),
                "the groups of the own and the lent transaction"
            );
        },
    );
}

/// The first column of `sql`'s first row, as text.
fn text(conn: &Arc<Connection>, sql: &str) -> Result<String, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    loop {
        match stmt.step().map_err(|e| e.to_string())? {
            crate::StepResult::IO => stmt._io().step().map_err(|e| e.to_string())?,
            crate::StepResult::Row => {
                return Ok(stmt.row().expect("a row").get_value(0).to_string());
            }
            crate::StepResult::Yield => stmt._io().step().map_err(|e| e.to_string())?,
            other => return Err(format!("{other:?}")),
        }
    }
}

/// A reader lent a transaction in autocommit, stepped to its end after its connection began an
/// explicit transaction and wrote in it, ends its own transaction at its halt and leaves the
/// explicit one as it was (the auditor's #6067 D1).
#[test]
fn a_reader_lent_before_begin_and_ended_inside_it_ends_its_own_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_lent_before_begin_and_ended_inside_it_ends_its_own_transaction"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-ended-inside-begin");
            let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut r);
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (9001, 'm')").unwrap();
            let r_rest = rest(&mut r);
            let after = (
                ct(&conn).is_open(TxnOf::Statement(r.atoll_txn_for_test())),
                conn.get_auto_commit(),
                ct(&conn).is_open(TxnOf::Connection),
            );
            drop(r);
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            let seen = count(&other);
            drop((conn, other, db));
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(
                r_rest,
                Ok(vec![2, 3]),
                "the reader did not read its own snapshot"
            );
            assert_eq!(
                after,
                (false, false, true),
                "the reader's halt left its own transaction open, or touched the explicit one"
            );
            assert_eq!(committed, Ok(()));
            assert_eq!(seen, Ok(Some(4)), "the explicit transaction's row was lost");
        },
    );
}

/// A reader lent a transaction in autocommit, stepped to its end after its connection began an
/// explicit transaction, set a savepoint and wrote after it, leaves the savepoint: ROLLBACK TO it
/// undoes the write, and COMMIT commits the rest. The connection's savepoints are its own
/// transaction's; a lent transaction's end leaves them as they are.
#[test]
fn a_reader_lent_before_begin_and_ended_inside_it_leaves_the_connections_savepoints() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_lent_before_begin_and_ended_inside_it_leaves_the_connections_savepoints"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-ended-beside-savepoint");
            let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut r);
            let lent = r.atoll_txn_for_test().holds_lent();
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (4, 'kept')").unwrap();
            conn.execute("SAVEPOINT sp").unwrap();
            conn.execute("INSERT INTO t VALUES (5, 'undone')").unwrap();
            let r_rest = rest(&mut r);
            drop(r);
            let rolled_back = conn.execute("ROLLBACK TO sp").map_err(|e| e.to_string());
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            let seen = count(&other);
            drop((conn, other, db));
            assert_eq!(first, Ok(Some(1)));
            assert!(lent, "the reader was not lent a transaction");
            assert_eq!(r_rest, Ok(vec![2, 3]));
            assert_eq!(
                rolled_back,
                Ok(()),
                "the savepoint did not outlive the lent reader's end"
            );
            assert_eq!(committed, Ok(()));
            assert_eq!(
                seen,
                Ok(Some(4)),
                "the rows committed: the write after the savepoint not undone, or the one before lost"
            );
        },
    );
}

/// A statement whose program begins no transaction of the main database (a `SELECT` of no table,
/// an `EXPLAIN`) is lent nothing as its run starts in autocommit, and the connection lends and makes
/// none for it; a statement of a table run after them is lent the connection's first (ASYNC1 E1b:
/// a run that lends is lent as it starts only when its program begins the main database).
#[test]
fn a_statement_that_begins_no_transaction_is_lent_nothing() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_statement_that_begins_no_transaction_is_lent_nothing"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-nothing-begun");
            // The rows' INSERT was lent the connection's first, and gave it back.
            let before = ct(&conn).lendable();
            let mut no_table = conn.prepare("SELECT 1").unwrap();
            let one = next_row(&mut no_table);
            let no_table_lent = no_table.atoll_txn_for_test().holds_lent();
            let no_table_rest = rest(&mut no_table);
            let mut explain = conn.prepare("EXPLAIN SELECT id FROM t").unwrap();
            let explained = next_row(&mut explain).map(|row| row.is_some());
            let explain_lent = explain.atoll_txn_for_test().holds_lent();
            drop(explain);
            let made = ct(&conn).lendable();
            let mut table = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut table);
            let table_lent = (
                table.atoll_txn_for_test().holds_lent(),
                ct(&conn).lendable(),
            );
            drop((no_table, table));
            drop((conn, other, db));
            assert_eq!(one, Ok(Some(1)));
            assert!(
                !no_table_lent,
                "a SELECT of no table was lent a transaction"
            );
            assert_eq!(no_table_rest, Ok(vec![]));
            assert_eq!(explained, Ok(true));
            assert!(!explain_lent, "an EXPLAIN was lent a transaction");
            assert_eq!(before, (1, 1));
            assert_eq!(
                made, before,
                "a transaction lent or made to lend for statements that begin none"
            );
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(
                table_lent,
                (true, (1, 0)),
                "the statement of a table was not lent the connection's first"
            );
        },
    );
}

/// A reader lent a transaction in autocommit, stepped to its end after its connection began an
/// explicit transaction and wrote a virtual table in it, ends its own transaction and nothing of
/// the explicit one's: the virtual table's transaction stays open for the explicit one's ROLLBACK,
/// which rolls it back, and the change capture's transaction stays the explicit one's (ASYNC1 E1b:
/// the halt committed every virtual table transaction of the connection and reset its change
/// capture's transaction, whichever transaction the halting statement ended).
#[test]
fn a_lent_readers_end_leaves_the_explicit_transactions_virtual_tables_and_change_capture() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_readers_end_leaves_the_explicit_transactions_virtual_tables_and_change_capture"
        ),
        || {
            let recorded = Recorded::default();
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open(
                io.clone(),
                "/db/lent-end-beside-a-virtual-table",
                OpenOptions::new(Arc::new(SqliteDialect))
                    .atoll(crate::storage::atoll::sql::catalog::tests::config())
                    .native_module(
                        "recorded",
                        crate::native_ext::VTabKind::VirtualTable,
                        recorded.clone(),
                    ),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
                .unwrap();
            conn.execute("CREATE VIRTUAL TABLE store USING recorded")
                .unwrap();
            let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut r);
            let lent = r.atoll_txn_for_test().holds_lent();
            conn.execute("BEGIN").unwrap();
            let written = conn
                .execute("INSERT INTO store(value) VALUES (7)")
                .map_err(|e| e.to_string());
            // As the explicit transaction's change capture would have it.
            conn.set_cdc_transaction_id(41);
            let r_rest = rest(&mut r);
            let after = (
                recorded.events.lock().clone(),
                conn.get_cdc_transaction_id(),
            );
            drop(r);
            let rolled_back = conn.execute("ROLLBACK").map_err(|e| e.to_string());
            let (events, rows) = (
                recorded.events.lock().clone(),
                recorded.rows.lock().clone(),
            );
            drop((conn, db));
            assert_eq!(first, Ok(Some(1)));
            assert!(lent, "the reader was not lent a transaction");
            assert_eq!(written, Ok(()));
            assert_eq!(r_rest, Ok(vec![2, 3]));
            assert_eq!(
                after,
                (vec!["begin", "write"], 41),
                "(the virtual table's transaction, the change capture's) after the lent reader's end"
            );
            assert_eq!(rolled_back, Ok(()));
            assert_eq!(events, vec!["begin", "write", "rollback"]);
            assert_eq!(rows, Vec::<i64>::new(), "the rolled-back write kept");
        },
    );
}

/// A reader lent a transaction in autocommit and dropped before its end, while a member of the
/// connection's own transaction waits in its write with its virtual table's transaction open,
/// rolls back its own and nothing of the member's: the member's write is committed at its halt
/// (ASYNC1 E1b: the dropped reader's abort rolled back every virtual table transaction of the
/// connection, whichever transaction it ended, and reset its change capture's transaction).
#[test]
fn a_lent_readers_drop_leaves_a_members_virtual_table_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_readers_drop_leaves_a_members_virtual_table_transaction"
        ),
        || {
            let recorded = Recorded::default();
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open(
                io.clone(),
                "/db/lent-drop-beside-a-virtual-table",
                OpenOptions::new(Arc::new(SqliteDialect))
                    .atoll(crate::storage::atoll::sql::catalog::tests::config())
                    .native_module(
                        "recorded",
                        crate::native_ext::VTabKind::VirtualTable,
                        recorded.clone(),
                    ),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
                .unwrap();
            conn.execute("CREATE VIRTUAL TABLE store USING recorded")
                .unwrap();
            recorded
                .hold
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let mut member = conn.prepare("INSERT INTO store(value) VALUES (8)").unwrap();
            let member_paused = match member.step() {
                Ok(crate::StepResult::IO | crate::StepResult::Yield) => Ok(()),
                other => Err(format!("{other:?}")),
            };
            let member_lent = member.atoll_txn_for_test().holds_lent();
            let at_pause = recorded.events.lock().clone();
            conn.set_cdc_transaction_id(41);
            let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut r);
            let lent = r.atoll_txn_for_test().holds_lent();
            drop(r);
            let after_drop = (
                recorded.events.lock().clone(),
                conn.get_cdc_transaction_id(),
            );
            recorded
                .held
                .lock()
                .take()
                .expect("the member's held write")
                .complete(0);
            let member_rest = rest(&mut member);
            drop(member);
            let (events, rows) = (recorded.events.lock().clone(), recorded.rows.lock().clone());
            drop((conn, db));
            assert_eq!(member_paused, Ok(()), "the member's write did not wait");
            assert_eq!(
                at_pause,
                vec!["begin"],
                "the member's virtual table transaction"
            );
            assert!(
                !member_lent,
                "the virtual table's writer was lent a transaction"
            );
            assert_eq!(first, Ok(Some(1)));
            assert!(lent, "the reader was not lent a transaction");
            assert_eq!(
                after_drop,
                (vec!["begin"], 41),
                "(the member's virtual table transaction, the change capture's) after the lent \
                 reader's drop"
            );
            assert_eq!(member_rest, Ok(vec![]));
            assert_eq!(events, vec!["begin", "write", "commit"]);
            assert_eq!(rows, vec![8], "the member's write");
        },
    );
}

/// A writer lent a transaction in autocommit whose deferred foreign key fails at its halt, which
/// comes after its connection began an explicit transaction and read in it, fails and leaves the
/// explicit transaction as it was: its state, and its COMMIT, which ends it (ASYNC1 E1b: the halt's
/// failure set the connection's state to none, whichever transaction the halting statement ended,
/// and the COMMIT then left the explicit transaction open).
#[test]
fn a_lent_writers_failed_deferred_key_leaves_the_explicit_transaction_as_it_was() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_writers_failed_deferred_key_leaves_the_explicit_transaction_as_it_was"
        ),
        || {
            let (db, conn, other) = two_connections("/db/lent-deferred-key");
            conn.execute("PRAGMA foreign_keys = ON").unwrap();
            conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute(
                "CREATE TABLE c (pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
            )
            .unwrap();
            let mut w = conn
                .prepare("INSERT INTO c VALUES (99) RETURNING pid")
                .unwrap();
            let returned = next_row(&mut w);
            let lent = w.atoll_txn_for_test().holds_lent();
            conn.execute("BEGIN").unwrap();
            let counted = count(&conn);
            let in_begin = (conn.get_tx_state(), ct(&conn).is_open(TxnOf::Connection));
            let ended = next_row(&mut w);
            let after = (conn.get_tx_state(), ct(&conn).is_open(TxnOf::Connection));
            drop(w);
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            let after_commit = (ct(&conn).is_open(TxnOf::Connection), conn.get_auto_commit());
            let children = text(&other, "SELECT count(*) FROM c");
            drop((conn, other, db));
            assert_eq!(returned, Ok(Some(99)));
            assert!(lent, "the writer was not lent a transaction");
            assert_eq!(counted, Ok(Some(0)));
            assert!(in_begin.1, "the explicit transaction's read is not open");
            assert!(
                ended
                    .as_ref()
                    .is_err_and(|e| e.contains("FOREIGN KEY constraint failed")),
                "the lent writer's halt: {ended:?}"
            );
            assert_eq!(
                after, in_begin,
                "(the connection's state, its own transaction open) after the lent writer's failure"
            );
            assert_eq!(committed, Ok(()));
            assert_eq!(
                after_commit,
                (false, true),
                "(its own transaction open, autocommit) after the COMMIT"
            );
            assert_eq!(children, Ok("0".to_string()), "the failed child row kept");
        },
    );
}

/// A virtual table whose rows and transactions a test reads: each begin, write, commit and
/// rollback of its transaction, in order. With `hold` set, its next insert waits on a completion
/// it leaves in `held` for the test to finish.
#[derive(Clone, Debug, Default)]
struct Recorded {
    rows: Arc<crate::storage::atoll::locks::Mutex<Vec<i64>>>,
    events: Arc<crate::storage::atoll::locks::Mutex<Vec<&'static str>>>,
    hold: Arc<std::sync::atomic::AtomicBool>,
    held: Arc<crate::storage::atoll::locks::Mutex<Option<crate::Completion>>>,
}

impl crate::native_ext::VirtualTableModule for Recorded {
    type Table = RecordedTable;

    fn schema(&self, _args: &[Value]) -> crate::Result<String> {
        Ok("CREATE TABLE x(value INTEGER)".into())
    }

    fn create(&self, _args: &[Value]) -> crate::Result<RecordedTable> {
        Ok(RecordedTable {
            recorded: self.clone(),
            before: None,
        })
    }
}

#[derive(Debug)]
struct RecordedTable {
    recorded: Recorded,
    /// The rows as its transaction began, for its rollback.
    before: Option<Vec<i64>>,
}

impl crate::native_ext::VirtualTable for RecordedTable {
    type Cursor = RecordedCursor;

    fn open(&self, _connection: Arc<Connection>) -> crate::Result<RecordedCursor> {
        Ok(RecordedCursor {
            rows: self.recorded.rows.lock().clone(),
            at: 0,
        })
    }

    fn best_index(
        &self,
        constraints: &[crate::native_ext::ConstraintInfo],
        _order_by: &[crate::native_ext::OrderByInfo],
    ) -> std::result::Result<crate::native_ext::IndexInfo, turso_ext::ResultCode> {
        Ok(crate::native_ext::IndexInfo {
            constraint_usages: constraints
                .iter()
                .map(|_| crate::native_ext::ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
            ..Default::default()
        })
    }

    fn readonly(&self) -> bool {
        false
    }

    fn create_update(
        &mut self,
        args: Vec<Value>,
    ) -> crate::Result<Box<dyn crate::native_ext::TableUpdate>> {
        Ok(Box::new(RecordedInsert {
            args,
            recorded: self.recorded.clone(),
            waiting: None,
        }))
    }

    fn begin(&mut self) -> crate::Result<()> {
        self.before = Some(self.recorded.rows.lock().clone());
        self.recorded.events.lock().push("begin");
        Ok(())
    }

    fn commit(&mut self) -> crate::Result<()> {
        self.before = None;
        self.recorded.events.lock().push("commit");
        Ok(())
    }

    fn rollback(&mut self) -> crate::Result<()> {
        if let Some(before) = self.before.take() {
            *self.recorded.rows.lock() = before;
        }
        self.recorded.events.lock().push("rollback");
        Ok(())
    }
}

/// A scan of the rows as its cursor opened.
struct RecordedCursor {
    rows: Vec<i64>,
    at: usize,
}

impl crate::native_ext::VirtualTableCursor for RecordedCursor {
    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> crate::types::IOResultOr<bool> {
        self.at = 0;
        Ok(crate::IOResult::Done(!self.rows.is_empty()))
    }

    fn next(&mut self) -> crate::types::IOResultOr<bool> {
        self.at += 1;
        Ok(crate::IOResult::Done(self.at < self.rows.len()))
    }

    fn column(&mut self, _column: usize) -> crate::types::IOResultOr<Value> {
        Ok(crate::IOResult::Done(Value::from_i64(self.rows[self.at])))
    }

    fn rowid(&self) -> i64 {
        self.at as i64 + 1
    }
}

/// An insert of one row: its arguments are the old rowid (none), the new one, then its value.
struct RecordedInsert {
    args: Vec<Value>,
    recorded: Recorded,
    /// The completion it waits on, held.
    waiting: Option<crate::Completion>,
}

impl crate::native_ext::TableUpdate for RecordedInsert {
    fn step(&mut self) -> crate::types::IOResultOr<Option<i64>> {
        if self
            .recorded
            .hold
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            let completion = crate::Completion::new_wait();
            *self.recorded.held.lock() = Some(completion.clone());
            self.waiting = Some(completion);
        }
        if let Some(waiting) = &self.waiting {
            if !waiting.finished() {
                return Ok(crate::IOResult::IO(crate::types::IOCompletions(
                    waiting.clone(),
                )));
            }
            self.waiting = None;
        }
        let crate::Value::Numeric(crate::Numeric::Integer(value)) = self.args[2] else {
            return Err(
                crate::LimboError::ExtensionError("an insert of one integer".into()).into(),
            );
        };
        let mut rows = self.recorded.rows.lock();
        rows.push(value);
        self.recorded.events.lock().push("write");
        Ok(crate::IOResult::Done(Some(rows.len() as i64)))
    }
}

/// The read served to a member at its transaction's COMMIT ends with the member, dropped or
/// stepped to its end, though a statement lent a transaction of its own ran meanwhile; the
/// connection's next statement that runs in its own transaction then reads the database as it is
/// (the auditor's #6067 D3).
#[test]
fn the_read_served_to_a_member_ends_with_it_after_a_lent_statement_ran() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::the_read_served_to_a_member_ends_with_it_after_a_lent_statement_ran"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-served-member");
            let mut seen = Vec::new();
            for (id, ended) in [(10, false), (11, true)] {
                conn.execute("BEGIN").unwrap();
                let mut m = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
                let first = next_row(&mut m);
                let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
                let served = (ct(&conn).is_open(TxnOf::Connection), conn.get_auto_commit());
                let written = conn
                    .execute(format!("INSERT INTO t VALUES ({id}, 'w')"))
                    .map_err(|e| e.to_string());
                let m_rest = if ended { Some(rest(&mut m)) } else { None };
                drop(m);
                let after = ct(&conn).is_open(TxnOf::Connection);
                let check = text(&conn, "PRAGMA integrity_check");
                seen.push((first, committed, served, written, m_rest, after, check));
            }
            drop((conn, other, db));
            let ok = || (Ok(Some(1)), Ok(()), (true, true), Ok(()));
            let (f, c, s, w, r, a, k) = &seen[0];
            assert_eq!((f.clone(), c.clone(), *s, w.clone()), ok(), "dropped");
            assert_eq!(*r, None);
            assert!(!a, "the read served to a dropped member was left open");
            assert_eq!(*k, Ok("ok".to_string()), "dropped");
            let (f, c, s, w, r, a, k) = &seen[1];
            assert_eq!((f.clone(), c.clone(), *s, w.clone()), ok(), "ended");
            assert_eq!(
                *r,
                Some(Ok(vec![2, 3, 10])),
                "the member did not read on in the read served to it"
            );
            assert!(
                !a,
                "the read served to a member stepped to its end was left open"
            );
            assert_eq!(*k, Ok("ok".to_string()), "ended");
        },
    );
}

/// A member reads on after a write-write conflict rolled back the transaction it read in: in a
/// read begun for it, which sees what the rollback left and ends with it (the auditor's #6067 D4).
#[test]
fn a_member_reads_on_after_a_conflict_rolls_its_transaction_back() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_member_reads_on_after_a_conflict_rolls_its_transaction_back"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-member-conflict");
            conn.execute("BEGIN").unwrap();
            let mut m = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut m);
            let other_wrote = other
                .execute("UPDATE t SET v = 'o' WHERE id = 2")
                .map_err(|e| e.to_string());
            let conflict = conn
                .execute("UPDATE t SET v = 'm' WHERE id = 2")
                .map_err(|e| e.to_string());
            let state = (conn.get_auto_commit(), ct(&conn).is_open(TxnOf::Connection));
            let m_rest = rest(&mut m);
            drop(m);
            let after = ct(&conn).is_open(TxnOf::Connection);
            let check = text(&conn, "PRAGMA integrity_check");
            let v = text(&other, "SELECT v FROM t WHERE id = 2");
            drop((conn, other, db));
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(other_wrote, Ok(()));
            assert!(
                conflict.as_ref().is_err_and(|e| e.contains("conflict")),
                "the second write of row 2 did not conflict: {conflict:?}"
            );
            assert_eq!(
                state,
                (true, true),
                "the conflict's rollback left the member no read"
            );
            assert_eq!(m_rest, Ok(vec![2, 3]), "the member did not read on");
            assert!(!after, "the read begun for the member was left open");
            assert_eq!(check, Ok("ok".to_string()));
            assert_eq!(v, Ok("o".to_string()));
        },
    );
}

/// A reader lent a transaction of its own leaves its connection's count of deferred foreign key
/// violations to the writer they belong to: one lent before BEGIN and ended inside it, and one run
/// whole beside a lent writer stopped at its first row.
#[test]
fn a_lent_reader_leaves_the_deferred_foreign_key_violations_to_their_writer() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_reader_leaves_the_deferred_foreign_key_violations_to_their_writer"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = open(&io, "/db/lent-deferred-fk");
            let conn = db.connect(ConnectOpts::new()).unwrap();
            for sql in [
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE p (id INTEGER PRIMARY KEY)",
                "CREATE TABLE c (pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
                "INSERT INTO p VALUES (1), (2), (3)",
            ] {
                conn.execute(sql).unwrap();
            }
            let mut r = conn.prepare("SELECT id FROM p ORDER BY id").unwrap();
            let first = next_row(&mut r);
            conn.execute("BEGIN").unwrap();
            let orphan = conn
                .execute("INSERT INTO c VALUES (99)")
                .map_err(|e| e.to_string());
            let r_rest = rest(&mut r);
            drop(r);
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            let rolled_back = conn.execute("ROLLBACK").map_err(|e| e.to_string());
            let mut w = conn
                .prepare("INSERT INTO c VALUES (98) RETURNING pid")
                .unwrap();
            let w_first = next_row(&mut w);
            let read = text(&conn, "SELECT count(*) FROM p");
            let w_end = run_to_end(&mut w);
            drop(w);
            let orphans = text(&conn, "SELECT count(*) FROM c");
            drop((conn, db));
            assert_eq!(first, Ok(Some(1)));
            assert_eq!(orphan, Ok(()));
            assert_eq!(
                r_rest,
                Ok(vec![2, 3]),
                "the reader ended inside BEGIN failed"
            );
            assert!(
                committed.as_ref().is_err_and(|e| e.contains("FOREIGN KEY")),
                "COMMIT did not meet the explicit transaction's violation: {committed:?}"
            );
            assert_eq!(rolled_back, Ok(()));
            assert_eq!(w_first, Ok(Some(98)));
            assert_eq!(
                read,
                Ok("3".to_string()),
                "the reader beside the writer failed"
            );
            assert!(
                w_end.contains("FOREIGN KEY"),
                "the writer did not meet its own violation: {w_end}"
            );
            assert_eq!(orphans, Ok("0".to_string()));
        },
    );
}

/// A reader of the connection run whole at every stop of a schema change the connection runs in
/// autocommit in one-unit frames: before the change has written the schema the reader is lent a
/// transaction of its own; after, no snapshot of its own has the schema it was prepared with, so it
/// reads in the change's transaction, or is refused while that transaction ends (the auditor's
/// #6067 D2, parked schedule 26).
#[test]
fn a_reader_beside_its_connections_uncommitted_schema_change_reads_in_that_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_beside_its_connections_uncommitted_schema_change_reads_in_that_transaction"
        ),
        || {
            let (db, conn, other) = three_rows_with_worker("/db/lent-beside-ddl");
            let mut x = conn
                .prepare("CREATE TABLE c (k INTEGER PRIMARY KEY)")
                .unwrap();
            let (mut stops, mut lent, mut member, mut refused) = (0usize, 0usize, 0usize, 0usize);
            let mut wrong = Vec::new();
            let end = loop {
                let stepped = {
                    let _frame = crate::storage::atoll::cooperative::Frame::with_budget(1);
                    x.step_in_frame(std::task::Waker::noop())
                };
                let waits = match stepped {
                    Ok(crate::StepResult::IO) => true,
                    Ok(crate::StepResult::Yield) => false,
                    Ok(other) => break format!("{other:?}"),
                    Err(e) => break format!("error: {e}"),
                };
                stops += 1;
                match conn.prepare("SELECT count(*) FROM t") {
                    Ok(mut r) => match next_row(&mut r) {
                        Ok(Some(3)) if r.atoll_txn_for_test().holds_lent() => lent += 1,
                        Ok(Some(3)) => member += 1,
                        Err(e) if e.contains("ending the connection's transaction") => refused += 1,
                        got => wrong.push((stops, format!("{got:?}"))),
                    },
                    Err(e) => wrong.push((stops, format!("prepare: {e}"))),
                }
                if waits {
                    x._io().step().unwrap();
                }
                if stops == 100_000 {
                    break "not done in 100000 stops".to_string();
                }
            };
            drop(x);
            let made = text(
                &other,
                "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
            );
            drop((conn, other, db));
            assert_eq!(end, "Done");
            assert!(wrong.is_empty(), "reads beside the change: {wrong:?}");
            assert!(
                lent > 0 && member > 0,
                "of {stops} stops: lent {lent}, read in the change's transaction {member}, refused {refused}"
            );
            assert_eq!(made, Ok("1".to_string()));
        },
    );
}

/// [`three_rows`], opened with its writes queued and a worker stepping its checkpoint's task, as a
/// statement stepped in cooperative frames needs.
fn three_rows_with_worker(path: &str) -> (Arc<Database>, Arc<Connection>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut cfg = config();
    cfg.checkpointing = Checkpointing::Thread;
    let db = crate::storage::atoll::requests::opened_async(|| {
        Database::open(
            io.clone(),
            path,
            OpenOptions::new(Arc::new(SqliteDialect)).atoll(cfg),
        )
        .unwrap()
    });
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    (db, conn, other)
}

/// A writer lent a transaction, parked at its take behind another connection's schema change with
/// a reader of its connection run beside it, runs again once the change is committed: its
/// transaction is its own, so it begins again under the new schema, and its row is committed (the
/// auditor's #6067, parked schedule 20).
#[test]
fn a_writer_parked_behind_a_schema_change_begins_again_and_commits_its_row() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_parked_behind_a_schema_change_begins_again_and_commits_its_row"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-parked-behind-ddl");
            conn.set_busy_timeout(std::time::Duration::from_secs(10));
            other.execute("BEGIN").unwrap();
            other
                .execute("CREATE TABLE z (k INTEGER PRIMARY KEY)")
                .unwrap();
            let mut x = conn.prepare("INSERT INTO t VALUES (6000, 'x')").unwrap();
            let once = loop {
                match x.step() {
                    Ok(crate::StepResult::IO) => break "IO".to_string(),
                    Ok(crate::StepResult::Yield) => {}
                    other => break format!("{other:?}"),
                }
            };
            let beside = count(&conn);
            let committed = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = run_to_end(&mut x);
            drop(x);
            let seen = text(&other, "SELECT count(*) FROM t WHERE id = 6000");
            let schema = text(&conn, "SELECT count(*) FROM z");
            drop((conn, other, db));
            assert_eq!(
                once, "IO",
                "the writer was not parked behind the schema change"
            );
            assert_eq!(beside, Ok(Some(3)));
            assert_eq!(committed, Ok(()));
            assert_eq!(finished, "Done");
            assert_eq!(seen, Ok("1".to_string()), "the writer's row is not there");
            assert_eq!(schema, Ok("0".to_string()));
        },
    );
}

/// A writer lent a transaction and parked at its take, stepped again after its connection began
/// an explicit transaction, keeps its own transaction to its halt and commits its row there: it
/// began in autocommit, and its begun transaction is not the explicit one's to take in.
#[test]
fn a_writer_parked_at_its_take_keeps_its_lent_transaction_across_a_begin() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_parked_at_its_take_keeps_its_lent_transaction_across_a_begin"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-parked-begin");
            conn.set_busy_timeout(std::time::Duration::from_secs(10));
            let held = other.execute("BEGIN IMMEDIATE").map_err(|e| e.to_string());
            let mut x = conn.prepare("INSERT INTO t VALUES (9, 'x')").unwrap();
            let once = loop {
                match x.step() {
                    Ok(crate::StepResult::IO) => break "IO".to_string(),
                    Ok(crate::StepResult::Yield) => {}
                    other => break format!("{other:?}"),
                }
            };
            let begun = conn.execute("BEGIN").map_err(|e| e.to_string());
            let released = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = run_to_end(&mut x);
            let after = (
                x.atoll_txn_for_test().holds_lent(),
                ct(&conn).is_open(TxnOf::Statement(x.atoll_txn_for_test())),
                conn.get_auto_commit(),
            );
            drop(x);
            let rolled_back = conn.execute("ROLLBACK").map_err(|e| e.to_string());
            let seen = count(&other);
            drop((conn, other, db));
            assert_eq!(held, Ok(()));
            assert_eq!(once, "IO", "the writer was not parked at its take");
            assert_eq!(begun, Ok(()));
            assert_eq!(released, Ok(()));
            assert_eq!(finished, "Done");
            assert_eq!(
                after,
                (true, false, false),
                "the writer gave its lent transaction back, or left it open"
            );
            assert_eq!(rolled_back, Ok(()));
            assert_eq!(
                seen,
                Ok(Some(4)),
                "the writer's row, committed at its halt, is not there"
            );
        },
    );
}

/// A writer lent a transaction and parked at its take is its connection's one registered writer:
/// inside a BEGIN begun after it, a write of the connection's own transaction is refused, and so
/// are its COMMIT and its ROLLBACK, which would end the transaction under the lent one's
/// deferred-key count. Once the lent writer is done, the explicit transaction writes and commits
/// (one top-level writer a connection, ONE-TXN rule 1, with a lent writer on one side).
#[test]
fn a_lent_writer_parked_at_its_take_keeps_its_connections_own_writers_and_ending_out() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_writer_parked_at_its_take_keeps_its_connections_own_writers_and_ending_out"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-writer-keeps-own-out");
            conn.set_busy_timeout(std::time::Duration::from_secs(10));
            let held = other.execute("BEGIN IMMEDIATE").map_err(|e| e.to_string());
            let mut x = conn.prepare("INSERT INTO t VALUES (9, 'lent')").unwrap();
            let parked = loop {
                match x.step() {
                    Ok(crate::StepResult::IO) => break "IO".to_string(),
                    Ok(crate::StepResult::Yield) => {}
                    other => break format!("{other:?}"),
                }
            };
            let lent = x.atoll_txn_for_test().holds_lent();
            let begun = conn.execute("BEGIN").map_err(|e| e.to_string());
            let member = conn
                .execute("INSERT INTO t VALUES (10, 'member')")
                .map_err(|e| e.to_string());
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            let rolled_back = conn.execute("ROLLBACK").map_err(|e| e.to_string());
            let released = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = run_to_end(&mut x);
            drop(x);
            let after = conn
                .execute("INSERT INTO t VALUES (10, 'member')")
                .map_err(|e| e.to_string());
            let ended = conn.execute("COMMIT").map_err(|e| e.to_string());
            let seen = text(&other, "SELECT group_concat(id) FROM t ORDER BY id");
            drop((conn, other, db));
            assert_eq!(held, Ok(()));
            assert_eq!(parked, "IO", "the lent writer was not parked at its take");
            assert!(lent, "the writer was not lent a transaction");
            assert_eq!(begun, Ok(()));
            assert!(
                member
                    .as_ref()
                    .is_err_and(|e| e.contains("cannot start a write statement")),
                "a write of the connection's own beside the lent writer: {member:?}"
            );
            assert!(
                committed
                    .as_ref()
                    .is_err_and(|e| e.contains("cannot commit transaction")),
                "a COMMIT beside the lent writer: {committed:?}"
            );
            assert!(
                rolled_back
                    .as_ref()
                    .is_err_and(|e| e.contains("cannot rollback transaction")),
                "a ROLLBACK beside the lent writer: {rolled_back:?}"
            );
            assert_eq!(released, Ok(()));
            assert_eq!(finished, "Done");
            assert_eq!(after, Ok(()));
            assert_eq!(ended, Ok(()));
            assert_eq!(seen, Ok("1,2,3,9,10".to_string()), "the rows committed");
        },
    );
}

/// A statement that reads or writes an attached database, captures its changes, or runs while the
/// schema has a materialized view runs in its connection's own transaction, whose state each
/// keeps on the connection; a writer of the main database alone is lent one (ASYNC1 E1b). Each
/// does its work: the rows read and written, the change captured, the view's counts.
#[test]
fn a_statement_of_an_attached_database_a_kept_view_or_change_capture_is_lent_nothing() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_statement_of_an_attached_database_a_kept_view_or_change_capture_is_lent_nothing"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open(
                io.clone(),
                "/db/lent-kinds",
                OpenOptions::new(Arc::new(SqliteDialect))
                    .atoll(config())
                    .db_opts(DatabaseOpts::default().with_attach(true).with_views(true)),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let set = [
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "ATTACH '/db/lent-kinds-aux' AS aux",
                "CREATE TABLE aux.side (b INTEGER)",
                "INSERT INTO aux.side VALUES (7)",
                "CREATE TABLE g (k TEXT, n INTEGER)",
                "CREATE TABLE turso_cdc(change_id INTEGER PRIMARY KEY AUTOINCREMENT, \
                 change_time INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, \
                 after BLOB, updates BLOB)",
            ]
            .map(|sql| conn.execute(sql).map_err(|e| e.to_string()));
            // Runs `sql` to its end: how it ended, and whether it was lent a transaction.
            let run = |sql: &str| {
                let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
                let ended = run_to_end(&mut stmt);
                Ok::<_, String>((ended, stmt.atoll_txn_for_test().holds_lent()))
            };
            let main_writer = run("INSERT INTO t VALUES (4, 'd')");
            let attached_reader = run("SELECT count(*) FROM t, aux.side");
            let attached_writer = run("INSERT INTO aux.side SELECT id FROM t WHERE id < 3");
            let capture_on = conn
                .execute("PRAGMA capture_data_changes_conn('full')")
                .map_err(|e| e.to_string());
            let capturing = run("INSERT INTO t VALUES (5, 'e')");
            let capture_off = conn
                .execute("PRAGMA capture_data_changes_conn('off')")
                .map_err(|e| e.to_string());
            let view_made = conn
                .execute("CREATE MATERIALIZED VIEW gv AS SELECT k, count(*) AS c FROM g GROUP BY k")
                .map_err(|e| e.to_string());
            let view_keeper = run("INSERT INTO g VALUES ('x', 1), ('x', 2)");
            let state = (conn.get_auto_commit(), ct(&conn).is_open(TxnOf::Connection));
            let seen = [
                text(&conn, "SELECT group_concat(id) FROM t"),
                text(&conn, "SELECT group_concat(b) FROM aux.side"),
                text(&conn, "SELECT k || ':' || c FROM gv"),
                text(
                    &conn,
                    "SELECT group_concat(table_name || ':' || id) FROM turso_cdc \
                     WHERE table_name = 't'",
                ),
            ];
            drop((conn, db));
            assert_eq!(set.map(|r| r.map(drop)), [const { Ok(()) }; 7]);
            let ran = |lent| Ok(("Done".to_string(), lent));
            assert_eq!(
                main_writer,
                ran(true),
                "a writer of the main database alone was not lent a transaction"
            );
            assert_eq!(
                attached_reader,
                ran(false),
                "a reader of an attached database was lent a transaction"
            );
            assert_eq!(
                attached_writer,
                ran(false),
                "a writer of an attached database was lent a transaction"
            );
            assert_eq!(capture_on, Ok(()));
            assert_eq!(
                capturing,
                ran(false),
                "a writer that captures its changes was lent a transaction"
            );
            assert_eq!((capture_off, view_made), (Ok(()), Ok(())));
            assert_eq!(
                view_keeper,
                ran(false),
                "a writer beside a materialized view was lent a transaction"
            );
            assert_eq!(
                state,
                (true, false),
                "(autocommit, the connection's own transaction open) after the statements"
            );
            assert_eq!(
                seen,
                [
                    Ok("1,2,3,4,5".to_string()),
                    Ok("7,1,2".to_string()),
                    Ok("x:2".to_string()),
                    Ok("t:5".to_string()),
                ],
                "the rows, the attached rows, the view's count and the change captured"
            );
        },
    );
}

/// A statement that writes the schema (a whole-table DELETE clears its tree, a DROP TABLE), one
/// that checks pages, and one that begins or ends a transaction run in their connection's own
/// transaction, as they are prepared; a writer of a table's rows lends (ASYNC1 E1b). Run in
/// autocommit on a second database, each does its work in the connection's own transaction.
#[test]
fn a_schema_writer_a_page_check_and_a_transactions_begin_run_in_their_connections_own() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_schema_writer_a_page_check_and_a_transactions_begin_run_in_their_connections_own"
        ),
        || {
            let made = |path: &str| {
                let (db, conn, other) = three_rows(path);
                let set = [
                    "CREATE TABLE c (a)",
                    "INSERT INTO c VALUES (1), (2)",
                    "CREATE TABLE d (a)",
                ]
                .map(|sql| conn.execute(sql).map_err(|e| e.to_string()));
                (db, conn, other, set)
            };
            let sqls = [
                "INSERT INTO t VALUES (4, 'd')",
                "DELETE FROM c",
                "DROP TABLE d",
                "PRAGMA integrity_check",
                "BEGIN IMMEDIATE",
            ];
            // Whether each lends in autocommit, as prepared; asserted before any of them runs.
            let (db, conn, other, set) = made("/db/lent-schema-pages-begin");
            let lends = sqls.map(|sql| {
                conn.prepare(sql)
                    .map(|stmt| stmt.program.prepared().lends_in_autocommit)
                    .map_err(|e| e.to_string())
            });
            drop((conn, other, db));
            assert_eq!(set, [const { Ok(()) }; 3]);
            let [writer, clear, drop_table, page_check, begin] = lends;
            assert_eq!(writer, Ok(true), "a writer of a table's rows does not lend");
            assert_eq!(
                clear,
                Ok(false),
                "a whole-table DELETE, which clears its tree, lends"
            );
            assert_eq!(drop_table, Ok(false), "a DROP TABLE lends");
            assert_eq!(page_check, Ok(false), "a page check lends");
            assert_eq!(begin, Ok(false), "a BEGIN IMMEDIATE lends");
            // Each run to its end in autocommit, then whether it was lent a transaction.
            let (db, conn, other, set) = made("/db/lent-schema-pages-begin-run");
            let mut ran = Vec::new();
            for sql in sqls
                .iter()
                .chain(&["INSERT INTO t VALUES (5, 'e')", "COMMIT"])
            {
                ran.push(
                    conn.prepare(*sql)
                        .map_err(|e| e.to_string())
                        .map(|mut stmt| {
                            (
                                run_to_end(&mut stmt),
                                stmt.atoll_txn_for_test().holds_lent(),
                            )
                        }),
                );
            }
            let seen = [
                text(&other, "SELECT group_concat(id) FROM t"),
                text(&other, "SELECT count(*) FROM c"),
                text(
                    &other,
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'd'",
                ),
            ];
            drop((conn, other, db));
            assert_eq!(set, [const { Ok(()) }; 3]);
            let done = |lent| Ok(("Done".to_string(), lent));
            assert_eq!(
                ran,
                [true, false, false, false, false, false, false].map(done),
                "how each ended, and whether it was lent a transaction"
            );
            assert_eq!(
                seen,
                [
                    Ok("1,2,3,4,5".to_string()),
                    Ok("0".to_string()),
                    Ok("0".to_string()),
                ],
                "the rows, the cleared table, the dropped table"
            );
        },
    );
}

/// A lent writer that meets a write-write conflict while two members of its connection read on
/// in its connection's own transaction is run again with a new snapshot, as a sole autocommit
/// writer is: its transaction is its own, whatever the members hold (ASYNC1 E1b). It ends Done
/// with its row written, and the members read on to their ends.
#[test]
fn a_lent_writer_that_meets_a_conflict_beside_two_members_is_run_again() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_writer_that_meets_a_conflict_beside_two_members_is_run_again"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-conflict-members");
            let member_sql =
                "SELECT t.id FROM t, pragma_table_info('t') WHERE cid = 0 ORDER BY t.id";
            let mut first = conn.prepare(member_sql).unwrap();
            let mut second = conn.prepare(member_sql).unwrap();
            let firsts = (next_row(&mut first), next_row(&mut second));
            let members = conn.atoll_members();
            let began = other.execute("BEGIN").map_err(|e| e.to_string());
            let held = other
                .execute("UPDATE t SET v = 'other' WHERE id = 1")
                .map_err(|e| e.to_string());
            conn.set_busy_timeout(std::time::Duration::from_secs(30));
            let mut x = conn
                .prepare("UPDATE t SET v = 'mine' WHERE id = 1")
                .unwrap();
            let waiting = loop {
                match x.step() {
                    Ok(crate::StepResult::IO) => break "IO".to_string(),
                    Ok(crate::StepResult::Yield) => {}
                    other => break format!("{other:?}"),
                }
            };
            let lent = x.atoll_txn_for_test().holds_lent();
            let committed = other.execute("COMMIT").map_err(|e| e.to_string());
            let finished = run_to_end(&mut x);
            drop(x);
            let rests = (rest(&mut first), rest(&mut second));
            drop((first, second));
            let seen = text(&other, "SELECT v FROM t WHERE id = 1");
            drop((conn, other, db));
            assert_eq!(firsts, (Ok(Some(1)), Ok(Some(1))));
            assert_eq!(members, 2, "the two readers are not members");
            assert_eq!((began, held), (Ok(()), Ok(())));
            assert_eq!(waiting, "IO", "the writer did not wait for the other's row");
            assert!(lent, "the writer was not lent a transaction");
            assert_eq!(committed, Ok(()));
            assert_eq!(
                finished, "Done",
                "a lent writer beside two members was not run again after its conflict"
            );
            assert_eq!(rests, (Ok(vec![2, 3]), Ok(vec![2, 3])));
            assert_eq!(seen, Ok("mine".to_string()));
        },
    );
}

/// A lent writer's halt sets its connection's change counts, as a writer in its connection's own
/// transaction does: `changes()` reads the rows of the last write, `total_changes()` every row
/// written since the connection opened (ASYNC1 E1b).
#[test]
fn a_lent_writers_halt_sets_its_connections_change_counts() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_writers_halt_sets_its_connections_change_counts"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-change-counts");
            let mut x = conn
                .prepare("INSERT INTO t VALUES (4, 'd'), (5, 'e')")
                .unwrap();
            let finished = run_to_end(&mut x);
            let lent = x.atoll_txn_for_test().holds_lent();
            drop(x);
            let after_insert = text(&conn, "SELECT changes() || ',' || total_changes()");
            let deleted = conn
                .execute("DELETE FROM t WHERE id > 2")
                .map_err(|e| e.to_string());
            let after_delete = text(&conn, "SELECT changes() || ',' || total_changes()");
            drop((conn, other, db));
            assert_eq!(finished, "Done");
            assert!(lent, "the writer was not lent a transaction");
            assert_eq!(
                after_insert,
                Ok("2,5".to_string()),
                "(changes, total changes) after a lent writer's halt"
            );
            assert_eq!(deleted, Ok(()));
            assert_eq!(
                after_delete,
                Ok("3,8".to_string()),
                "(changes, total changes) after a lent delete's halt"
            );
        },
    );
}

/// A lent reader begun while a member reads on in its connection's own transaction, reset at its
/// first row, ends its lent transaction there: a lent statement's reset ends what it was lent,
/// whatever its connection's state says (ASYNC1 E1b). Run again, it reads every row, and the
/// member reads on to its end.
#[test]
fn a_lent_reader_begun_beside_a_member_and_reset_at_its_first_row_ends_its_transaction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_reader_begun_beside_a_member_and_reset_at_its_first_row_ends_its_transaction"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-reset-beside-member");
            let mut member = conn
                .prepare("SELECT t.id FROM t, pragma_table_info('t') WHERE cid = 0 ORDER BY t.id")
                .unwrap();
            let member_first = next_row(&mut member);
            let state = conn.get_tx_state();
            let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let first = next_row(&mut r);
            let lent = r.atoll_txn_for_test().holds_lent();
            let reset = r.reset().map_err(|e| e.to_string());
            let open_after_reset = ct(&conn).is_open(TxnOf::Statement(r.atoll_txn_for_test()));
            let again = rest(&mut r);
            drop(r);
            let member_rest = rest(&mut member);
            drop(member);
            drop((conn, other, db));
            assert_eq!(member_first, Ok(Some(1)));
            assert_eq!(
                state,
                crate::TransactionState::Read,
                "the member began no read"
            );
            assert_eq!(first, Ok(Some(1)));
            assert!(lent, "the reader was not lent a transaction");
            assert_eq!(reset, Ok(()));
            assert!(
                !open_after_reset,
                "a lent reader reset at its first row left its transaction open"
            );
            assert_eq!(again, Ok(vec![1, 2, 3]));
            assert_eq!(member_rest, Ok(vec![2, 3]));
        },
    );
}

/// A reader lent a transaction, open at its first row while its connection commits a schema
/// change in autocommit, reads its own snapshot to its end: the schema it was prepared with and
/// the rows as of its begin. Its next run is prepared again under the committed schema
/// (the auditor's #6069 item 2).
#[test]
fn a_reader_lent_before_its_connections_schema_change_reads_its_snapshot_to_its_end() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_lent_before_its_connections_schema_change_reads_its_snapshot_to_its_end"
        ),
        || {
            let mut seen = Vec::new();
            for (i, ddl) in [
                "CREATE TABLE c (k INTEGER PRIMARY KEY)",
                "ALTER TABLE t ADD COLUMN w TEXT",
                "CREATE INDEX t_v ON t(v)",
                "DROP TABLE t",
                "ALTER TABLE t RENAME TO t2",
            ]
            .into_iter()
            .enumerate()
            {
                let (db, conn, other) = three_rows(&format!("/db/lent-before-ddl-{i}"));
                let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
                let first = next_row(&mut r);
                let lent = r.atoll_txn_for_test().holds_lent();
                let ran = conn.execute(ddl).map_err(|e| e.to_string());
                let r_rest = rest(&mut r);
                r.reset().unwrap();
                let again = rest(&mut r);
                drop(r);
                let check = text(&conn, "PRAGMA integrity_check");
                drop((conn, other, db));
                seen.push((ddl, first, lent, ran, r_rest, again, check));
            }
            for (ddl, first, lent, ran, r_rest, again, check) in seen {
                assert_eq!((first, lent, ran), (Ok(Some(1)), true, Ok(())), "{ddl}");
                assert_eq!(r_rest, Ok(vec![2, 3]), "{ddl}: the reader's rest");
                let gone = ddl.starts_with("DROP") || ddl.contains("RENAME");
                if gone {
                    assert_eq!(
                        again,
                        Err("Parse error: no such table: t".to_string()),
                        "{ddl}: the next run"
                    );
                } else {
                    assert_eq!(again, Ok(vec![1, 2, 3]), "{ddl}: the next run");
                }
                assert_eq!(check, Ok("ok".to_string()), "{ddl}");
            }
        },
    );
}

/// A reader lent a transaction before its connection's schema change, stepped a row at each stop
/// of that change in one-unit frames while it has rows to spare and always at the first stop after
/// the change has written the schema, then after the change has committed: it returns its
/// snapshot's rows, all of them in order, in its own transaction throughout (the auditor's #6069
/// item 2). How many stops the change takes depends on the checkpoint thread, so the reader keeps
/// rows back for the stops that matter.
#[test]
fn a_reader_lent_before_an_uncommitted_schema_change_reads_its_snapshot_through_it() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_lent_before_an_uncommitted_schema_change_reads_its_snapshot_through_it"
        ),
        || {
            let (db, conn, other) = three_rows_with_worker("/db/lent-before-uncommitted-ddl");
            conn.execute("INSERT INTO t SELECT value, 'r' FROM generate_series(4, 400)")
                .unwrap();
            let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
            let mut read = vec![next_row(&mut r)];
            let mut lent = r.atoll_txn_for_test().holds_lent();
            let mut x = conn
                .prepare("CREATE TABLE c (k INTEGER PRIMARY KEY)")
                .unwrap();
            let (mut stops, mut changed_at) = (0usize, None);
            let end = loop {
                let stepped = {
                    let _frame = crate::storage::atoll::cooperative::Frame::with_budget(1);
                    x.step_in_frame(std::task::Waker::noop())
                };
                let waits = match stepped {
                    Ok(crate::StepResult::IO) => true,
                    Ok(crate::StepResult::Yield) => false,
                    Ok(other) => break format!("{other:?}"),
                    Err(e) => break format!("error: {e}"),
                };
                stops += 1;
                let changed_now = changed_at.is_none()
                    && matches!(
                        conn.get_tx_state(),
                        crate::connection::TransactionState::Write {
                            schema_did_change: true
                        }
                    );
                if changed_now {
                    changed_at = Some(read.len());
                }
                // Two rows kept back: one for the stop after the schema write, one after the commit.
                if changed_now || read.len() + 2 < 400 {
                    read.push(next_row(&mut r));
                    lent &= r.atoll_txn_for_test().holds_lent();
                }
                if waits {
                    x._io().step().unwrap();
                }
                if stops == 100_000 {
                    break "not done in 100000 stops".to_string();
                }
            };
            drop(x);
            let read_before_commit = read.len();
            let r_rest = rest(&mut r);
            drop(r);
            let made = text(
                &other,
                "SELECT count(*) FROM sqlite_schema WHERE name = 'c'",
            );
            drop((conn, other, db));
            assert_eq!(end, "Done");
            let changed_at = changed_at.expect("no stop came after the schema write");
            assert!(
                changed_at < read_before_commit,
                "no row was read between the schema write and the commit"
            );
            assert!(lent, "the reader left its own transaction");
            let all: Vec<i64> = read
                .into_iter()
                .map(|row| row.unwrap().unwrap())
                .chain(r_rest.unwrap())
                .collect();
            assert_eq!(all, (1..=400).collect::<Vec<_>>(), "the reader's rows");
            assert_eq!(made, Ok("1".to_string()));
        },
    );
}

/// Each row's two columns left in `stmt`, an integer and a text.
fn pairs(stmt: &mut crate::Statement) -> Result<Vec<(i64, String)>, String> {
    let mut out = Vec::new();
    loop {
        match stmt.step().map_err(|e| e.to_string())? {
            crate::StepResult::IO => stmt._io().step().map_err(|e| e.to_string())?,
            crate::StepResult::Row => {
                let row = stmt.row().expect("a row");
                let id = match row.get_value(0) {
                    crate::Value::Numeric(crate::Numeric::Integer(i)) => *i,
                    other => return Err(format!("not an integer: {other:?}")),
                };
                out.push((id, row.get_value(1).to_string()));
            }
            crate::StepResult::Done => return Ok(out),
            crate::StepResult::Yield => stmt._io().step().map_err(|e| e.to_string())?,
            other => return Err(format!("{other:?}")),
        }
    }
}

/// The connection's own DROP TABLE is not refused while its lent reader of that table is open (a
/// difference from SQLite, documented with the read rule): the reader reads its snapshot to its
/// end, and the dropped table's pages are not reused under it, through the drop's commit, two
/// checkpoints and another table's inserts that need pages (the auditor's #6071 item 2).
#[test]
fn a_table_dropped_under_its_lent_reader_keeps_its_pages_until_the_reader_ends() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_table_dropped_under_its_lent_reader_keeps_its_pages_until_the_reader_ends"
        ),
        || {
            let (db, conn, other) = two_connections("/db/lent-drop-under-reader");
            conn.execute(
                "INSERT INTO t SELECT value, printf('%0200d', value) FROM generate_series(1, 2000)",
            )
            .unwrap();
            let mut r = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
            let mut read = Vec::new();
            match r.step() {
                Ok(crate::StepResult::Row) => {
                    let row = r.row().unwrap();
                    read.push((
                        match row.get_value(0) {
                            crate::Value::Numeric(crate::Numeric::Integer(i)) => *i,
                            _ => -1,
                        },
                        row.get_value(1).to_string(),
                    ));
                }
                other => panic!("the reader's first step: {other:?}"),
            }
            let lent = r.atoll_txn_for_test().holds_lent();
            let dropped = conn.execute("DROP TABLE t").map_err(|e| e.to_string());
            let checkpoints = (
                text(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").map(|_| ()),
                text(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").map(|_| ()),
            );
            let filled = conn
                .execute("CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)")
                .and_then(|()| {
                    conn.execute("INSERT INTO u SELECT value, printf('%0200d', value + 5000) FROM generate_series(1, 2000)")
                })
                .map_err(|e| e.to_string());
            let r_rest = pairs(&mut r);
            drop(r);
            let check = text(&conn, "PRAGMA integrity_check");
            let u_rows = text(&other, "SELECT count(*) FROM u");
            drop((conn, other, db));
            assert!(lent);
            assert_eq!(
                dropped,
                Ok(()),
                "the connection's DROP TABLE beside its open reader"
            );
            assert_eq!(checkpoints, (Ok(()), Ok(())));
            assert_eq!(filled, Ok(()));
            read.extend(r_rest.unwrap());
            let wrong: Vec<_> = read
                .iter()
                .filter(|(id, v)| *v != format!("{id:0200}"))
                .take(3)
                .collect();
            assert_eq!(read.len(), 2000, "the reader's rows after the drop");
            assert!(
                wrong.is_empty(),
                "rows the reader read from reused pages: {wrong:?}"
            );
            assert_eq!(check, Ok("ok".to_string()));
            assert_eq!(u_rows, Ok("2000".to_string()));
        },
    );
}

/// Every place a root statement's run becomes or stops being a member of its connection's own
/// transaction (ASYNC1 E1b, #6065 C3): its run's kind is decided as the run starts, and the root
/// count's one read-modify-write carries it; the release takes the kind back with the count's
/// other; the Transaction instruction reads it to lend or give back, and nothing else writes it.
#[test]
fn a_run_becomes_a_member_with_its_root_count_and_stops_with_it() {
    let read =
        |p: &str| std::fs::read_to_string(format!("{}/{p}", env!("CARGO_MANIFEST_DIR"))).unwrap();
    let mut sites = Vec::new();
    for file in [
        "connection.rs",
        "statement.rs",
        "vdbe/execute.rs",
        "vdbe/mod.rs",
        "storage/atoll/sql/mode.rs",
    ] {
        for line in read(file).lines() {
            let line = line.trim();
            if line.starts_with("//") {
                continue;
            }
            if line.contains("atoll_run") || line.contains("root_count(") {
                sites.push(format!("{file}: {line}"));
            }
        }
    }
    let want = [
        "connection.rs: atoll_run: crate::vdbe::AtollRun,",
        "connection.rs: .fetch_add(atoll_run.root_count(), Ordering::SeqCst);",
        "statement.rs: self.state.atoll_run = vdbe::AtollRun::None;",
        "statement.rs: roots.fetch_sub(vdbe::AtollRun::Member.root_count(), Ordering::SeqCst);",
        "statement.rs: let run = self.atoll_run_this_run();",
        "statement.rs: self.state.atoll_run = run;",
        "statement.rs: fn atoll_run_this_run(&self) -> vdbe::AtollRun {",
        "vdbe/execute.rs: let in_lent = match state.atoll_run {",
        "vdbe/mod.rs: pub(crate) fn root_count(self) -> i64 {",
        "vdbe/mod.rs: pub(crate) atoll_run: AtollRun,",
        "vdbe/mod.rs: atoll_run: AtollRun::None,",
        "vdbe/mod.rs: self.atoll_run == AtollRun::Member",
    ];
    assert_eq!(
        sites,
        want.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        "the places a run's kind and the root count are written or read"
    );
}

/// Each open statement of a connection holds a transaction slot of its own (ASYNC1 E1b, #6065 C4):
/// with the database's 4 slots held by one connection's 4 open readers, its 5th statement and
/// another connection's are each Busy once the busy bound has passed, with no other wait between
/// the connection's transactions; a reader dropped frees its slot for the next.
#[test]
fn a_connection_with_more_open_statements_than_slots_meets_busy_at_the_bound() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_connection_with_more_open_statements_than_slots_meets_busy_at_the_bound"
        ),
        || {
            let (db, conn, other) = three_rows("/db/lent-slots");
            conn.set_busy_timeout(std::time::Duration::from_millis(50));
            other.set_busy_timeout(std::time::Duration::from_millis(50));
            let mut open = Vec::new();
            let mut firsts = Vec::new();
            for _ in 0..4 {
                let mut r = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
                firsts.push(next_row(&mut r));
                open.push(r);
            }
            let fifth = count(&conn);
            let others = count(&other);
            drop(open.pop());
            let after = count(&conn);
            let others_after = count(&other);
            drop(open);
            drop((conn, other, db));
            assert_eq!(firsts, vec![Ok(Some(1)); 4]);
            assert_eq!(
                fifth,
                Err("Busy".to_string()),
                "the connection's 5th statement"
            );
            assert_eq!(
                others,
                Err("Busy".to_string()),
                "another connection's statement"
            );
            assert_eq!(after, Ok(Some(3)), "a statement once a slot was let go");
            assert_eq!(others_after, Ok(Some(3)));
        },
    );
}

/// A close of a connection whose autocommit INSERT, in a transaction lent to it, is at its
/// ending is refused with the lent ending's words and leaves the connection open; the statement,
/// dropped, finishes its commit, another connection reads the row, and the close then goes ahead
/// (the auditor's #6176 F1 (a); mutants close-ignores-lent-endings, lent-ending-never-uncounted).
#[test]
fn a_close_at_a_lent_endings_wait_is_refused_until_its_statement_is_dropped() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_close_at_a_lent_endings_wait_is_refused_until_its_statement_is_dropped"
        ),
        || {
            use crate::sync::atomic::Ordering;
            let crash = Arc::new(crate::io::crash::CrashIO::new(681));
            let io: Arc<dyn IO> = crash.clone();
            let db = open(&io, "/db/lent-ending-close");
            let writer = db.connect(ConnectOpts::new()).unwrap();
            let reader = db.connect(ConnectOpts::new()).unwrap();
            run_on(
                &writer,
                &crash,
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            );
            let mut insert = writer.prepare("INSERT INTO t VALUES (1, 'one')").unwrap();
            let met = loop {
                match insert.step() {
                    Ok(crate::StepResult::IO) => {
                        if insert.atoll_ending_phase() == Some("ending") {
                            break Ok(insert.atoll_txn_for_test().holds_lent());
                        }
                        crash.step().unwrap();
                    }
                    Ok(crate::StepResult::Yield) => crash.step().unwrap(),
                    Ok(other) => break Err(format!("{other:?}")),
                    Err(e) => break Err(e.to_string()),
                }
            };
            let counted = writer.atoll_lent_endings.load(Ordering::SeqCst);
            let refused = match writer.close() {
                Err(crate::LimboError::StatementsInProgress(words)) => Ok(words.to_string()),
                other => Err(format!("{other:?}")),
            };
            let left_open = !writer.is_closed();
            drop(insert);
            let after_drop = writer.atoll_lent_endings.load(Ordering::SeqCst);
            let mut select = reader.prepare("SELECT v FROM t WHERE id = 1").unwrap();
            let read = loop {
                match select.step() {
                    Ok(crate::StepResult::IO) => crash.step().unwrap(),
                    Ok(crate::StepResult::Row) => {
                        break Ok(select.row().unwrap().get_values().next().cloned())
                    }
                    Ok(crate::StepResult::Yield) => crash.step().unwrap(),
                    Ok(other) => break Err(format!("{other:?}")),
                    Err(e) => break Err(e.to_string()),
                }
            };
            drop(select);
            let closed = writer.close().map_err(|e| e.to_string());
            drop((writer, reader, db));
            assert_eq!(
                met,
                Ok(true),
                "the INSERT never reached its lent transaction's ending"
            );
            assert_eq!(counted, 1, "the lent ending was not counted");
            assert_eq!(
                refused,
                Ok(
                    "a statement is ending its transaction: step it to its end, reset it or drop \
                    it before closing"
                        .to_string()
                ),
                "a close at a lent ending was not refused"
            );
            assert!(left_open, "a refused close closed its connection");
            assert_eq!(
                after_drop, 0,
                "the dropped statement's ending is still counted"
            );
            assert_eq!(
                read,
                Ok(Some(Value::build_text("one"))),
                "the commit a refused close left to its statement was not finished"
            );
            assert_eq!(
                closed,
                Ok(()),
                "the connection did not close once its lent ending had finished"
            );
        },
    );
}

/// An ending's start on a lent name under a close that holds the connection's end word is
/// refused, and leaves no lent ending counted (the auditor's #6176 F1 (b); mutant
/// lent-ending-ignores-close).
#[test]
fn a_lent_endings_start_under_a_close_is_refused_and_leaves_none_counted() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_lent_endings_start_under_a_close_is_refused_and_leaves_none_counted"
        ),
        || {
            use crate::sync::atomic::Ordering;
            let (db, conn, other) = two_connections("/db/lent-ending-after-close");
            let mut x = StatementTxn::default();
            ct(&conn).lend(&mut x);
            let lent = x.holds_lent();
            let closed = conn.close().map_err(|e| e.to_string());
            let word = conn.atoll_end.load(Ordering::SeqCst);
            let begun = crate::vdbe::EndingBegun::new(&conn, &x)
                .map(drop)
                .map_err(|e| e.to_string());
            let counted = conn.atoll_lent_endings.load(Ordering::SeqCst);
            ct(&conn).take_back(&mut x);
            drop((conn, other, db));
            assert!(lent, "the name was not lent");
            assert_eq!(closed, Ok(()));
            assert_eq!(
                word,
                Connection::END_CLOSING,
                "the close does not hold the end word"
            );
            assert!(
                begun
                    .as_ref()
                    .is_err_and(|e| e.contains("Connection closed")),
                "a lent ending began under a close: {begun:?}"
            );
            assert_eq!(counted, 0, "a refused lent ending is still counted");
        },
    );
}
