//! A cooperative frame's budget of work (ASYNC1 D2e (iv-a)): each call of a statement in a frame
//! runs at most the budget and what one countdown carries in, a call after a stop for the budget
//! looks at once, frames inside a frame share the outer's budget, and outside any frame nothing
//! stops. The budget is the VDBE's, so these use databases outside Atoll mode.

use super::*;
use crate::storage::atoll::cooperative::{Frame, CHECKS, DISPATCHED};

const DEFAULT_POLL_BUDGET: u32 = Frame::DEFAULT_BUDGET;
use crate::StepResult;

/// The most one countdown carries into a frame before its first check: a check reloads it with at
/// most 256, and the instruction it runs on counts.
const CARRIED: u64 = 255;

/// A database outside Atoll mode, in memory.
fn plain(path: &str) -> (Arc<dyn IO>, Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open(io.clone(), path, OpenOptions::new(Arc::new(SqliteDialect))).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    (io, db, conn)
}

/// What a statement called in frames of one budget came to.
struct Polls {
    /// The instructions each call dispatched, over every program state.
    dispatched: Vec<u64>,
    rows: Vec<Vec<Value>>,
    /// The calls that stopped with a wait: the budget's, or the backend's.
    stops: usize,
}

impl Polls {
    fn most(&self) -> u64 {
        self.dispatched.iter().copied().max().unwrap_or(0)
    }
}

/// Calls `stmt` in a frame of `budget` at a time until it is done, a wait it returns driven
/// outside the frame.
fn in_polls(stmt: &mut crate::Statement, budget: u32) -> Polls {
    let mut polls = Polls {
        dispatched: Vec::new(),
        rows: Vec::new(),
        stops: 0,
    };
    for _ in 0..10_000_000 {
        let before = DISPATCHED.with(|d| d.get());
        let out = {
            let _frame = Frame::with_budget(budget);
            stmt.step()
        };
        polls.dispatched.push(DISPATCHED.with(|d| d.get()) - before);
        match out.unwrap() {
            StepResult::Row => polls
                .rows
                .push(stmt.row().unwrap().get_values().cloned().collect()),
            StepResult::Done => return polls,
            // A nested statement's stop reaches its parent as an explicit yield.
            StepResult::IO | StepResult::Yield => {
                polls.stops += 1;
                stmt._io().step().unwrap();
            }
            other => panic!("a step in a frame came to {other:?}"),
        }
    }
    panic!("not done in 10 000 000 calls");
}

/// Proof 1 (IV-1): a scan with an aggregate, an insert of 100 000 rows, and an insert whose every
/// row fires eight cached triggers, each called in frames of the default budget. Every call
/// dispatches at most the budget and one carried countdown, counted over every program state by a
/// counter that is not the budget; the statements stop many times, and each ends with what the
/// synchronous API gives. The scan again in frames of 100 runs at most 100 a call (mutants
/// `vdbe-uncharged`, `nested-uncharged`, `check-unclamped`).
#[test]
fn each_call_in_a_frame_runs_at_most_its_budget_and_ends_as_the_synchronous_api_does() {
    let bound = u64::from(DEFAULT_POLL_BUDGET) + CARRIED;
    let (_io, _db, conn) = plain("budget-bound.db");
    let (_sio, _sdb, sync) = plain("budget-bound-sync.db");
    for c in [&conn, &sync] {
        for sql in [
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b)",
            "INSERT INTO t SELECT value, value * 3 FROM generate_series(1, 20000)",
            "CREATE TABLE u(a INTEGER PRIMARY KEY, b)",
            "CREATE TABLE w(a INTEGER PRIMARY KEY, b)",
            "CREATE TABLE log1(a)",
            "CREATE TABLE log2(a)",
            "CREATE TABLE log3(a)",
            "CREATE TABLE log4(a)",
            "CREATE TABLE log5(a)",
            "CREATE TABLE log6(a)",
            "CREATE TABLE log7(a)",
            "CREATE TABLE log8(a)",
            "CREATE TRIGGER tr1 AFTER INSERT ON w BEGIN INSERT INTO log1 VALUES (new.a * 1); END",
            "CREATE TRIGGER tr2 AFTER INSERT ON w BEGIN INSERT INTO log2 VALUES (new.a * 2); END",
            "CREATE TRIGGER tr3 AFTER INSERT ON w BEGIN INSERT INTO log3 VALUES (new.a * 3); END",
            "CREATE TRIGGER tr4 AFTER INSERT ON w BEGIN INSERT INTO log4 VALUES (new.a * 4); END",
            "CREATE TRIGGER tr5 AFTER INSERT ON w BEGIN INSERT INTO log5 VALUES (new.a * 5); END",
            "CREATE TRIGGER tr6 AFTER INSERT ON w BEGIN INSERT INTO log6 VALUES (new.a * 6); END",
            "CREATE TRIGGER tr7 AFTER INSERT ON w BEGIN INSERT INTO log7 VALUES (new.a * 7); END",
            "CREATE TRIGGER tr8 AFTER INSERT ON w BEGIN INSERT INTO log8 VALUES (new.a * 8); END",
        ] {
            c.execute(sql).unwrap();
        }
    }
    let mut scan = conn
        .prepare("SELECT count(*), sum(b), max(b) FROM t WHERE b % 7 <> 0")
        .unwrap();
    let scanned = in_polls(&mut scan, DEFAULT_POLL_BUDGET);
    drop(scan);
    // A statement with no nested program: each check cuts the countdown to what is left, so a call
    // in a frame of 100 runs at most 100, never the check's 256.
    let mut small = conn
        .prepare("SELECT count(*), sum(b), max(b) FROM t WHERE b % 7 <> 0")
        .unwrap();
    let small_calls = in_polls(&mut small, 100);
    drop(small);
    let mut insert = conn
        .prepare("INSERT INTO u SELECT value, value || 'x' FROM generate_series(1, 100000)")
        .unwrap();
    let inserted = in_polls(&mut insert, DEFAULT_POLL_BUDGET);
    drop(insert);
    let mut fired = conn
        .prepare("INSERT INTO w SELECT value, value FROM generate_series(1, 5000)")
        .unwrap();
    let triggered = in_polls(&mut fired, DEFAULT_POLL_BUDGET);
    drop(fired);
    sync.execute("INSERT INTO u SELECT value, value || 'x' FROM generate_series(1, 100000)")
        .unwrap();
    sync.execute("INSERT INTO w SELECT value, value FROM generate_series(1, 5000)")
        .unwrap();
    for (name, polls) in [
        ("the scan", &scanned),
        ("the insert", &inserted),
        ("the insert with triggers", &triggered),
    ] {
        assert!(
            // The tighter B + 255, which these workloads meet. The bound the mechanism promises is
            // B + 255 for the top statement plus up to 255 for each nested program kept across
            // rows (DESIGN.md 22.18), so a later workload past 2303 but inside that is no regression.
            polls.most() <= bound,
            "{name}: a call in a frame ran {} instructions, past its budget and one carried \
             countdown ({bound}): {:?}",
            polls.most(),
            polls
                .dispatched
                .iter()
                .filter(|n| **n > bound)
                .collect::<Vec<_>>()
        );
        assert!(
            polls.stops > 10,
            "{name}: the budget stopped it only {} times",
            polls.stops
        );
    }
    assert!(
        small_calls.most() <= 100,
        "a call of a statement with no nested program in a frame of 100 ran {}",
        small_calls.most()
    );
    assert_eq!(
        small_calls.rows, scanned.rows,
        "the scan's row, in frames of 100"
    );
    assert_eq!(
        scanned.rows,
        rows(
            &sync,
            "SELECT count(*), sum(b), max(b) FROM t WHERE b % 7 <> 0"
        ),
        "the scan's row"
    );
    for check in [
        "SELECT count(*), sum(length(b)) FROM u",
        "SELECT count(*), sum(a) FROM log1",
        "SELECT count(*), sum(a) FROM log2",
        "SELECT count(*), sum(a) FROM log3",
        "SELECT count(*), sum(a) FROM log4",
        "SELECT count(*), sum(a) FROM log5",
        "SELECT count(*), sum(a) FROM log6",
        "SELECT count(*), sum(a) FROM log7",
        "SELECT count(*), sum(a) FROM log8",
    ] {
        assert_eq!(rows(&conn, check), rows(&sync, check), "{check}");
    }
}

/// IV-2: a check that stops a statement for the budget leaves its countdown at 1, so the next call
/// looks at the budget at its first iteration, before it runs anything. With a budget of one unit
/// each call then runs one instruction, and with a progress handler on every instruction one handler
/// call, and the statement ends with the synchronous result (mutant `yield-keeps-countdown`).
#[test]
fn a_call_after_a_stop_for_the_budget_checks_before_its_first_instruction() {
    let (_io, _db, conn) = plain("budget-countdown.db");
    let (_sio, _sdb, sync) = plain("budget-countdown-sync.db");
    for c in [&conn, &sync] {
        c.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b)")
            .unwrap();
        c.execute("INSERT INTO t SELECT value, value FROM generate_series(1, 300)")
            .unwrap();
    }
    const SQL: &str = "SELECT count(*), sum(b) FROM t";
    // Without a progress handler: each stop's vm_steps, and the first check after it.
    let mut stmt = conn.prepare(SQL).unwrap();
    let mut after_stops = Vec::new();
    let mut rows_seen = Vec::new();
    loop {
        CHECKS.with(|c| c.borrow_mut().clear());
        let out = {
            let _frame = Frame::with_budget(64);
            stmt.step().unwrap()
        };
        let checks = CHECKS.with(|c| c.borrow().clone());
        after_stops.push(checks);
        match out {
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Row => rows_seen.push(
                stmt.row()
                    .unwrap()
                    .get_values()
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            StepResult::Done => break,
            other => panic!("{other:?}"),
        }
    }
    drop(stmt);
    // A call after a stop checks at once: its first check is at the vm_steps the stop left.
    let mut late = Vec::new();
    for pair in after_stops.windows(2) {
        let (stopped_at, next_first) = (pair[0].last().copied(), pair[1].first().copied());
        if let (Some(at), Some(first)) = (stopped_at, next_first) {
            if first != at {
                late.push((at, first));
            }
        }
    }
    assert!(
        late.is_empty(),
        "a call after a stop for the budget ran instructions before its first check: {late:?}"
    );
    assert!(
        after_stops.len() > 10,
        "the budget stopped the statement only {} times",
        after_stops.len()
    );
    // With a budget of one unit and a progress handler on every instruction.
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counted = calls.clone();
    conn.set_progress_handler(
        1,
        Some(Box::new(move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            false
        })),
    );
    let mut stmt = conn.prepare(SQL).unwrap();
    let mut each = Vec::new();
    let one = loop {
        let before = (
            DISPATCHED.with(|d| d.get()),
            calls.load(std::sync::atomic::Ordering::Relaxed),
        );
        let out = {
            let _frame = Frame::with_budget(1);
            stmt.step().unwrap()
        };
        each.push((
            DISPATCHED.with(|d| d.get()) - before.0,
            calls.load(std::sync::atomic::Ordering::Relaxed) - before.1,
        ));
        match out {
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Row => {
                break stmt
                    .row()
                    .unwrap()
                    .get_values()
                    .cloned()
                    .collect::<Vec<_>>()
            }
            other => panic!("{other:?}"),
        }
    };
    // The handler counts instructions already run, so the first call, whose check comes before
    // any, makes none; each later call makes one, for the instruction the call before it ran.
    let stopped: Vec<_> = each[..each.len() - 1].to_vec();
    assert_eq!(
        stopped.first(),
        Some(&(1, 0)),
        "the first call of one unit: its instructions and handler calls"
    );
    assert!(
        stopped[1..]
            .iter()
            .all(|&(ran, handled)| ran == 1 && handled == 1),
        "a call of one unit ran other than one instruction and one handler call: {:?}",
        stopped[1..]
            .iter()
            .filter(|&&(r, h)| r != 1 || h != 1)
            .take(5)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows_seen,
        rows(&sync, SQL),
        "the statement's row, budget 64"
    );
    assert_eq!(vec![one], rows(&sync, SQL), "the statement's row, budget 1");
}

/// Proof 4: a frame inside a frame shares the outer's budget: it sees what the outer has left,
/// whatever its own budget says, what it charges is the outer's, and leaving it changes nothing
/// (mutant `inner-resets`).
#[test]
fn a_frame_inside_a_frame_shares_the_outer_budget() {
    use crate::storage::atoll::cooperative::{charge, remaining};
    let outer = Frame::with_budget(100);
    charge(30);
    let before = remaining();
    let inner_saw;
    {
        let _inner = Frame::with_budget(5000);
        inner_saw = remaining();
        charge(20);
    }
    let after = remaining();
    // A frame dropped while a panic unwinds ends the process, so the frames go before the asserts.
    drop(outer);
    assert_eq!(before, 70, "the outer frame's budget after its own charge");
    assert_eq!(inner_saw, 70, "the inner frame's budget");
    assert_eq!(
        after, 50,
        "the outer frame's budget after the inner one left"
    );
}

/// Proof 6: outside any frame a statement has no budget. One of more than 10^6 instructions runs
/// to its row in one call, with no stop.
#[test]
fn a_statement_outside_any_frame_runs_to_its_end_in_one_call() {
    let (_io, _db, conn) = plain("budget-none.db");
    let mut stmt = conn
        .prepare("SELECT count(*), sum(value % 13) FROM generate_series(1, 400000)")
        .unwrap();
    let before = DISPATCHED.with(|d| d.get());
    let first = stmt.step().unwrap();
    let ran = DISPATCHED.with(|d| d.get()) - before;
    assert!(
        matches!(first, StepResult::Row),
        "a statement outside any frame stopped: {first:?} after {ran} instructions"
    );
    assert!(ran > 1_000_000, "the statement ran only {ran} instructions");
}

/// A database opened async in Atoll mode with four transaction slots and its checkpoint thread (the
/// worker that drives the log's preparer, which a write in a frame needs), a table `t` of `n` rows,
/// and everything written checkpointed so its pages are clean.
fn async_atoll(path: &str, n: usize) -> (Arc<dyn IO>, Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut cfg = config();
    cfg.txn.slots = 4;
    cfg.checkpointing = Checkpointing::Thread;
    let db = crate::storage::atoll::requests::opened_async(|| open_with(&io, path, cfg));
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b)")
        .unwrap();
    conn.execute(&format!(
        "INSERT INTO t SELECT value, zeroblob(3000) FROM generate_series(1, {n})"
    ))
    .unwrap();
    rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    (io, db, conn)
}

/// A waker that counts its wakes.
struct Counted(std::sync::atomic::AtomicUsize);

impl std::task::Wake for Counted {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Proof 3: the self-woken stop is only the budget's. In a database opened async, a statement in a
/// frame meets a cold read with its IO thread held: it returns a wait that is unfinished, and its
/// waker is not woken until the read lands. A stop for the budget wakes it at once (mutant
/// `self-woken-wait`).
#[test]
fn only_a_stop_for_the_budget_wakes_its_own_waker() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::only_a_stop_for_the_budget_wakes_its_own_waker"
        ),
        || {
            let (io, db, conn) = async_atoll("/db/budget-wake", 200);
            let atoll = db.atoll.as_ref().unwrap().atoll();
            let counted = Arc::new(Counted(std::sync::atomic::AtomicUsize::new(0)));
            let waker = std::task::Waker::from(counted.clone());
            let wakes = || counted.0.load(std::sync::atomic::Ordering::SeqCst);
            // A cold read, its IO thread held.
            drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
            atoll.pool().evict_all_clean();
            atoll.store().meta().evict_all();
            let mut stmt = conn.prepare("SELECT sum(length(b)) FROM t").unwrap();
            let held = crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
                atoll.requests_for_test(),
            )
            .expect("the IO thread held");
            let cold = {
                let _frame = Frame::with_budget(DEFAULT_POLL_BUDGET);
                stmt.step_with_waker(&waker)
            };
            let wait = stmt.pending_io_for_test();
            let unfinished = wait.as_ref().is_some_and(|c| !c.finished());
            let woken_while_held = wakes();
            drop(held);
            let landed = wait.as_ref().is_some_and(|c| {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                while !c.finished() && std::time::Instant::now() < deadline {
                    let _ = io.step();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                c.finished()
            });
            let woken_after = wakes();
            // A stop for the budget, on a database outside Atoll mode, whose only stop is the
            // budget's.
            let (_pio, _pdb, plain_conn) = plain("budget-wake-plain.db");
            let mut warm = plain_conn
                .prepare("SELECT count(*) FROM generate_series(1, 5000)")
                .unwrap();
            let before = wakes();
            let stopped = {
                let _frame = Frame::with_budget(16);
                warm.step_with_waker(&waker)
            };
            let woken_by_stop = wakes() - before;
            // A nested program's stop: an insert firing a trigger whose every firing runs a 100-row
            // subquery, in a frame of 50, so the trigger's program meets its check and stops. The
            // stop reaches the top statement as an explicit yield, the waker woken once.
            for sql in [
                "CREATE TABLE small(a INTEGER PRIMARY KEY, b)",
                "INSERT INTO small SELECT value, value FROM generate_series(1, 100)",
                "CREATE TABLE v(a INTEGER PRIMARY KEY, b)",
                "CREATE TABLE logv(a)",
                "CREATE TRIGGER trv AFTER INSERT ON v BEGIN INSERT INTO logv VALUES (new.a + (SELECT sum(b) FROM small)); END",
            ] {
                plain_conn.execute(sql).unwrap();
            }
            let mut fired = plain_conn.prepare("INSERT INTO v VALUES (1, 1)").unwrap();
            let before_nested = wakes();
            let nested_stop = {
                let _frame = Frame::with_budget(50);
                fired.step_with_waker(&waker)
            };
            let woken_by_nested = wakes() - before_nested;
            drop((stmt, warm, fired));
            assert!(
                matches!(cold, Ok(StepResult::IO)),
                "the cold read did not return for IO: {cold:?}"
            );
            assert!(unfinished, "the cold read's wait was finished at once");
            assert_eq!(
                woken_while_held, 0,
                "a wait for a read woke its waker before the read landed"
            );
            assert!(landed, "the read never landed");
            assert!(
                woken_after >= 1,
                "the read landed and its waker was not woken"
            );
            assert!(
                matches!(stopped, Ok(StepResult::IO)),
                "the warm statement did not stop for its budget: {stopped:?}"
            );
            assert_eq!(
                woken_by_stop, 1,
                "a stop for the budget did not wake its waker at once"
            );
            assert!(
                matches!(nested_stop, Ok(StepResult::Yield)),
                "the trigger's program did not stop for the budget: {nested_stop:?}"
            );
            assert_eq!(
                woken_by_nested, 1,
                "a nested program's stop for the budget did not wake its waker once"
            );
        },
    );
}

/// The auditor's #5758 item 4: `Statement::reset_metrics` sets `vm_steps` back to 0, and the
/// budget's mark goes with it, so the calls after it are charged whole. Each call in frames of 100
/// runs at most 100, and the scan ends with its synchronous result (mutant `reset-keeps-mark`).
#[test]
fn a_reset_of_a_statements_counters_resets_its_budget_mark() {
    // A child: a broken mark trips the charge's assertion inside a frame, which ends the process.
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reset_of_a_statements_counters_resets_its_budget_mark"
        ),
        || {
            let (_io, _db, conn) = plain("budget-reset-metrics.db");
            conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b)")
                .unwrap();
            conn.execute("INSERT INTO t SELECT value, value FROM generate_series(1, 3000)")
                .unwrap();
            const SQL: &str = "SELECT count(*), sum(b) FROM t";
            let mut stmt = conn.prepare(SQL).unwrap();
            let mut calls = Vec::new();
            let mut row = None;
            for i in 0..1_000_000 {
                if i == 20 {
                    stmt.reset_metrics();
                }
                let before = DISPATCHED.with(|d| d.get());
                let out = {
                    let _frame = Frame::with_budget(100);
                    stmt.step().unwrap()
                };
                calls.push(DISPATCHED.with(|d| d.get()) - before);
                match out {
                    StepResult::IO | StepResult::Yield => stmt._io().step().unwrap(),
                    StepResult::Row => {
                        row = Some(
                            stmt.row()
                                .unwrap()
                                .get_values()
                                .cloned()
                                .collect::<Vec<_>>(),
                        )
                    }
                    StepResult::Done => break,
                    other => panic!("{other:?}"),
                }
            }
            drop(stmt);
            assert!(
                calls.len() > 40,
                "the scan stopped only {} times",
                calls.len()
            );
            assert!(
                calls.iter().all(|n| *n <= 100),
                "a call after the counters' reset ran past its frame of 100: {:?}",
                calls.iter().filter(|n| **n > 100).collect::<Vec<_>>()
            );
            assert_eq!(
                row.map(|r| vec![r]),
                Some(rows(&conn, SQL)),
                "the scan's row"
            );
        },
    );
}

/// Proof 7, and the contract's instructions whose work grows with a value's size (IV-5): a
/// function call is one unit and runs to its end. `hex` of a 4 MB blob, called in a frame of the
/// default budget, finishes inside one call that dispatches a handful of instructions, with the
/// synchronous result: the engine does not cut inside a function, whose time is the function's.
#[test]
fn a_function_call_is_one_unit_whatever_its_values_size() {
    let (_io, _db, conn) = plain("budget-function.db");
    const SQL: &str = "SELECT length(hex(zeroblob(4000000)))";
    let mut stmt = conn.prepare(SQL).unwrap();
    let polls = in_polls(&mut stmt, DEFAULT_POLL_BUDGET);
    drop(stmt);
    assert_eq!(polls.rows, rows(&conn, SQL), "the function's result");
    assert_eq!(polls.stops, 0, "the call stopped for its budget");
    assert!(
        polls.most() < 32,
        "the call dispatched {} instructions",
        polls.most()
    );
}

/// Calls `stmt` in a frame of `budget` at a time until it is done, as [`in_polls`] does, on a
/// database opened async: a wait it returns is waited for outside the frame, its IO thread
/// finishing it; a stop for the budget leaves nothing to wait for.
fn in_polls_waiting(io: &Arc<dyn IO>, stmt: &mut crate::Statement, budget: u32) -> Polls {
    let mut polls = Polls {
        dispatched: Vec::new(),
        rows: Vec::new(),
        stops: 0,
    };
    for _ in 0..10_000_000 {
        let before = DISPATCHED.with(|d| d.get());
        let out = {
            let _frame = Frame::with_budget(budget);
            stmt.step()
        };
        polls.dispatched.push(DISPATCHED.with(|d| d.get()) - before);
        match out.unwrap() {
            StepResult::Row => polls
                .rows
                .push(stmt.row().unwrap().get_values().cloned().collect()),
            StepResult::Done => return polls,
            StepResult::IO | StepResult::Yield => match stmt.pending_io_for_test() {
                Some(c) if !c.finished() => {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                    while !c.finished() {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "a wait never finished"
                        );
                        let _ = io.step();
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
                _ => polls.stops += 1,
            },
            other => panic!("a step in a frame came to {other:?}"),
        }
    }
    panic!("not done in 10 000 000 calls");
}

/// Proof 1 on Atoll (the auditor's #5758 item 1): the scan, the insert and three inserts with eight
/// cached triggers, on a database opened async in Atoll mode, called in frames of the default
/// budget. Every call dispatches at most the budget and one carried countdown, each statement stops
/// for its budget many times, and each ends with what the synchronous API gives on a second such
/// database. Here the cached triggers enter frames with their own countdowns, so the driver's cut
/// of a nested state's countdown is what keeps the calls within the bound (condition 1; mutant
/// `nested-unclamped`).
#[test]
fn each_call_of_an_atoll_statement_in_a_frame_runs_at_most_its_budget() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::each_call_of_an_atoll_statement_in_a_frame_runs_at_most_its_budget"
        ),
        || {
            let bound = u64::from(DEFAULT_POLL_BUDGET) + CARRIED;
            let (io, _db, conn) = async_atoll("/db/budget-atoll", 10);
            let (_sio, _sdb, sync) = async_atoll("/db/budget-atoll-sync", 10);
            let mut setup = vec![
                "INSERT INTO t SELECT value, value * 3 FROM generate_series(11, 8000)".to_string(),
                "CREATE TABLE u(a INTEGER PRIMARY KEY, b)".to_string(),
                "CREATE TABLE w(a INTEGER PRIMARY KEY, b)".to_string(),
            ];
            for i in 1..=8 {
                setup.push(format!("CREATE TABLE log{i}(a)"));
                setup.push(format!(
                    "CREATE TRIGGER tr{i} AFTER INSERT ON w BEGIN INSERT INTO log{i} VALUES (new.a * {i}); END"
                ));
            }
            for c in [&conn, &sync] {
                for sql in &setup {
                    c.execute(sql).unwrap();
                }
            }
            const SCAN: &str = "SELECT count(*), sum(a), max(a) FROM t WHERE a % 7 <> 0";
            const INSERT: &str =
                "INSERT INTO u SELECT value, value || 'x' FROM generate_series(1, 20000)";
            const FIRE: &str = "INSERT INTO w SELECT value, value FROM generate_series(1, 2000)";
            const FIRE_MORE: &str =
                "INSERT INTO w SELECT value, value FROM generate_series(2001, 4000)";
            const FIRE_LAST: &str =
                "INSERT INTO w SELECT value, value FROM generate_series(4001, 6000)";
            let mut polls = Vec::new();
            for (name, sql) in [
                ("the scan", SCAN),
                ("the insert", INSERT),
                ("the insert with triggers", FIRE),
                ("the insert with triggers", FIRE_MORE),
                ("the insert with triggers", FIRE_LAST),
            ] {
                let mut stmt = conn.prepare(sql).unwrap();
                polls.push((name, in_polls_waiting(&io, &mut stmt, DEFAULT_POLL_BUDGET)));
            }
            sync.execute(INSERT).unwrap();
            sync.execute(FIRE).unwrap();
            sync.execute(FIRE_MORE).unwrap();
            sync.execute(FIRE_LAST).unwrap();
            let mut checks = vec![
                "SELECT count(*), sum(length(b)) FROM u".to_string(),
                "SELECT count(*), sum(a) FROM w".to_string(),
            ];
            checks.extend((1..=8).map(|i| format!("SELECT count(*), sum(a) FROM log{i}")));
            let compared: Vec<_> = checks
                .iter()
                .map(|c| (c.clone(), rows(&conn, c), rows(&sync, c)))
                .collect();
            let scan_sync = rows(&sync, SCAN);
            for (name, p) in &polls {
                assert!(
                    p.most() <= bound,
                    "{name} on Atoll: a call in a frame ran {} instructions, past its budget and \
                     one carried countdown ({bound})",
                    p.most()
                );
                assert!(
                    p.stops > 10,
                    "{name} on Atoll: the budget stopped it only {} times",
                    p.stops
                );
            }
            assert_eq!(polls[0].1.rows, scan_sync, "the scan's row on Atoll");
            for (check, framed, synced) in compared {
                assert_eq!(framed, synced, "{check} on Atoll");
            }
        },
    );
}
