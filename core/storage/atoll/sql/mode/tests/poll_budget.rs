//! A cooperative frame's budget of work (ASYNC1 D2e (iv-a)): each call of a statement in a frame
//! runs at most the budget, every dispatch charged, or, when that is more, the chain of running
//! program states it continues and one instruction, each running state's first dispatch in a
//! poll's step made whatever the budget. A call after a stop for the budget looks at once, frames
//! inside a frame share the outer's budget, and outside any frame nothing stops. The budget is the
//! VDBE's, so most of these use databases outside Atoll mode.

use super::*;
use crate::storage::atoll::cooperative::{
    Frame, BEFORE_PARENT_PARKS, BEFORE_WAKER_IS_SET, CHARGED, CHECKS, CONTINUED, DISPATCHED, FREE,
};

const DEFAULT_POLL_BUDGET: u32 = Frame::DEFAULT_BUDGET;
use crate::StepResult;

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
    /// The program states each call continued: running at the poll's step of their first dispatch
    /// in the call, each counted once.
    continued: Vec<u64>,
    /// Of each call's dispatches, those made while the frame's budget was spent.
    free: Vec<u64>,
    /// The units each call charged to the budget.
    charged: Vec<u64>,
    rows: Vec<Vec<Value>>,
    /// The calls that stopped with a wait: the budget's, or the backend's.
    stops: usize,
}

impl Polls {
    fn new() -> Self {
        Self {
            dispatched: Vec::new(),
            continued: Vec::new(),
            free: Vec::new(),
            charged: Vec::new(),
            rows: Vec::new(),
            stops: 0,
        }
    }

    fn most(&self) -> u64 {
        self.dispatched.iter().copied().max().unwrap_or(0)
    }

    fn most_continued(&self) -> u64 {
        self.continued.iter().copied().max().unwrap_or(0)
    }

    /// The first call past what a call may run in frames of `budget`: (index, dispatched,
    /// continued, free).
    fn over_cap(&self, budget: u32) -> Option<(usize, u64, u64, u64)> {
        (0..self.dispatched.len())
            .map(|i| (i, self.dispatched[i], self.continued[i], self.free[i]))
            .find(|&(_, d, c, f)| past_cap(budget, d, c, f))
    }

    /// Over the whole run, the units charged against the dispatches, when they differ.
    fn unconserved(&self) -> Option<(u64, u64)> {
        let charged: u64 = self.charged.iter().sum();
        let dispatched: u64 = self.dispatched.iter().sum();
        (charged != dispatched).then_some((charged, dispatched))
    }

    fn median(&self) -> u64 {
        let mut calls = self.dispatched.clone();
        calls.sort_unstable();
        calls.get(calls.len() / 2).copied().unwrap_or(0)
    }
}

/// Whether a call in a frame of `budget` that dispatched `dispatched` instructions, continued
/// `continued` running program states and dispatched `free` of its instructions with the budget
/// spent, ran past what a call may: more than the budget or the states it continued and one
/// instruction, a free dispatch not a continued state's first, or more than the budget besides.
fn past_cap(budget: u32, dispatched: u64, continued: u64, free: u64) -> bool {
    let budget = u64::from(budget);
    dispatched > budget.max(continued + 1) || free > continued || dispatched - free > budget
}

/// The dispatches, the continued program states, the dispatches made with the budget spent and
/// the units charged, on this thread so far.
fn counted() -> (u64, u64, u64, u64) {
    (
        DISPATCHED.with(|d| d.get()),
        CONTINUED.with(|c| c.get()),
        FREE.with(|f| f.get()),
        CHARGED.with(|c| c.get()),
    )
}

/// Calls `stmt` in a frame of `budget` at a time until it is done, a wait it returns driven
/// outside the frame.
fn in_polls(stmt: &mut crate::Statement, budget: u32) -> Polls {
    let mut polls = Polls::new();
    for _ in 0..10_000_000 {
        let before = counted();
        let out = {
            let _frame = Frame::with_budget(budget);
            polled(stmt)
        };
        let after = counted();
        polls.dispatched.push(after.0 - before.0);
        polls.continued.push(after.1 - before.1);
        polls.free.push(after.2 - before.2);
        polls.charged.push(after.3 - before.3);
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
/// row fires eight cached triggers, each called in frames of the default budget. Every call runs
/// at most the budget, counted over every program state by counters that are not the budget, none
/// of it with the budget spent, and continues at most the nesting depth and one running states;
/// over each run the units charged are its dispatches; the statements stop many times, and each
/// ends with what the synchronous API gives. The scan again in frames of 100 runs at most 100 a
/// call (mutants `vdbe-uncharged`, `exit-uncharged`, and `check-unclamped` at the insert with
/// triggers, whose triggers return between the parent's checks).
#[test]
fn each_call_in_a_frame_runs_at_most_its_budget_and_ends_as_the_synchronous_api_does() {
    let bound = u64::from(DEFAULT_POLL_BUDGET);
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
    for (name, polls, depth) in [
        ("the scan", &scanned, 0),
        ("the insert", &inserted, 0),
        ("the insert with triggers", &triggered, 1),
    ] {
        // The budget is above the nesting depth: a call runs at most the budget, exactly.
        assert!(
            polls.most() <= bound,
            "{name}: a call in a frame ran {} instructions, past its budget ({bound}): {:?}",
            polls.most(),
            polls
                .dispatched
                .iter()
                .filter(|n| **n > bound)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            polls.free.iter().sum::<u64>(),
            0,
            "{name}: a call dispatched with the budget spent, which is above the nesting depth"
        );
        assert!(
            polls.most_continued() <= depth + 1,
            "{name}: a call continued {} program states, past the nesting depth and one ({})",
            polls.most_continued(),
            depth + 1
        );
        assert_eq!(
            polls.unconserved(),
            None,
            "{name}: the units charged over the run were not its dispatches (charged, dispatched)"
        );
        assert!(
            polls.stops > 10,
            "{name}: the budget stopped it only {} times",
            polls.stops
        );
        // A call charges only what it ran since it began: one charged from further back would spend
        // its budget at its first check and stop again at once.
        assert!(
            polls.median() >= u64::from(DEFAULT_POLL_BUDGET) / 2,
            "{name}: the median call ran only {} instructions of its budget of {DEFAULT_POLL_BUDGET}",
            polls.median()
        );
    }
    assert!(
        small_calls.most() <= 100,
        "a call of a statement with no nested program in a frame of 100 ran {} units",
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
    let run = counted();
    loop {
        CHECKS.with(|c| c.borrow_mut().clear());
        let out = {
            let _frame = Frame::with_budget(64);
            polled(&mut stmt).unwrap()
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
    let budget_64 = unconserved_since(run);
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
    let handled = calls.clone();
    conn.set_progress_handler(
        1,
        Some(Box::new(move || {
            handled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            false
        })),
    );
    let mut stmt = conn.prepare(SQL).unwrap();
    let mut each = Vec::new();
    let run = counted();
    let one = loop {
        let before = (
            DISPATCHED.with(|d| d.get()),
            calls.load(std::sync::atomic::Ordering::Relaxed),
        );
        let out = {
            let _frame = Frame::with_budget(1);
            polled(&mut stmt).unwrap()
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
    let budget_1 = unconserved_since(run);
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
    for (budget, unconserved) in [(64, budget_64), (1, budget_1)] {
        assert_eq!(
            unconserved, None,
            "budget {budget}: the units charged over the run were not its dispatches \
             (charged, dispatched)"
        );
    }
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

/// The synchronous API carries no budget, even inside a frame: `Statement::step` and
/// `Statement::step_with_waker` run the loop with nothing of the budget in it (the auditor's #5772
/// and #5802). One of more than 10^6 instructions, stepped by either in a frame of 100, runs to its
/// row in one call; stepped by `step_in_frame` in the same frame, it stops (mutants `sync-budgeted`
/// and `waker-budgeted`).
#[test]
fn a_synchronous_step_in_a_frame_carries_no_budget() {
    let (_io, _db, conn) = plain("budget-sync-in-frame.db");
    let sql = "SELECT count(*), sum(value % 13) FROM generate_series(1, 400000)";
    let first_call = |step: fn(&mut crate::Statement) -> crate::Result<StepResult>| {
        let mut stmt = conn.prepare(sql).unwrap();
        let before = DISPATCHED.with(|d| d.get());
        let first = {
            let _frame = Frame::with_budget(100);
            step(&mut stmt).unwrap()
        };
        (first, DISPATCHED.with(|d| d.get()) - before)
    };
    let (synchronous, ran) = first_call(|stmt| stmt.step());
    let (with_waker, ran_with_waker) =
        first_call(|stmt| stmt.step_with_waker(std::task::Waker::noop()));
    let (polled_first, _) = first_call(polled);
    assert!(
        matches!(synchronous, StepResult::Row) && ran > 1_000_000,
        "a synchronous step in a frame stopped: {synchronous:?} after {ran} instructions"
    );
    assert!(
        matches!(with_waker, StepResult::Row) && ran_with_waker > 1_000_000,
        "a step with a waker in a frame stopped: {with_waker:?} after {ran_with_waker} instructions"
    );
    assert!(
        matches!(polled_first, StepResult::IO),
        "a poll's step in a frame of 100 did not stop: {polled_first:?}"
    );
}

/// One call in a frame, counted: the instructions dispatched over every program state, the
/// running program states it continued, its dispatches made with the budget spent, the units it
/// charged, the statement's own dispatches, and whether the call came to a row.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Call {
    dispatched: u64,
    continued: u64,
    free: u64,
    charged: u64,
    parent: u64,
    row: bool,
}

/// Calls `stmt` in frames of `budget` until it is done, counting each call, on a database outside
/// Atoll mode, where every stop is the budget's. With `reset_counters`, the statement's counters
/// are reset before each call. Fails, naming the statement as `what`, after `most` calls.
fn calls_in_frames(
    what: &str,
    stmt: &mut crate::Statement,
    budget: u32,
    most: usize,
    reset_counters: bool,
) -> Vec<Call> {
    let mut calls = Vec::new();
    for _ in 0..most {
        if reset_counters {
            stmt.reset_metrics();
        }
        let (call, out) = one_call(stmt, budget);
        calls.push(call);
        match out.unwrap() {
            StepResult::Done => return calls,
            StepResult::Row => {}
            StepResult::IO | StepResult::Yield => stmt._io().step().unwrap(),
            other => panic!("{what}: a step in a frame came to {other:?}"),
        }
    }
    panic!("{what}: not done in {most} calls");
}

/// One poll's step of `stmt` in a frame of `budget`, counted.
fn one_call(stmt: &mut crate::Statement, budget: u32) -> (Call, crate::Result<StepResult>) {
    let (before, own) = (counted(), stmt.own_vm_steps_for_test());
    let out = {
        let _frame = Frame::with_budget(budget);
        polled(stmt)
    };
    let after = counted();
    let call = Call {
        dispatched: after.0 - before.0,
        continued: after.1 - before.1,
        free: after.2 - before.2,
        charged: after.3 - before.3,
        parent: stmt.own_vm_steps_for_test() - own,
        row: matches!(out, Ok(StepResult::Row)),
    };
    (call, out)
}

/// Over the whole run, the units charged against the dispatches, when they differ: a dispatch
/// charged twice or never.
fn unconserved(calls: &[Call]) -> Option<(u64, u64)> {
    let charged: u64 = calls.iter().map(|c| c.charged).sum();
    let dispatched: u64 = calls.iter().map(|c| c.dispatched).sum();
    (charged != dispatched).then_some((charged, dispatched))
}

/// [`unconserved`] over everything counted on this thread since `before`, for a run stepped by a
/// loop of its own: only polls' steps inside it, whose every dispatch the budget sees.
fn unconserved_since(before: (u64, u64, u64, u64)) -> Option<(u64, u64)> {
    let after = counted();
    let charged = after.3 - before.3;
    let dispatched = after.0 - before.0;
    (charged != dispatched).then_some((charged, dispatched))
}

/// The first call past what a call may run in frames of `budget` ([`past_cap`]), with its index.
fn over_cap(calls: &[Call], budget: u32) -> Option<(usize, Call)> {
    calls
        .iter()
        .copied()
        .enumerate()
        .find(|(_, c)| past_cap(budget, c.dispatched, c.continued, c.free))
}

/// Tables `v` (each insert fires a trigger), `v2` (its trigger's insert fires another) and `v3`
/// (three levels), each trigger's body a sum over 300 rows: longer than the default budget, so a call that enters it
/// spends whatever the budget has left.
fn nested_triggers(conn: &Arc<Connection>) {
    for sql in [
        "CREATE TABLE big(a INTEGER PRIMARY KEY, b)",
        "INSERT INTO big SELECT value, value FROM generate_series(1, 300)",
        "CREATE TABLE v(a, b)",
        "CREATE TABLE logv(a)",
        "CREATE TRIGGER trv AFTER INSERT ON v BEGIN INSERT INTO logv VALUES (new.a + (SELECT sum(b) FROM big)); END",
        "CREATE TABLE v2(a, b)",
        "CREATE TABLE logv2(a)",
        "CREATE TABLE logw(a)",
        "CREATE TRIGGER trv2 AFTER INSERT ON v2 BEGIN INSERT INTO logv2 VALUES (new.a + (SELECT sum(b) FROM big)); END",
        "CREATE TRIGGER trl AFTER INSERT ON logv2 BEGIN INSERT INTO logw VALUES (new.a + (SELECT sum(b) FROM big)); END",
        "CREATE TABLE v3(a, b)",
        "CREATE TABLE logv3(a)",
        "CREATE TABLE logw3(a)",
        "CREATE TABLE logx3(a)",
        "CREATE TRIGGER trv3 AFTER INSERT ON v3 BEGIN INSERT INTO logv3 VALUES (new.a + (SELECT sum(b) FROM big)); END",
        "CREATE TRIGGER trw3 AFTER INSERT ON logv3 BEGIN INSERT INTO logw3 VALUES (new.a + (SELECT sum(b) FROM big)); END",
        "CREATE TRIGGER trx3 AFTER INSERT ON logw3 BEGIN INSERT INTO logx3 VALUES (new.a + (SELECT sum(b) FROM big)); END",
    ] {
        conn.execute(sql).unwrap();
    }
}

/// An insert of `rows` rows into `table` whose row carries an expression of `k` terms, in groups
/// of 50 so the parser's depth limit is not met.
fn insert_with_terms(table: &str, k: usize, rows: u32) -> String {
    let groups: String = (0..k)
        .collect::<Vec<_>>()
        .chunks(50)
        .map(|group| {
            let terms: Vec<String> = group.iter().map(|i| (i % 7).to_string()).collect();
            format!(" + ({})", terms.join(" + "))
        })
        .collect();
    format!("INSERT INTO {table} SELECT value, value{groups} FROM generate_series(1, {rows})")
}

/// The call whose trigger spends the frame's budget, made on purpose (#5810, #5813). The parent's
/// row carries an expression of k terms, swept from 0 to 300, so a trigger's instruction falls at
/// every distance from the parent's last check; the triggers' bodies are longer than the budget.
/// At budgets 1, 2, 7, 100 and 2048, with one level of triggers and with two, every call runs at
/// most max(budget, continued + 1), its free dispatches its continued states' first, and at most
/// the budget besides; it continues at most the nesting depth and one states; at a budget above the
/// depth it runs at most the budget, none free (mutants `parent-uncharged`, `parent-uncut`,
/// `entry-uncut`).
#[test]
fn a_trigger_that_spends_the_budget_ends_its_call_within_the_bound() {
    let (_io, _db, conn) = plain("budget-trigger-spends.db");
    nested_triggers(&conn);
    for (table, depth) in [("v", 1u64), ("v2", 2)] {
        for budget in [1, 2, 7, 100, DEFAULT_POLL_BUDGET] {
            for k in 0..=300 {
                let what = format!("into {table} at k {k}, budget {budget}");
                let mut stmt = conn.prepare(insert_with_terms(table, k, 2)).unwrap();
                let calls = calls_in_frames(&what, &mut stmt, budget, 10_000_000, false);
                if let Some((i, call)) = over_cap(&calls, budget) {
                    panic!(
                        "into {table} at k {k}, budget {budget}: call {i} ran past its cap: {} \
                         dispatched (parent {}, nested {}), {} continued, {} free",
                        call.dispatched,
                        call.parent,
                        call.dispatched - call.parent,
                        call.continued,
                        call.free
                    );
                }
                let continued = calls.iter().map(|c| c.continued).max().unwrap();
                assert!(
                    continued <= depth + 1,
                    "into {table} at k {k}, budget {budget}: a call continued {continued} program \
                     states, past the nesting depth and one ({})",
                    depth + 1
                );
                if u64::from(budget) > depth {
                    let most = calls.iter().map(|c| c.dispatched).max().unwrap();
                    let free: u64 = calls.iter().map(|c| c.free).sum();
                    assert!(
                        most <= u64::from(budget) && free == 0,
                        "into {table} at k {k}, budget {budget}: above the nesting depth a call ran \
                         {most}, {free} free in all"
                    );
                }
                assert_eq!(
                    unconserved(&calls),
                    None,
                    "into {table} at k {k}, budget {budget}: the units charged over the run were \
                     not its dispatches"
                );
            }
        }
    }
}

/// Progress where the budget is below the chain (#5827 item 3): statements firing one, two and
/// three levels of triggers, at budgets 1 and 2, in frames to their end. Each call runs at least one
/// instruction and at most max(budget, depth + 1), continues at most the depth and one states, and
/// some call continues the whole chain; each statement ends with the synchronous result. The same with the statement's
/// counters reset before every call (mutant `no-free`, a running state's first dispatch charged
/// like any other: the parent's re-dispatch spends the budget and the trigger stops having run
/// nothing, call after call).
#[test]
fn a_budget_below_the_chain_moves_the_statement_to_its_end() {
    let (_io, _db, conn) = plain("budget-below.db");
    let (_sio, _sdb, sync) = plain("budget-below-sync.db");
    nested_triggers(&conn);
    nested_triggers(&sync);
    let mut first = 0;
    for (table, depth, budget, reset_counters) in [
        ("v", 1u64, 1u32, false),
        ("v2", 2, 1, false),
        ("v3", 3, 1, false),
        ("v3", 3, 2, false),
        ("v2", 2, 2, false),
        ("v3", 3, 1, true),
    ] {
        let sql = insert_with_terms(table, 0, 3).replace(
            "generate_series(1, 3)",
            &format!("generate_series({}, {})", first + 1, first + 3),
        );
        first += 3;
        let mut stmt = conn.prepare(&sql).unwrap();
        let label = if reset_counters {
            format!("into {table} at budget {budget}, its counters reset before each call")
        } else {
            format!("into {table} at budget {budget}")
        };
        let calls = calls_in_frames(&label, &mut stmt, budget, 1_000_000, reset_counters);
        if let Some((i, call)) = over_cap(&calls, budget) {
            panic!("{label}: call {i} ran past its cap: {call:?}");
        }
        let cap = u64::from(budget).max(depth + 1);
        for (i, call) in calls.iter().enumerate() {
            assert!(
                (1..=cap).contains(&call.dispatched) && call.continued <= depth + 1,
                "{label}: call {i} dispatched {} (at most {cap}) and continued {}, depth {depth}",
                call.dispatched,
                call.continued
            );
        }
        assert!(
            calls.iter().any(|c| c.continued == depth + 1),
            "{label}: no call continued the whole chain of {} states",
            depth + 1
        );
        assert_eq!(
            unconserved(&calls),
            None,
            "{label}: the units charged over the run were not its dispatches"
        );
        sync.execute(&sql).unwrap();
    }
    for check in [
        "SELECT count(*), sum(b) FROM v",
        "SELECT count(*), sum(a) FROM logv",
        "SELECT count(*), sum(b) FROM v2",
        "SELECT count(*), sum(a) FROM logv2",
        "SELECT count(*), sum(a) FROM logw",
        "SELECT count(*), sum(b) FROM v3",
        "SELECT count(*), sum(a) FROM logv3",
        "SELECT count(*), sum(a) FROM logw3",
        "SELECT count(*), sum(a) FROM logx3",
    ] {
        assert_eq!(rows(&conn, check), rows(&sync, check), "{check}");
    }
}

/// A call after a row continues the statement, running, and its first dispatch is charged like any
/// other (#5815 item 1): within the cap, and over the run the units charged are the dispatches.
/// - On a database opened async in Atoll mode, a scan of a table of one page whose reads are cold,
///   its IO thread held for the first: the scan stops inside that read, and every call after it
///   comes to a row or continues a read, the whole scan inside one check interval.
/// - On a database outside Atoll, an insert returning its rows, each row firing a trigger longer
///   than the budget, at budgets 7, 100 and 2048, the same; at the default budget some call
///   continues a running statement and comes to a row.
#[test]
fn a_call_after_a_row_charges_its_first_instruction() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_call_after_a_row_charges_its_first_instruction"
        ),
        || {
            let (io, db, conn) = async_atoll("/db/budget-after-row", 10);
            let atoll = db.atoll.as_ref().unwrap().atoll();
            conn.execute("CREATE TABLE s(a INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute("INSERT INTO s VALUES (1), (2), (3)").unwrap();
            rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
            rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
            let mut scan = conn.prepare("SELECT a FROM s").unwrap();
            drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
            atoll.pool().evict_all_clean();
            atoll.store().meta().evict_all();
            let hold = crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
                atoll.requests_for_test(),
            )
            .expect("the IO thread held");
            let (first, first_out) = one_call(&mut scan, DEFAULT_POLL_BUDGET);
            let first_unfinished = scan.pending_io_for_test().is_some_and(|c| !c.finished());
            drop(hold);
            let mut scanned = vec![first];
            if first_unfinished {
                if let Some(c) = scan.pending_io_for_test() {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                    while !c.finished() && std::time::Instant::now() < deadline {
                        let _ = io.step();
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }
            scanned.extend(calls_waiting(&io, &mut scan, DEFAULT_POLL_BUDGET));
            drop(scan);
            let (_pio, _pdb, plain_conn) = plain("budget-after-row.db");
            nested_triggers(&plain_conn);
            let mut returning = Vec::new();
            for table in ["v", "v2"] {
                for budget in [7, 100, DEFAULT_POLL_BUDGET] {
                    let what = format!("into {table} returning its rows, budget {budget}");
                    let sql = format!("{} RETURNING a", insert_with_terms(table, 0, 3));
                    let mut stmt = plain_conn.prepare(&sql).unwrap();
                    let calls = calls_in_frames(&what, &mut stmt, budget, 10_000_000, false);
                    returning.push((what, budget, calls));
                }
            }
            assert!(
                matches!(first_out, Ok(StepResult::IO)) && first_unfinished,
                "the scan's cold read did not return an unfinished wait: {first_out:?}"
            );
            assert!(
                scanned.iter().filter(|c| c.row).count() == 3,
                "the scan came to {} rows, not 3",
                scanned.iter().filter(|c| c.row).count()
            );
            if let Some((i, call)) = over_cap(&scanned, DEFAULT_POLL_BUDGET) {
                panic!("the scan of a page read cold: call {i} ran past its cap: {call:?}");
            }
            assert_eq!(
                unconserved(&scanned),
                None,
                "the scan of a page read cold: the units charged over the run were not its dispatches"
            );
            for (what, budget, calls) in &returning {
                if let Some((i, call)) = over_cap(calls, *budget) {
                    panic!("{what}: call {i} ran past its cap: {call:?}");
                }
                assert_eq!(
                    unconserved(calls),
                    None,
                    "{what}: the units charged over the run were not its dispatches"
                );
                if *budget == DEFAULT_POLL_BUDGET {
                    assert!(
                        calls.iter().any(|c| c.row && c.continued > 0),
                        "{what}: no call continued a running statement and came to a row"
                    );
                }
            }
        },
    );
}

/// A statement reset after a stop continues nothing: it starts again at its first instruction, not
/// running, so that dispatch is not made under a spent budget. An insert stopped inside its trigger
/// is reset, called once in a frame of no budget, and called again in frames to its end: no call
/// runs past its cap, and over the run the units charged are its dispatches.
#[test]
fn a_statement_reset_after_a_stop_continues_nothing() {
    let (_io, _db, conn) = plain("budget-reset-stop.db");
    nested_triggers(&conn);
    let mut stmt = conn.prepare(insert_with_terms("v", 0, 3)).unwrap();
    let (stopped, out) = one_call(&mut stmt, 100);
    assert!(
        matches!(out, Ok(StepResult::Yield)),
        "the insert did not stop inside its trigger: {out:?} after {stopped:?}"
    );
    stmt.reset().unwrap();
    let (zero, _) = one_call(&mut stmt, 0);
    let mut calls = vec![zero];
    calls.extend(calls_in_frames(
        "the reset insert",
        &mut stmt,
        100,
        10_000_000,
        false,
    ));
    if let Some((i, call)) = over_cap(&calls, 100) {
        panic!("the reset insert: call {i} ran past its cap: {call:?}");
    }
    assert_eq!(
        unconserved(&calls),
        None,
        "the reset insert: the units charged over the run were not its dispatches"
    );
}

/// A statement stepped again after its program is prepared again for a schema change starts
/// over: it was not running, so its first dispatch is not made under a spent budget. A scan
/// stopped by a budget of one before its transaction's instruction, another connection then
/// creating a table, called in frames of one to its end: it is prepared again exactly once, and
/// the call that prepares it dispatches one instruction, the one whose schema check failed, and
/// stops before the new program's first (mutant `free-regardless`).
#[test]
fn a_statement_prepared_again_in_its_call_starts_over() {
    let (_io, db, conn) = plain("budget-reprepare.db");
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b)")
        .unwrap();
    conn.execute("INSERT INTO t SELECT value, value FROM generate_series(1, 100)")
        .unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
    let (first, first_out) = one_call(&mut stmt, 1);
    assert!(
        matches!(first_out, Ok(StepResult::IO)) && first.dispatched == 1,
        "the scan's first call at a budget of one: {first_out:?} after {first:?}"
    );
    other.execute("CREATE TABLE u(a)").unwrap();
    let mut calls = vec![first];
    let mut prepared_in = Vec::new();
    let mut row = None;
    for _ in 0..10_000_000 {
        let before = stmt.metrics().reprepares;
        let (call, out) = one_call(&mut stmt, 1);
        if stmt.metrics().reprepares > before {
            prepared_in.push((calls.len(), call));
        }
        calls.push(call);
        match out.unwrap() {
            StepResult::Done => break,
            StepResult::Row => row = Some(stmt.row().unwrap().get_values().cloned().collect()),
            StepResult::IO | StepResult::Yield => stmt._io().step().unwrap(),
            other => panic!("the scan prepared again: a step in a frame came to {other:?}"),
        }
    }
    let reprepares = stmt.metrics().reprepares;
    drop(stmt);
    assert_eq!(
        (reprepares, prepared_in.len()),
        (1, 1),
        "the scan's preparations again, and the calls that made them"
    );
    let (i, call) = prepared_in[0];
    assert_eq!(
        call.dispatched, 1,
        "the call that prepared the scan again, call {i}: its instructions, the one whose schema \
         check failed and none of the program prepared again: {call:?}"
    );
    if let Some((i, call)) = over_cap(&calls, 1) {
        panic!("the scan prepared again: call {i} ran past its cap: {call:?}");
    }
    assert_eq!(
        unconserved(&calls),
        None,
        "the scan prepared again: the units charged over the run were not its dispatches"
    );
    assert_eq!(row, Some(vec![Value::from_i64(100)]), "the scan's row");
}

/// A subprogram that returns rows is stepped again inside the same Program dispatch for each: under
/// a spent budget only its first step, in the parent's first dispatch, goes on, and the parent stops
/// before the next. An insert whose trigger's body selects 300 rows, one level of nesting, at
/// budgets 1, 2 and 7: every call runs at most max(budget, 2), the insert's Program instruction and
/// the trigger's one (mutant `free-every-step`, a third after a row under a spent budget).
#[test]
fn a_subprogram_returning_rows_is_not_stepped_again_under_a_spent_budget() {
    let (_io, _db, conn) = plain("budget-trigger-rows.db");
    let (_sio, _sdb, sync) = plain("budget-trigger-rows-sync.db");
    for c in [&conn, &sync] {
        for sql in [
            "CREATE TABLE big(a INTEGER PRIMARY KEY, b)",
            "INSERT INTO big SELECT value, value FROM generate_series(1, 300)",
            "CREATE TABLE r(a, b)",
            "CREATE TABLE logr(a)",
            "CREATE TRIGGER trr AFTER INSERT ON r BEGIN SELECT a FROM big; INSERT INTO logr VALUES (new.a); END",
        ] {
            c.execute(sql).unwrap();
        }
    }
    let mut first = 0;
    for budget in [1, 2, 7] {
        let sql = format!(
            "INSERT INTO r SELECT value, value FROM generate_series({}, {})",
            first + 1,
            first + 2
        );
        first += 2;
        let what = format!("into r at budget {budget}");
        let mut stmt = conn.prepare(&sql).unwrap();
        let calls = calls_in_frames(&what, &mut stmt, budget, 10_000_000, false);
        if let Some((i, call)) = over_cap(&calls, budget) {
            panic!("{what}: call {i} ran past its cap: {call:?}");
        }
        let cap = u64::from(budget).max(2);
        if let Some((i, call)) = calls.iter().enumerate().find(|(_, c)| c.dispatched > cap) {
            panic!(
                "{what}: call {i} ran {} instructions, past {cap}: {call:?}",
                call.dispatched
            );
        }
        assert_eq!(
            unconserved(&calls),
            None,
            "{what}: the units charged over the run were not its dispatches"
        );
        sync.execute(&sql).unwrap();
    }
    for check in [
        "SELECT count(*), sum(b) FROM r",
        "SELECT count(*), sum(a) FROM logr",
    ] {
        assert_eq!(rows(&conn, check), rows(&sync, check), "{check}");
    }
}

/// A frame of no budget is the caller's error: a poll's step in it dispatches nothing and returns
/// the budget's yield, which wakes its own waker at once. A statement stopped inside its trigger,
/// running, is not continued in such a frame either: the poll's step stops before the step, ahead
/// of the first dispatch a running statement makes whatever the budget (mutant `zero-runs`).
#[test]
fn a_frame_of_no_budget_runs_nothing() {
    let (_io, _db, conn) = plain("budget-zero.db");
    let counted_wakes = Arc::new(Counted(std::sync::atomic::AtomicUsize::new(0)));
    let waker = std::task::Waker::from(counted_wakes.clone());
    let mut stmt = conn
        .prepare("SELECT count(*) FROM generate_series(1, 100)")
        .unwrap();
    for _ in 0..3 {
        let before = counted();
        let out = {
            let _frame = Frame::with_budget(0);
            stmt.step_in_frame(&waker).unwrap()
        };
        assert_eq!(
            counted().0 - before.0,
            0,
            "a call in a frame of no budget dispatched"
        );
        assert!(
            matches!(out, StepResult::IO),
            "a call in a frame of no budget came to {out:?}"
        );
    }
    assert_eq!(
        counted_wakes.0.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "each call in a frame of no budget woke its waker"
    );
    nested_triggers(&conn);
    let mut stopped = conn.prepare(insert_with_terms("v", 0, 2)).unwrap();
    let (first, first_out) = one_call(&mut stopped, 50);
    assert!(
        matches!(first_out, Ok(StepResult::Yield)),
        "the insert did not stop inside its trigger: {first_out:?} after {first:?}"
    );
    for i in 0..3 {
        let (call, out) = one_call(&mut stopped, 0);
        assert_eq!(
            (call.dispatched, call.free),
            (0, 0),
            "call {i} of a stopped statement in a frame of no budget: (dispatched, free)"
        );
        assert!(
            matches!(out, Ok(StepResult::IO)),
            "call {i} of a stopped statement in a frame of no budget came to {out:?}"
        );
    }
    let rest = calls_in_frames("the stopped insert", &mut stopped, 100, 10_000_000, false);
    if let Some((i, call)) = over_cap(&rest, 100) {
        panic!("the stopped insert after frames of no budget: call {i} ran past its cap: {call:?}");
    }
    assert_eq!(
        unconserved(&rest),
        None,
        "the stopped insert after frames of no budget: the units charged over the run were not its dispatches"
    );
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
            let woken = Arc::new(Counted(std::sync::atomic::AtomicUsize::new(0)));
            let waker = std::task::Waker::from(woken.clone());
            let wakes = || woken.0.load(std::sync::atomic::Ordering::SeqCst);
            // A cold read, its IO thread held.
            drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
            atoll.pool().evict_all_clean();
            atoll.store().meta().evict_all();
            let mut stmt = conn.prepare("SELECT sum(length(b)) FROM t").unwrap();
            let held = crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
                atoll.requests_for_test(),
            )
            .expect("the IO thread held");
            let window = counted();
            let cold = {
                let _frame = Frame::with_budget(DEFAULT_POLL_BUDGET);
                stmt.step_in_frame(&waker)
            };
            let cold_unconserved = unconserved_since(window);
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
            // The read's waker runs on the IO thread after its result is published. The thread
            // held where it would mark itself idle has run every waker of what it served, and
            // runs none in the windows below.
            let held = crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
                atoll.requests_for_test(),
            )
            .expect("the IO thread held after the read");
            let woken_after = wakes();
            // A stop for the budget, on a database outside Atoll mode, whose only stop is the
            // budget's.
            let (_pio, _pdb, plain_conn) = plain("budget-wake-plain.db");
            let mut warm = plain_conn
                .prepare("SELECT count(*) FROM generate_series(1, 5000)")
                .unwrap();
            let before = wakes();
            let window = counted();
            let stopped = {
                let _frame = Frame::with_budget(16);
                warm.step_in_frame(&waker)
            };
            let stop_unconserved = unconserved_since(window);
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
            let window = counted();
            let nested_stop = {
                let _frame = Frame::with_budget(50);
                fired.step_in_frame(&waker)
            };
            let nested_unconserved = unconserved_since(window);
            let woken_by_nested = wakes() - before_nested;
            drop(held);
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
            for (call, unconserved) in [
                ("the cold read's", cold_unconserved),
                ("the budget's stop", stop_unconserved),
                ("the nested program's stop", nested_unconserved),
            ] {
                assert_eq!(
                    unconserved, None,
                    "{call} call: the units charged were not its first dispatches"
                );
            }
        },
    );
}

/// A step parks its wait without the waker and sets the waker on it as it returns
/// (`hand_waker_to_stop`): a wait that lands in between still wakes the waker, since setting it
/// looks at whether the wait has landed (`Completion::set_keyed_waker`). On a database opened async
/// in Atoll mode, a scan whose pages are cold, its IO thread held, stepped in a frame by
/// `step_with_waker` and by `step_in_frame`: a hook lands the first wait after the park and before
/// the waker is set. No
/// waker is woken by the landing, the waker is woken as it is set, and the scan ends with the
/// synchronous result (mutant `waker-unset`).
#[test]
fn a_wait_that_lands_before_its_waker_is_set_still_wakes_it() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_wait_that_lands_before_its_waker_is_set_still_wakes_it"
        ),
        || {
            let (io, db, conn) = async_atoll("/db/waker-late", 200);
            let atoll = db.atoll.as_ref().unwrap().atoll();
            const SQL: &str = "SELECT sum(length(b)) FROM t";
            let expected = rows(&conn, SQL);
            let mut seen = Vec::new();
            for polled in [false, true] {
                let woken = Arc::new(Counted(std::sync::atomic::AtomicUsize::new(0)));
                let waker = std::task::Waker::from(woken.clone());
                drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
                atoll.pool().evict_all_clean();
                atoll.store().meta().evict_all();
                let mut stmt = conn.prepare(SQL).unwrap();
                let hold = crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
                    atoll.requests_for_test(),
                )
                .expect("the IO thread held");
                let hook_io = io.clone();
                let landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let saw_landed = landed.clone();
                let woken_at_landing = Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX));
                let saw_woken = woken_at_landing.clone();
                let woken_in_hook = woken.clone();
                BEFORE_WAKER_IS_SET.with(|h| {
                    *h.borrow_mut() = Some(Box::new(move |wait: &crate::Completion| {
                        drop(hold);
                        let deadline =
                            std::time::Instant::now() + std::time::Duration::from_secs(20);
                        while !wait.finished() && std::time::Instant::now() < deadline {
                            let _ = hook_io.step();
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                        saw_landed.store(wait.finished(), std::sync::atomic::Ordering::SeqCst);
                        saw_woken.store(
                            woken_in_hook.0.load(std::sync::atomic::Ordering::SeqCst),
                            std::sync::atomic::Ordering::SeqCst,
                        );
                    }))
                });
                // Both in a frame, where a read is queued for the IO threads; `step_with_waker`
                // carries no budget there.
                let step = |stmt: &mut crate::Statement| {
                    let _frame = Frame::with_budget(DEFAULT_POLL_BUDGET);
                    if polled {
                        stmt.step_in_frame(&waker)
                    } else {
                        stmt.step_with_waker(&waker)
                    }
                };
                let first = step(&mut stmt);
                let hook_ran = BEFORE_WAKER_IS_SET
                    .with(|h| h.borrow_mut().take())
                    .is_none();
                let woken_at_set = woken.0.load(std::sync::atomic::Ordering::SeqCst);
                let mut row = None;
                for _ in 0..10_000_000 {
                    match step(&mut stmt).unwrap() {
                        StepResult::Done => break,
                        StepResult::Row => {
                            row = Some(vec![stmt.row().unwrap().get_values().cloned().collect()])
                        }
                        StepResult::IO | StepResult::Yield => {
                            if let Some(c) = stmt.pending_io_for_test() {
                                let deadline =
                                    std::time::Instant::now() + std::time::Duration::from_secs(20);
                                while !c.finished() {
                                    assert!(
                                        std::time::Instant::now() < deadline,
                                        "a wait never finished"
                                    );
                                    let _ = io.step();
                                    std::thread::sleep(std::time::Duration::from_millis(1));
                                }
                            }
                        }
                        other => panic!("the scan came to {other:?}"),
                    }
                }
                drop(stmt);
                seen.push((
                    polled,
                    first,
                    hook_ran,
                    landed.load(std::sync::atomic::Ordering::SeqCst),
                    woken_at_landing.load(std::sync::atomic::Ordering::SeqCst),
                    woken_at_set,
                    row,
                ));
            }
            for (polled, first, hook_ran, landed, woken_at_landing, woken_at_set, row) in seen {
                let how = if polled {
                    "step_in_frame"
                } else {
                    "step_with_waker"
                };
                assert!(
                    matches!(first, Ok(StepResult::IO)) && hook_ran,
                    "{how}: the cold scan's first step did not park a wait: {first:?}, hook ran {hook_ran}"
                );
                assert!(landed, "{how}: the wait never landed in the hook");
                assert_eq!(
                    woken_at_landing, 0,
                    "{how}: the waker was woken before it was set"
                );
                assert!(
                    woken_at_set >= 1,
                    "{how}: the wait landed before its waker was set, and the waker was not woken"
                );
                assert_eq!(row.as_ref(), Some(&expected), "{how}: the scan's row");
            }
        },
    );
}

/// A wait inside a trigger, on a database opened async in Atoll mode: a trigger's read of a cold
/// page, its IO thread held, returns a wait that is unfinished. Continued by the next call, the
/// chain is two running states, the parent's Program instruction and the trigger's read. Finished
/// before the parent parks it (the test lands the read there), the wait is dispatched again inside
/// the call: the parent's Program instruction and the trigger's read are charged again, and the
/// trigger stepped again is not continued, as it was not running when the call began, so nothing
/// is free; with two levels of triggers, the read landing only as the top statement parks it, the
/// same. And when the top's dispatch again spends the budget's last unit, the parent stops
/// before it steps the trigger again: the next call continues both, the chain of two.
#[test]
fn a_trigger_continued_inside_its_call_is_charged_again() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_trigger_continued_inside_its_call_is_charged_again"
        ),
        || {
            let (io, db, conn) = async_atoll("/db/budget-inside", 200);
            let atoll = db.atoll.as_ref().unwrap().atoll();
            for sql in [
                "CREATE TABLE v(a INTEGER PRIMARY KEY, b)",
                "CREATE TABLE logv(a)",
                "CREATE TRIGGER trv AFTER INSERT ON v BEGIN INSERT INTO logv VALUES (new.a + (SELECT sum(length(b)) FROM t)); END",
                "CREATE TABLE v2(a INTEGER PRIMARY KEY, b)",
                "CREATE TABLE logv2(a)",
                "CREATE TABLE logw(a)",
                "CREATE TRIGGER trv2 AFTER INSERT ON v2 BEGIN INSERT INTO logv2 VALUES (new.a); END",
                "CREATE TRIGGER trl AFTER INSERT ON logv2 BEGIN INSERT INTO logw VALUES (new.a + (SELECT sum(length(b)) FROM t)); END",
            ] {
                conn.execute(sql).unwrap();
            }
            rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
            rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
            // Only `t`'s pages cold: the parent's insert and the trigger's own write find theirs, so
            // the first wait that reaches the parent unfinished is the trigger's read of `t`.
            let cold = || {
                drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
                atoll.pool().evict_all_clean();
                atoll.store().meta().evict_all();
                for table in ["v", "logv", "v2", "logv2", "logw"] {
                    rows(&conn, &format!("SELECT count(*) FROM {table}"));
                }
            };
            let held = || {
                crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
                    atoll.requests_for_test(),
                )
                .expect("the IO thread held")
            };
            let landed = |wait: &crate::Completion| {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                while !wait.finished() && std::time::Instant::now() < deadline {
                    let _ = io.step();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                wait.finished()
            };
            // The wait continued by the next call.
            let mut next = conn.prepare("INSERT INTO v VALUES (1, 1)").unwrap();
            cold();
            let hold = held();
            let (stopped, stopped_out) = one_call(&mut next, DEFAULT_POLL_BUDGET);
            let stopped_unfinished = next.pending_io_for_test().is_some_and(|c| !c.finished());
            drop(hold);
            let next_landed = next.pending_io_for_test().is_some_and(|c| landed(&c));
            let (continued, _) = one_call(&mut next, DEFAULT_POLL_BUDGET);
            let mut next_calls = vec![stopped, continued];
            next_calls.extend(calls_waiting(&io, &mut next, DEFAULT_POLL_BUDGET));
            // The wait finished before its parent parks it.
            let mut inside = conn.prepare("INSERT INTO v VALUES (2, 2)").unwrap();
            cold();
            let hook_io = io.clone();
            let hook_landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let hook_saw = hook_landed.clone();
            let hold = held();
            BEFORE_PARENT_PARKS.with(|h| {
                *h.borrow_mut() = Some(Box::new(move |wait: &crate::Completion| {
                    drop(hold);
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                    while !wait.finished() && std::time::Instant::now() < deadline {
                        let _ = hook_io.step();
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    hook_saw.store(wait.finished(), std::sync::atomic::Ordering::SeqCst);
                }))
            });
            let (redone, _) = one_call(&mut inside, DEFAULT_POLL_BUDGET);
            // A hook that did not run is dropped here, and the hold with it.
            let hook_ran = BEFORE_PARENT_PARKS
                .with(|h| h.borrow_mut().take())
                .is_none();
            let mut inside_calls = vec![redone];
            inside_calls.extend(calls_waiting(&io, &mut inside, DEFAULT_POLL_BUDGET));
            // Two levels, the read landing as the top statement parks it: the first wait to move
            // up is the read's, into the middle trigger, and the hook only sets itself again.
            let mut deep = conn.prepare("INSERT INTO v2 VALUES (1, 1)").unwrap();
            cold();
            let deep_io = io.clone();
            let deep_landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let deep_saw = deep_landed.clone();
            let hold = held();
            BEFORE_PARENT_PARKS.with(|h| {
                *h.borrow_mut() = Some(Box::new(move |_: &crate::Completion| {
                    BEFORE_PARENT_PARKS.with(|again| {
                        *again.borrow_mut() = Some(Box::new(move |wait: &crate::Completion| {
                            drop(hold);
                            let deadline =
                                std::time::Instant::now() + std::time::Duration::from_secs(20);
                            while !wait.finished() && std::time::Instant::now() < deadline {
                                let _ = deep_io.step();
                                std::thread::sleep(std::time::Duration::from_millis(1));
                            }
                            deep_saw.store(wait.finished(), std::sync::atomic::Ordering::SeqCst);
                        }))
                    })
                }))
            });
            let (deep_redone, _) = one_call(&mut deep, DEFAULT_POLL_BUDGET);
            let deep_hook_ran = BEFORE_PARENT_PARKS
                .with(|h| h.borrow_mut().take())
                .is_none();
            let mut deep_calls = vec![deep_redone];
            deep_calls.extend(calls_waiting(&io, &mut deep, DEFAULT_POLL_BUDGET));
            // The dispatches before the top parks the landed read, counted in a first statement,
            // then a second one in a frame of those and one: the top's dispatch again spends the
            // last unit.
            let land = |hold: std::sync::mpsc::Sender<()>,
                        seen: Arc<std::sync::atomic::AtomicU64>|
             -> crate::storage::atoll::cooperative::ParentParks {
                let land_io = io.clone();
                Box::new(move |wait: &crate::Completion| {
                    seen.store(
                        DISPATCHED.with(|d| d.get()),
                        std::sync::atomic::Ordering::SeqCst,
                    );
                    drop(hold);
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                    while !wait.finished() && std::time::Instant::now() < deadline {
                        let _ = land_io.step();
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                })
            };
            let mut count = conn.prepare("INSERT INTO v VALUES (3, 3)").unwrap();
            cold();
            let seen = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let hold = held();
            BEFORE_PARENT_PARKS.with(|h| *h.borrow_mut() = Some(land(hold, seen.clone())));
            let start = counted();
            let (counted_call, _) = one_call(&mut count, DEFAULT_POLL_BUDGET);
            let count_hook_ran = BEFORE_PARENT_PARKS
                .with(|h| h.borrow_mut().take())
                .is_none();
            let to_park = seen.load(std::sync::atomic::Ordering::SeqCst) - start.0;
            let mut count_calls = vec![counted_call];
            count_calls.extend(calls_waiting(&io, &mut count, DEFAULT_POLL_BUDGET));
            let mut exact = conn.prepare("INSERT INTO v VALUES (4, 4)").unwrap();
            cold();
            let hold = held();
            BEFORE_PARENT_PARKS.with(|h| *h.borrow_mut() = Some(land(hold, seen.clone())));
            let (spent, spent_out) = one_call(&mut exact, (to_park + 1) as u32);
            let exact_hook_ran = BEFORE_PARENT_PARKS
                .with(|h| h.borrow_mut().take())
                .is_none();
            let (after_spent, _) = one_call(&mut exact, DEFAULT_POLL_BUDGET);
            let mut exact_calls = vec![spent, after_spent];
            exact_calls.extend(calls_waiting(&io, &mut exact, DEFAULT_POLL_BUDGET));
            drop((next, inside, deep, count, exact));
            let logged = rows(
                &conn,
                "SELECT (SELECT count(*) FROM logv), (SELECT sum(a) FROM logv), (SELECT sum(a) FROM logw)",
            );
            let expected = rows(
                &conn,
                "SELECT 4, 10 + 4 * (SELECT sum(length(b)) FROM t), 1 + (SELECT sum(length(b)) FROM t)",
            );
            assert!(
                matches!(stopped_out, Ok(StepResult::IO)) && stopped_unfinished,
                "the trigger's cold read did not return an unfinished wait: {stopped_out:?}"
            );
            assert!(next_landed, "the trigger's read never landed");
            assert_eq!(
                continued.continued, 2,
                "the call that continued the trigger's read: the running states it continued"
            );
            assert!(hook_ran, "no trigger's wait reached its parent unfinished");
            assert!(
                hook_landed.load(std::sync::atomic::Ordering::SeqCst),
                "the trigger's read did not land before its parent parked it"
            );
            assert_eq!(
                (redone.continued, redone.free),
                (0, 0),
                "the call that dispatched the trigger's read again inside itself: (continued, \
                 free)"
            );
            assert!(
                deep_hook_ran,
                "with two levels, no wait reached the top statement unfinished"
            );
            assert!(
                deep_landed.load(std::sync::atomic::Ordering::SeqCst),
                "with two levels, the read did not land before the top statement parked it"
            );
            assert_eq!(
                (deep_redone.continued, deep_redone.free),
                (0, 0),
                "with two levels, the call that dispatched the read again inside itself: \
                 (continued, free)"
            );
            assert!(
                count_hook_ran && exact_hook_ran,
                "a wait did not reach the top statement unfinished: (counting, exact) = \
                 ({count_hook_ran}, {exact_hook_ran})"
            );
            assert!(
                matches!(spent_out, Ok(StepResult::IO | StepResult::Yield))
                    && spent.dispatched == to_park + 1,
                "the call whose dispatch again spent the last unit: {spent_out:?} after {spent:?}, \
                 {to_park} dispatches before the park"
            );
            assert_eq!(
                after_spent.continued, 2,
                "the call after the parent stopped before its trigger: the running states it \
                 continued"
            );
            for (name, calls) in [
                ("continued", &next_calls),
                ("inside", &inside_calls),
                ("two levels inside", &deep_calls),
                ("counting", &count_calls),
                ("the last unit spent inside", &exact_calls),
            ] {
                if let Some((i, call)) = over_cap(calls, DEFAULT_POLL_BUDGET) {
                    panic!("{name}: call {i} ran past its cap: {call:?}");
                }
                assert_eq!(
                    unconserved(calls),
                    None,
                    "{name}: the units charged over the run were not its dispatches"
                );
                let most = calls.iter().map(|c| c.dispatched).max().unwrap();
                assert!(
                    most <= u64::from(DEFAULT_POLL_BUDGET),
                    "{name}: a call ran {most} units"
                );
            }
            assert_eq!(logged, expected, "the triggers' rows");
        },
    );
}

/// A poll's step that dispatches nothing charges nothing: a statement stopped inside a cold read,
/// its IO thread held, is called again in a frame of 100 while its read has not landed, and
/// dispatches nothing; a second statement, outside Atoll, called next in the same frame runs the
/// 100 exactly.
#[test]
fn a_call_that_dispatches_nothing_leaves_the_budget_as_it_found_it() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_call_that_dispatches_nothing_leaves_the_budget_as_it_found_it"
        ),
        || {
            let (io, db, conn) = async_atoll("/db/budget-credit-back", 200);
            let atoll = db.atoll.as_ref().unwrap().atoll();
            let mut cold = conn.prepare("SELECT sum(length(b)) FROM t").unwrap();
            // The second statement outside Atoll: it needs no IO, so only the frame's budget stops
            // it, and the frame is the thread's, whatever the connection.
            let (_pio, _pdb, plain_conn) = plain("budget-credit-back-plain.db");
            let mut other = plain_conn
                .prepare("SELECT count(*) FROM generate_series(1, 100000)")
                .unwrap();
            drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
            atoll.pool().evict_all_clean();
            atoll.store().meta().evict_all();
            let hold = crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
                atoll.requests_for_test(),
            )
            .expect("the IO thread held");
            let (stopped, stopped_out) = one_call(&mut cold, DEFAULT_POLL_BUDGET);
            let before = counted();
            let (again, mid, shared) = {
                let _frame = Frame::with_budget(100);
                let again = polled(&mut cold);
                let mid = counted();
                let shared = polled(&mut other);
                (again, mid, shared)
            };
            let after = counted();
            drop(hold);
            if let Some(c) = cold.pending_io_for_test() {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                while !c.finished() && std::time::Instant::now() < deadline {
                    let _ = io.step();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
            let rest = calls_waiting(&io, &mut cold, DEFAULT_POLL_BUDGET);
            drop((cold, other));
            assert!(
                matches!(stopped_out, Ok(StepResult::IO)) && stopped.dispatched > 0,
                "the cold read did not stop the scan: {stopped_out:?} after {stopped:?}"
            );
            assert!(
                matches!(again, Ok(StepResult::IO)),
                "the scan, its read not landed, came to {again:?}"
            );
            assert_eq!(
                mid.0 - before.0,
                0,
                "the scan, its read not landed, dispatched"
            );
            assert!(
                matches!(shared, Ok(StepResult::IO)),
                "the second statement did not stop for the frame's budget: {shared:?}"
            );
            assert_eq!(
                after.0 - mid.0,
                100,
                "the second statement in the frame of 100: its instructions"
            );
            assert_eq!(
                (after.1 - before.1, after.2 - before.2),
                (0, 0),
                "the frame of 100: (continued, free)"
            );
            if let Some((i, call)) = over_cap(&rest, DEFAULT_POLL_BUDGET) {
                panic!("the scan after its read landed: call {i} ran past its cap: {call:?}");
            }
            assert_eq!(
                unconserved(&rest),
                None,
                "the scan after its read landed: the units charged over the run were not its dispatches"
            );
        },
    );
}

/// Calls `stmt` in frames of `budget` until it is done, counting each call, on a database opened
/// async: a wait it returns is waited for outside the frame.
fn calls_waiting(io: &Arc<dyn IO>, stmt: &mut crate::Statement, budget: u32) -> Vec<Call> {
    let mut calls = Vec::new();
    for _ in 0..10_000_000 {
        let (call, out) = one_call(stmt, budget);
        calls.push(call);
        match out.unwrap() {
            StepResult::Done => return calls,
            StepResult::Row => {}
            StepResult::IO | StepResult::Yield => {
                if let Some(c) = stmt.pending_io_for_test() {
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
            }
            other => panic!("a step in a frame came to {other:?}"),
        }
    }
    panic!("not done in 10 000 000 calls");
}

/// The auditor's #5758 item 4: `Statement::reset_metrics` sets `vm_steps` back to 0 between calls,
/// and each call's mark is where that call began, so the calls after it are charged whole. Each
/// call in frames of 100 runs at most 100, and the scan ends with its synchronous result.
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
            let run = counted();
            for i in 0..1_000_000 {
                if i == 20 {
                    stmt.reset_metrics();
                }
                let before = DISPATCHED.with(|d| d.get());
                let out = {
                    let _frame = Frame::with_budget(100);
                    polled(&mut stmt).unwrap()
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
            let unconserved = unconserved_since(run);
            drop(stmt);
            assert_eq!(
                unconserved, None,
                "the scan, its counters reset: the units charged over the run were not its first \
                 dispatches (charged, dispatched)"
            );
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
    assert_eq!(
        polls.unconserved(),
        None,
        "the function call: the units charged were not its first dispatches"
    );
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
    let mut polls = Polls::new();
    for _ in 0..10_000_000 {
        let before = counted();
        let out = {
            let _frame = Frame::with_budget(budget);
            polled(stmt)
        };
        let after = counted();
        polls.dispatched.push(after.0 - before.0);
        polls.continued.push(after.1 - before.1);
        polls.free.push(after.2 - before.2);
        polls.charged.push(after.3 - before.3);
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
/// budget. Every call runs at most the budget, none of it with the budget spent, and over each run
/// the units charged are its dispatches; each statement stops for its budget many times, and each ends with what the synchronous API gives on a second such database. Here the
/// cached triggers enter frames with their own countdowns, which their poll's step cuts to what the
/// budget has left (condition 1).
#[test]
fn each_call_of_an_atoll_statement_in_a_frame_runs_at_most_its_budget() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::each_call_of_an_atoll_statement_in_a_frame_runs_at_most_its_budget"
        ),
        || {
            let bound = u64::from(DEFAULT_POLL_BUDGET);
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
                    "{name} on Atoll: a call in a frame ran {} units, past its budget ({bound})",
                    p.most()
                );
                assert!(
                    p.most_continued() <= 2,
                    "{name} on Atoll: a call in a frame continued {} program states, past the \
                     nesting depth and one",
                    p.most_continued()
                );
                assert_eq!(
                    p.free.iter().sum::<u64>(),
                    0,
                    "{name} on Atoll: a call dispatched with the budget spent, which is above the \
                     nesting depth"
                );
                assert_eq!(
                    p.over_cap(DEFAULT_POLL_BUDGET),
                    None,
                    "{name} on Atoll: a call ran past its cap (call, dispatched, continued, free)"
                );
                assert_eq!(
                    p.unconserved(),
                    None,
                    "{name} on Atoll: the units charged over the run were not its dispatches (charged, dispatched)"
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

/// A drawn run beside the named cases (#5820 item 2), seeded, its draws printed with any failure:
/// on a database opened async in Atoll mode, statements drawn from a fixed list (a scan, a scan of
/// rows, inserts firing one and two levels of triggers, an insert returning its rows, a delete
/// cascading to a child table), each call's budget drawn from 1 to 300, a frame of no budget or a
/// reset of the statement thrown in at drawn calls (a write's before its ending begins, and not
/// the insert returning its rows, whose changes a reset keeps), the pages made cold before a drawn
/// statement
/// and the IO thread held through a drawn call. Every call runs at most max(budget, continued + 1),
/// its free dispatches its continued states' first, and at most the budget besides; each
/// statement's run conserves its units (charged = dispatched) and gives the rows the synchronous API gives on a second database; and the
/// two databases end alike.
#[test]
fn drawn_statements_budgets_and_waits_keep_the_bound_and_conserve_the_units() {
    use rand::{Rng, SeedableRng};
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::drawn_statements_budgets_and_waits_keep_the_bound_and_conserve_the_units"
        ),
        || {
            const SEED: u64 = 5820;
            let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(SEED);
            let (io, db, conn) = async_atoll("/db/budget-drawn", 40);
            let (_sio, _sdb, sync) = async_atoll("/db/budget-drawn-sync", 40);
            let atoll = db.atoll.as_ref().unwrap().atoll();
            for c in [&conn, &sync] {
                for sql in [
                    "PRAGMA foreign_keys = ON",
                    "CREATE TABLE big(a INTEGER PRIMARY KEY, b)",
                    "INSERT INTO big SELECT value, value FROM generate_series(1, 300)",
                    "CREATE TABLE v(a, b)",
                    "CREATE TABLE logv(a)",
                    "CREATE TRIGGER trv AFTER INSERT ON v BEGIN INSERT INTO logv VALUES (new.a + (SELECT sum(b) FROM big)); END",
                    "CREATE TABLE v2(a, b)",
                    "CREATE TABLE logv2(a)",
                    "CREATE TABLE logw(a)",
                    "CREATE TRIGGER trv2 AFTER INSERT ON v2 BEGIN INSERT INTO logv2 VALUES (new.a + (SELECT sum(b) FROM big)); END",
                    "CREATE TRIGGER trl AFTER INSERT ON logv2 BEGIN INSERT INTO logw VALUES (new.a + (SELECT sum(b) FROM big)); END",
                    "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
                    "CREATE TABLE child(id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
                    "INSERT INTO parent SELECT value FROM generate_series(1, 100)",
                    "INSERT INTO child SELECT value, value % 100 + 1 FROM generate_series(1, 300)",
                ] {
                    c.execute(sql).unwrap();
                }
            }
            rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
            rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
            let mut log: Vec<String> = Vec::new();
            let mut failures: Vec<String> = Vec::new();
            let (mut next, mut deleted) = (1_000i64, 0i64);
            for s in 0..40 {
                let sql = match rng.random_range(0..6) {
                    0 => "SELECT count(*), sum(length(b)) FROM t".to_string(),
                    1 => "SELECT a FROM t WHERE a % 3 = 0".to_string(),
                    2 => {
                        next += 3;
                        format!(
                            "INSERT INTO v SELECT value, value FROM generate_series({}, {})",
                            next - 3,
                            next - 1
                        )
                    }
                    3 => {
                        next += 2;
                        format!(
                            "INSERT INTO v2 SELECT value, value FROM generate_series({}, {})",
                            next - 2,
                            next - 1
                        )
                    }
                    4 => {
                        next += 3;
                        format!(
                            "INSERT INTO v SELECT value, value FROM generate_series({}, {}) RETURNING a, b",
                            next - 3,
                            next - 1
                        )
                    }
                    _ => {
                        deleted += 1;
                        format!("DELETE FROM parent WHERE id = {deleted}")
                    }
                };
                let expected = rows(&sync, &sql);
                let mut stmt = conn.prepare(&sql).unwrap();
                if rng.random_bool(0.5) {
                    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
                    atoll.pool().evict_all_clean();
                    atoll.store().meta().evict_all();
                    log.push(format!("statement {s}: pages cold"));
                }
                log.push(format!("statement {s}: {sql}"));
                let (mut calls, mut got, mut reset) = (Vec::new(), Vec::new(), false);
                let mut ended = false;
                for c in 0..1_000_000usize {
                    let budget = if rng.random_ratio(1, 20) {
                        0
                    } else {
                        rng.random_range(1..=300)
                    };
                    // A reset rolls a write back until its ending begins; from then the ending is
                    // carried to its end (DESIGN.md 22.18). An insert returning its rows has made
                    // its changes before its first row, as in SQLite.
                    let draw_reset = rng.random_ratio(1, 30);
                    if !reset
                        && draw_reset
                        && !sql.contains("RETURNING")
                        && !stmt.ending_begun_for_test()
                    {
                        stmt.reset().unwrap();
                        got.clear();
                        reset = true;
                        log.push(format!("statement {s} call {c}: reset"));
                    }
                    let hold = rng.random_ratio(1, 4).then(|| {
                        crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
                            atoll.requests_for_test(),
                        )
                        .expect("the IO thread held")
                    });
                    let held = hold.is_some();
                    let (call, out) = one_call(&mut stmt, budget);
                    drop(hold);
                    log.push(format!(
                        "statement {s} call {c}: budget {budget} held {held}: {call:?} {out:?}"
                    ));
                    if past_cap(budget, call.dispatched, call.continued, call.free)
                        || call.continued > 3
                        || call.charged != call.dispatched
                    {
                        failures.push(format!(
                            "statement {s} call {c}, budget {budget}: {call:?} (at most max(budget, \
                             continued + 1), free at most continued, continued at most 3, charged = \
                             dispatched)"
                        ));
                    }
                    calls.push(call);
                    match out {
                        Ok(StepResult::Done) => {
                            ended = true;
                            break;
                        }
                        Ok(StepResult::Row) => got.push(
                            stmt.row()
                                .unwrap()
                                .get_values()
                                .cloned()
                                .collect::<Vec<_>>(),
                        ),
                        Ok(StepResult::IO | StepResult::Yield) => {
                            if let Some(wait) = stmt.pending_io_for_test() {
                                let deadline =
                                    std::time::Instant::now() + std::time::Duration::from_secs(20);
                                while !wait.finished() && std::time::Instant::now() < deadline {
                                    let _ = io.step();
                                    std::thread::sleep(std::time::Duration::from_millis(1));
                                }
                            }
                        }
                        other => {
                            failures.push(format!("statement {s} call {c}: came to {other:?}"));
                            break;
                        }
                    }
                }
                drop(stmt);
                if !ended {
                    failures.push(format!("statement {s}: not done"));
                }
                if let Some((units, first)) = unconserved(&calls) {
                    failures.push(format!(
                        "statement {s}: {units} units charged less given back over the run, {first} \
                         first dispatches"
                    ));
                }
                if got != expected {
                    failures.push(format!(
                        "statement {s}: rows {got:?}, the synchronous API's {expected:?}"
                    ));
                }
                if !failures.is_empty() {
                    break;
                }
            }
            let compared: Vec<_> = ["v", "logv", "v2", "logv2", "logw", "parent", "child"]
                .iter()
                .map(|table| {
                    let check = if matches!(*table, "parent" | "child") {
                        format!("SELECT count(*), sum(id) FROM {table}")
                    } else {
                        format!("SELECT count(*), sum(a) FROM {table}")
                    };
                    (check.clone(), rows(&conn, &check), rows(&sync, &check))
                })
                .collect();
            let tail = log[log.len().saturating_sub(40)..].join("\n");
            assert!(
                failures.is_empty(),
                "seed {SEED}: {}\nthe last draws:\n{tail}",
                failures.join("; ")
            );
            for (check, polled, synced) in compared {
                assert_eq!(polled, synced, "seed {SEED}: {check}");
            }
            println!("seed {SEED}: {} draws", log.len());
        },
    );
}

/// One poll's step of `stmt`, as a binding's poll makes it, inside the caller's frame.
fn polled(stmt: &mut crate::Statement) -> crate::Result<StepResult> {
    stmt.step_in_frame(std::task::Waker::noop())
}
