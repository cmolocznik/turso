//! Sequence values outside transactions, concurrent `nextval`, and the value rows open removes.

use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::sql::catalog::Catalog;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::{Abort, Commit};
use crate::sync::Arc;

const BASE: &str = "/db/sequence";

fn run<T>(io: &Arc<dyn IO>, op: impl FnMut() -> Result<IOResult<T>>) -> T {
    drive(io, op).unwrap()
}

/// `nextval`'s step with an increment of 1 and no bound: the value set, if no `nextval` has
/// returned it yet, else one past it.
fn nextval(v: Value) -> Result<Value> {
    Ok(Value {
        value: if v.is_called { v.value + 1 } else { v.value },
        is_called: true,
    })
}

fn begin(db: &AtollDb, io: &Arc<dyn IO>) -> Txn {
    let txns = db.atoll().transactions();
    let mut t = run(io, || txns.begin(Isolation::Snapshot));
    crate::storage::atoll::testing::share(txns, &mut t);
    txns.statement(&mut t).unwrap();
    t
}

fn commit(db: &AtollDb, io: &Arc<dyn IO>, t: Txn) {
    let mut op = Commit::new(t);
    run(io, || db.atoll().transactions().commit(&mut op));
}

fn rollback(db: &AtollDb, io: &Arc<dyn IO>, t: Txn) {
    let txns = db.atoll().transactions();
    let mut op = Abort::new(t);
    run(io, || txns.rollback(db.versions(), &mut op));
}

/// Sequence `id` created in `t`, starting at `start`; returns its value row's key.
fn create(db: &AtollDb, io: &Arc<dyn IO>, t: &mut Txn, id: u64, start: i64) -> [u8; 16] {
    let mut op = Create::new(
        id,
        Value {
            value: start,
            is_called: false,
        },
    );
    let w = run(io, || db.create_sequence(t, &mut op));
    assert_eq!(w, Write::Done(Outcome::Inserted));
    value_key(id, t.start())
}

fn draw(db: &AtollDb, io: &Arc<dyn IO>, key: [u8; 16]) -> i64 {
    let mut op = Change::new(key);
    run(io, || db.change_sequence(&mut op, &mut nextval)).value
}

/// The value rows' keys, read plainly.
fn rows(db: &AtollDb, io: &Arc<dyn IO>) -> Vec<Vec<u8>> {
    let tree = db.sequences();
    let mut cursor = Cursor::new();
    let mut out = Vec::new();
    let mut more = run(io, || tree.seek(&mut cursor, &[], Seek::Ge));
    while more {
        out.push(cursor.key().unwrap().to_vec());
        more = run(io, || tree.next(&mut cursor));
    }
    out
}

/// A value drawn stays drawn: a transaction that draws and rolls back gives nothing back, and the
/// value survives a restart.
#[test]
fn a_sequence_counts_outside_transactions_and_across_a_restart() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    let mut t = begin(&db, &io);
    let key = create(&db, &io, &mut t, 50, 1);
    commit(&db, &io, t);
    assert_eq!((draw(&db, &io, key), draw(&db, &io, key)), (1, 2));
    let t = begin(&db, &io);
    assert_eq!(draw(&db, &io, key), 3);
    rollback(&db, &io, t);
    let mut op = Change::new(key);
    let set = run(&io, || {
        db.change_sequence(&mut op, &mut |_| {
            Ok(Value {
                value: 40,
                is_called: false,
            })
        })
    });
    assert_eq!(set.value, 40, "setval");
    drop(db);
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    assert_eq!((draw(&db, &io, key), draw(&db, &io, key)), (40, 41));
}

/// A creation rolled back and a drop committed leave their value rows until the next open, which
/// removes them, keeping the row a committed catalog row names; a row is keyed by the creating
/// transaction's start, so another creation of the same id has a row of its own.
#[test]
fn open_removes_the_value_rows_no_committed_catalog_row_names() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    let mut t = begin(&db, &io);
    let undone = create(&db, &io, &mut t, 50, 1);
    rollback(&db, &io, t);
    let mut t = begin(&db, &io);
    let kept = create(&db, &io, &mut t, 50, 100);
    let dropped = create(&db, &io, &mut t, 51, 1);
    commit(&db, &io, t);
    assert_eq!(
        draw(&db, &io, kept),
        100,
        "its own row, not the undone one's"
    );
    assert_eq!(draw(&db, &io, undone), 1);
    let mut t = begin(&db, &io);
    let mut op = Catalog::delete_op(51);
    let txns = db.atoll().transactions();
    let w = run(&io, || {
        db.catalog().write(txns, db.versions(), &mut t, &mut op)
    });
    assert_eq!(w, Write::Done(Outcome::Updated));
    commit(&db, &io, t);
    assert_eq!(rows(&db, &io).len(), 3, "nothing removed while running");
    drop(db);
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    assert_eq!(rows(&db, &io), vec![kept.to_vec()]);
    assert_eq!(draw(&db, &io, kept), 101);
    let _ = dropped;
}

/// Many threads draw from one sequence at once: every value is drawn exactly once.
fn draws_from_many_threads_are_each_drawn_once(threads: usize, each: usize) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(AtollDb::open(io.clone(), BASE, config()).unwrap());
    let mut t = begin(&db, &io);
    let key = create(&db, &io, &mut t, 50, 1);
    commit(&db, &io, t);
    let drawers: Vec<_> = (0..threads)
        .map(|_| {
            let (db, io) = (db.clone(), io.clone());
            crate::thread::spawn(move || (0..each).map(|_| draw(&db, &io, key)).collect::<Vec<_>>())
        })
        .collect();
    let mut all: Vec<i64> = drawers
        .into_iter()
        .flat_map(|d| d.join().unwrap())
        .collect();
    all.sort_unstable();
    let expected: Vec<i64> = (1..=(threads * each) as i64).collect();
    assert_eq!(all, expected, "a value drawn twice or skipped");
}

#[test]
fn draws_from_twelve_threads_are_each_drawn_once() {
    draws_from_many_threads_are_each_drawn_once(12, 300);
}

#[cfg(shuttle)]
#[test]
fn draws_from_many_threads_are_each_drawn_once_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || draws_from_many_threads_are_each_drawn_once(3, 3),
        300,
    );
}
