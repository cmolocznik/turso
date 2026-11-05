//! The wait graph: cycles found and broken by their youngest member, over holders faked and over
//! real transactions, many threads deadlocking at once, and, under loom, the flag and park
//! pairing.

use super::*;
use crate::storage::atoll::txn::Ts;
use std::collections::HashMap;

/// Holders named by hand: a blocker's group while the test says it runs.
#[derive(Default)]
struct Fake(std::sync::Mutex<HashMap<(u16, Ts), GroupId>>);

impl Fake {
    fn runs(&self, slot: u16, group: GroupId) -> Blocker {
        let blocker = Blocker {
            slot,
            start: 100 + slot as Ts,
        };
        self.0
            .lock()
            .unwrap()
            .insert((blocker.slot, blocker.start), group);
        blocker
    }

    #[cfg(not(atoll_loom))]
    fn ends(&self, blocker: Blocker) {
        self.0
            .lock()
            .unwrap()
            .remove(&(blocker.slot, blocker.start));
    }
}

impl Holders for Fake {
    fn holder(&self, blocker: Blocker) -> Option<GroupId> {
        self.0
            .lock()
            .unwrap()
            .get(&(blocker.slot, blocker.start))
            .copied()
    }
}

fn row(db: &Arc<Fake>, blocker: Blocker) -> Edge {
    Edge::Row {
        db: db.clone(),
        blocker,
    }
}

#[cfg(not(atoll_loom))]
mod plain {
    use super::*;
    use crate::io::{MemoryIO, IO};
    use crate::storage::atoll::btree::Mode;
    use crate::storage::atoll::sql::catalog::tests::config;
    use crate::storage::atoll::sql::AtollDb;
    use crate::storage::atoll::testing::drive;
    use crate::storage::atoll::txn::versioned::{VersionedTree, Write, WriteOp};
    use crate::storage::atoll::txn::{Abort, Commit, Isolation, Txn};
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    /// Two groups wait on each other: whichever closes the cycle, the younger is the victim, and
    /// a victim already parked is woken.
    #[test]
    fn two_groups_waiting_on_each_other_flag_the_younger() {
        for younger_closes in [true, false] {
            let graph = WaitGraph::default();
            let db = Arc::new(Fake::default());
            let (sa, sb) = (graph.seat(), graph.seat());
            let a = graph.begin(&sa);
            let b = graph.begin(&sb);
            let (ba, bb) = (db.runs(0, a), db.runs(1, b));
            if younger_closes {
                let ra = graph.publish(a, row(&db, bb)).unwrap();
                assert_eq!(graph.publish(b, row(&db, ba)).err(), Some(Deadlock));
                assert!(graph.doomed(b) && !graph.doomed(a));
                assert!(!ra.flagged());
                graph.withdraw(&ra).unwrap();
            } else {
                let rb = graph.publish(b, row(&db, ba)).unwrap();
                let c = Completion::new_wait();
                graph.park(&rb, &c).unwrap();
                let ra = graph.publish(a, row(&db, bb)).unwrap();
                assert!(rb.flagged() && c.finished(), "the parked victim woken");
                assert_eq!(graph.withdraw(&rb), Err(Deadlock));
                graph.withdraw(&ra).unwrap();
            }
        }
    }

    /// Three groups in a cycle, the youngest holding a gate exclusive: the youngest of the others
    /// is the victim; with every member holding one, the youngest is.
    #[test]
    fn a_member_holding_an_exclusive_gate_is_spared_unless_every_member_holds_one() {
        for all_hold in [false, true] {
            let graph = WaitGraph::default();
            let db = Arc::new(Fake::default());
            let seats: Vec<Seat> = (0..3).map(|_| graph.seat()).collect();
            let g: Vec<GroupId> = seats.iter().map(|s| graph.begin(s)).collect();
            let b: Vec<Blocker> = (0..3).map(|i| db.runs(i as u16, g[i])).collect();
            graph.exclusive(g[2], true);
            if all_hold {
                graph.exclusive(g[0], true);
                graph.exclusive(g[1], true);
            }
            let r0 = graph.publish(g[0], row(&db, b[1])).unwrap();
            let r2 = graph.publish(g[2], row(&db, b[0])).unwrap();
            let closing = graph.publish(g[1], row(&db, b[2]));
            let victim = if all_hold { 2 } else { 1 };
            assert_eq!(closing.is_err(), victim == 1, "all hold: {all_hold}");
            assert_eq!(r2.flagged(), victim == 2, "all hold: {all_hold}");
            assert!(!r0.flagged());
            for i in 0..3 {
                assert_eq!(
                    graph.doomed(g[i]),
                    i == victim,
                    "group {i}, all hold: {all_hold}"
                );
            }
        }
    }

    /// A cycle through a holder whose transaction has ended, or through a group that has ended,
    /// is no cycle; one across two databases is.
    /// A cycle behind an edge to an ended group, whose seat a later group waiting into the cycle
    /// now holds, is found and broken: the search reads only a live group's node, so the false
    /// path through the seat's new group, which the cycle's check refuses, hides nothing behind
    /// it. Here `g1`'s first wait is on the ended group's transaction, still named, and its second
    /// closes the cycle with `a`.
    #[test]
    fn a_cycle_behind_an_ended_groups_reused_seat_is_broken() {
        let graph = WaitGraph::default();
        let db = Arc::new(Fake::default());
        let seats: Vec<Seat> = (0..3).map(|_| graph.seat()).collect();
        let (a, g1, old) = (
            graph.begin(&seats[0]),
            graph.begin(&seats[1]),
            graph.begin(&seats[2]),
        );
        let (ba, b1, bold) = (db.runs(0, a), db.runs(1, g1), db.runs(2, old));
        graph.end(old);
        let next = graph.begin(&seats[2]);
        let rn = graph.publish(next, row(&db, ba)).unwrap();
        let _r1a = graph.publish(g1, row(&db, bold)).unwrap();
        let r1b = graph.publish(g1, row(&db, ba)).unwrap();
        let ra = graph.publish(a, row(&db, b1));
        assert!(
            ra.is_err() || r1b.flagged(),
            "the cycle through g1 was not broken"
        );
        assert!(
            !rn.flagged() && !graph.doomed(next),
            "the seat's new group closes no cycle"
        );
    }

    #[test]
    fn ended_holders_and_groups_close_no_cycle_and_two_databases_close_one() {
        let graph = WaitGraph::default();
        let (db1, db2) = (Arc::new(Fake::default()), Arc::new(Fake::default()));
        let seats: Vec<Seat> = (0..6).map(|_| graph.seat()).collect();
        let g: Vec<GroupId> = seats.iter().map(|s| graph.begin(s)).collect();
        // The holder of one edge has ended.
        let (b0, b1) = (db1.runs(0, g[0]), db2.runs(0, g[1]));
        let r0 = graph.publish(g[0], row(&db2, b1)).unwrap();
        db2.ends(b1);
        let r1 = graph.publish(g[1], row(&db1, b0)).unwrap();
        assert!(
            !r0.flagged() && !r1.flagged(),
            "a holder gone closes nothing"
        );
        // The group one edge names has ended, though its blocker still names it.
        let (b2, b3) = (db1.runs(1, g[2]), db2.runs(1, g[3]));
        let r3 = graph.publish(g[3], row(&db1, b2)).unwrap();
        graph.end(g[3]);
        // The seat's next group waits on the same blocker: an edge to the ended group must not
        // reach it.
        let next = graph.begin(&seats[3]);
        let rn = graph.publish(next, row(&db1, b2)).unwrap();
        let r2 = graph.publish(g[2], row(&db2, b3)).unwrap();
        assert!(
            !rn.flagged() && !graph.doomed(next),
            "an ended group's seat closes nothing"
        );
        assert!(
            !r2.flagged() && !graph.doomed(g[2]),
            "an ended group closes nothing"
        );
        assert_eq!(
            graph.withdraw(&r3),
            Ok(()),
            "a record its group's end closed"
        );
        // Two databases.
        let (b4, b5) = (db1.runs(2, g[4]), db2.runs(2, g[5]));
        let r4 = graph.publish(g[4], row(&db2, b5)).unwrap();
        assert_eq!(graph.publish(g[5], row(&db1, b4)).err(), Some(Deadlock));
        assert!(!r4.flagged() && graph.doomed(g[5]));
    }

    struct Db {
        io: Arc<dyn IO>,
        db: Arc<AtollDb>,
        tree: VersionedTree,
    }

    fn open() -> Db {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let mut config = config();
        config.txn.slots = 16;
        let db = Arc::new(AtollDb::open(io.clone(), "/db/wait", config).unwrap());
        let a = db.atoll();
        let root = drive(&io, || {
            crate::storage::atoll::btree::Tree::create(
                &io,
                a.pool(),
                a.store(),
                crate::storage::atoll::page::SizeClass::new(0).unwrap(),
            )
        })
        .unwrap();
        let tree = VersionedTree::open(io.clone(), a.pool().clone(), a.store().clone(), root);
        let d = Db { io, db, tree };
        let mut t = d.begin(0);
        for k in 0..4 {
            assert!(matches!(d.put(&mut t, k), Write::Done(_)));
        }
        d.commit(t);
        d
    }

    impl Db {
        fn begin(&self, group: GroupId) -> Txn {
            let txns = self.db.atoll().transactions();
            let mut t = drive(&self.io, || txns.begin_in(Isolation::ReadCommitted, group)).unwrap();
            crate::storage::atoll::testing::share(txns, &mut t);
            txns.statement(&mut t).unwrap();
            t
        }

        fn put(&self, txn: &mut Txn, key: u8) -> Write {
            let mut op = WriteOp::put(&[key], vec![key; 8], Mode::Upsert);
            let txns = self.db.atoll().transactions();
            drive(&self.io, || {
                self.tree.write(txns, self.db.versions(), txn, &mut op)
            })
            .unwrap()
        }

        fn commit(&self, txn: Txn) {
            let mut op = Commit::new(txn);
            drive(&self.io, || self.db.atoll().transactions().commit(&mut op)).unwrap();
        }

        fn rollback(&self, txn: Txn) {
            let mut op = Abort::new(txn);
            let txns = self.db.atoll().transactions();
            drive(&self.io, || txns.rollback(self.db.versions(), &mut op)).unwrap();
        }

        /// Writes `key` as the SQL layer will: on a writer still open it publishes an edge,
        /// registers on that writer's end, writes again, and parks only while it still waits on
        /// the same writer; `Deadlock` once its group is a victim. Returns whether it parked.
        fn put_waiting(
            &self,
            graph: &WaitGraph,
            group: GroupId,
            txn: &mut Txn,
            key: u8,
        ) -> Result<bool, Deadlock> {
            let mut wait: Option<(Blocker, Completion, Arc<WaitRecord>)> = None;
            let mut parked = false;
            loop {
                if graph.doomed(group) {
                    if let Some((_, _, r)) = wait.take() {
                        graph.withdraw(&r).ok();
                    }
                    return Err(Deadlock);
                }
                match self.put(txn, key) {
                    Write::Done(_) => {
                        if let Some((_, _, r)) = wait.take() {
                            graph.withdraw(&r)?;
                        }
                        return Ok(parked);
                    }
                    Write::Conflict => panic!("a conflict under read committed"),
                    Write::Wait(blocker) => match &wait {
                        Some((b, c, r)) if *b == blocker && !c.finished() => {
                            graph.park(r, c)?;
                            parked = true;
                            let deadline =
                                std::time::Instant::now() + std::time::Duration::from_secs(60);
                            while !c.finished() {
                                assert!(std::time::Instant::now() < deadline, "never woken");
                                crate::thread::yield_now();
                            }
                        }
                        _ => {
                            if let Some((_, _, r)) = wait.take() {
                                graph.withdraw(&r)?;
                            }
                            let edge = Edge::Row {
                                db: self.db.clone(),
                                blocker,
                            };
                            let r = graph.publish(group, edge)?;
                            let c = Completion::new_wait();
                            self.db.atoll().transactions().wait_end(blocker, &c);
                            wait = Some((blocker, c, r));
                        }
                    },
                }
            }
        }
    }

    /// Two real transactions cross over two keys: the younger is the victim, rolls back, and the
    /// older's parked write goes on.
    #[test]
    fn transactions_crossing_two_keys_deadlock_and_the_younger_rolls_back() {
        let d = open();
        let graph = WaitGraph::default();
        let (sa, sb) = (graph.seat(), graph.seat());
        let (a, b) = (graph.begin(&sa), graph.begin(&sb));
        let (mut ta, mut tb) = (d.begin(a), d.begin(b));
        d.put_waiting(&graph, a, &mut ta, 0).unwrap();
        d.put_waiting(&graph, b, &mut tb, 1).unwrap();
        let older = {
            let d = &d;
            let graph = &graph;
            std::thread::scope(|s| {
                let h = s.spawn(move || {
                    let parked = d.put_waiting(graph, a, &mut ta, 1);
                    (parked, ta)
                });
                // Parked, not only published: a rollback that lands before the park would leave
                // the older's next write nothing to wait on.
                while !graph.waits_of(a).iter().any(|r| !r.flagged() && r.parked()) {
                    std::thread::yield_now();
                }
                assert_eq!(d.put_waiting(graph, b, &mut tb, 0), Err(Deadlock));
                d.rollback(tb);
                graph.end(b);
                h.join().unwrap()
            })
        };
        let (parked, ta) = older;
        assert_eq!(parked, Ok(true), "the older parked and went on");
        d.commit(ta);
        graph.end(a);
    }

    /// Many groups write random keys in random orders, parking on one another: every deadlock is
    /// broken, every group finishes, and none is left waiting.
    fn crossing_writers_all_finish(threads: usize, rounds: usize, seed: u64) -> usize {
        let d = Arc::new(open());
        let graph = Arc::new(WaitGraph::default());
        let workers: Vec<_> = (0..threads)
            .map(|w| {
                let (d, graph) = (d.clone(), graph.clone());
                crate::thread::spawn(move || {
                    let seat = graph.seat();
                    let mut rng = ChaCha8Rng::seed_from_u64(seed * 1000 + w as u64);
                    let mut deadlocks = 0;
                    let mut done = 0;
                    while done < rounds {
                        let group = graph.begin(&seat);
                        let mut t = d.begin(group);
                        let mut keys: Vec<u8> = (0..4).collect();
                        for i in (1..keys.len()).rev() {
                            keys.swap(i, rng.random_range(0..=i));
                        }
                        let ok = keys[..3]
                            .iter()
                            .all(|&k| d.put_waiting(&graph, group, &mut t, k).is_ok());
                        if ok {
                            d.commit(t);
                            done += 1;
                        } else {
                            d.rollback(t);
                            deadlocks += 1;
                        }
                        graph.end(group);
                    }
                    graph.unseat(seat);
                    deadlocks
                })
            })
            .collect();
        let deadlocks = workers.into_iter().map(|w| w.join().unwrap()).sum();
        for node in lock(&graph.nodes).iter() {
            assert!(lock(&node.waits).is_empty(), "a wait left behind");
        }
        deadlocks
    }

    #[test]
    fn crossing_writers_all_finish_on_twelve_threads() {
        let deadlocks = crossing_writers_all_finish(12, 30, 1);
        println!("{deadlocks} deadlocks broken");
        assert!(deadlocks > 0, "no deadlock met");
    }

    #[cfg(shuttle)]
    #[test]
    fn crossing_writers_all_finish_in_shuttle_schedules() {
        // Outside any schedule, so std's: shuttle's atomics live inside one.
        let seed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        crate::storage::atoll::testing::schedules(
            move || {
                let s = seed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                crossing_writers_all_finish(3, 2, s);
            },
            300,
        );
    }
}

#[cfg(atoll_loom)]
mod loom_tests {
    use super::*;
    use ::loom::thread;

    /// A younger group waits and parks while an older one closes the cycle: the younger is the
    /// victim however the two interleave, and if it parked, the flag woke it.
    #[test]
    fn a_flag_reaches_a_parked_victim() {
        let mut model = ::loom::model::Builder::new();
        model.preemption_bound = Some(2);
        model.check(|| {
            let graph = Arc::new(WaitGraph::default());
            let db = Arc::new(Fake::default());
            let (so, sy) = (graph.seat(), graph.seat());
            let older = graph.begin(&so);
            let younger = graph.begin(&sy);
            let (bo, by) = (db.runs(0, older), db.runs(1, younger));
            let waiter = {
                let (graph, db) = (graph.clone(), db.clone());
                thread::spawn(move || {
                    let r = graph.publish(younger, row(&db, bo))?;
                    let c = Completion::new_wait();
                    graph.park(&r, &c)?;
                    Ok::<_, Deadlock>((r, c))
                })
            };
            let closer = graph.publish(older, row(&db, by));
            match waiter.join().unwrap() {
                Ok((r, c)) => {
                    assert!(
                        r.flagged(),
                        "the younger closed no cycle and was not flagged"
                    );
                    assert!(c.finished(), "a parked victim never woken");
                }
                Err(Deadlock) => {}
            }
            assert!(closer.is_ok(), "the older was made the victim");
            assert!(graph.doomed(younger) && !graph.doomed(older));
        });
    }
}
