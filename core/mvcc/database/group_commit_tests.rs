use crate::mvcc::database::{CommitCoordinator, GroupWork, LogRecord};
use crate::SyncMode;

fn empty_record(end_ts: u64) -> LogRecord {
    LogRecord::empty(end_ts, crate::alloc::DynAllocator::default())
}

#[test]
fn requeued_records_go_back_in_ticket_order() {
    let coordinator = CommitCoordinator::new();
    let first = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    let second = coordinator.enqueue(2, empty_record(20), SyncMode::Full);

    let batch = coordinator.take_pending();
    assert_eq!(
        batch.iter().map(|queued| queued.ticket).collect::<Vec<_>>(),
        vec![first, second]
    );

    let latecomer = coordinator.enqueue(3, empty_record(30), SyncMode::Full);
    coordinator.requeue(batch.into_iter());
    assert_eq!(
        coordinator
            .take_pending()
            .iter()
            .map(|queued| queued.ticket)
            .collect::<Vec<_>>(),
        vec![first, second, latecomer]
    );
}

#[test]
fn drop_pending_only_removes_queued_records() {
    let coordinator = CommitCoordinator::new();

    let queued = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    assert!(coordinator.drop_pending(queued));

    let claimed = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let _batch = coordinator.take_pending();
    assert!(
        !coordinator.drop_pending(claimed),
        "a record the leader already took is not in the queue"
    );
}

#[test]
fn durability_watermark_only_moves_forward() {
    let coordinator = CommitCoordinator::new();
    coordinator.note_written(7);
    coordinator.mark_durable(7);
    coordinator.mark_durable(3);
    assert_eq!(coordinator.durable_through(), 7);
}

#[test]
fn failed_leader_does_not_publish_unsynced_prefix() {
    let coordinator = CommitCoordinator::new();
    let first = coordinator.enqueue(1, empty_record(10), SyncMode::Full);
    let second = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let mut batch = coordinator.take_pending();
    let writing = batch.pop_front().unwrap();
    assert_eq!(writing.ticket, first);
    coordinator.note_written(first);
    coordinator.requeue(batch.into_iter());

    assert_eq!(
        coordinator.durable_through(),
        0,
        "owning log bytes is not the same as fsyncing them"
    );
    assert_eq!(coordinator.written_through(), first);

    coordinator.mark_durable(second);
    assert_eq!(
        coordinator.durable_through(),
        first,
        "durable cannot pass a ticket that was not owned"
    );
}

#[test]
fn failed_mid_batch_leader_does_not_cover_retry_hole() {
    let coordinator = CommitCoordinator::new();
    let t2 = coordinator.enqueue(2, empty_record(20), SyncMode::Full);
    let t3 = coordinator.enqueue(3, empty_record(30), SyncMode::Full);
    let t4 = coordinator.enqueue(4, empty_record(40), SyncMode::Full);
    assert_eq!((t2, t3, t4), (1, 2, 3));

    let mut batch = coordinator.take_pending();
    let _owned = batch.pop_front().unwrap();
    let _retry = batch.pop_front().unwrap();
    let later = batch.pop_front().unwrap();
    coordinator.note_written(t2);
    coordinator.request_retry(t3);
    coordinator.requeue(std::iter::once(later));
    coordinator.note_written(t4);
    coordinator.mark_durable(coordinator.written_through());

    assert!(
        coordinator.durable_through() < t3,
        "T3 is a retry hole; durable must not cover it (durable={}, t3={t3})",
        coordinator.durable_through()
    );
    assert!(
        !matches!(
            coordinator.take_work(),
            GroupWork::Lead { writing, .. } if writing.ticket >= t3
        ),
        "later tickets cannot be taken while an earlier ticket is a retry hole"
    );
    assert!(
        coordinator.take_retry(t3),
        "T3 must still be retrying, not acked via the watermark"
    );
}

/// Keeps, for each frame it receives, whether the frame's transaction was still `Preparing`: a
/// checkpoint covers only commits whose frames the replicator has, which it can tell only while the
/// transaction it receives a frame of counts as not yet committed.
#[cfg(feature = "conn_raw_api")]
#[derive(Default)]
struct PreparingWatch {
    store: std::sync::OnceLock<Arc<crate::MvStore>>,
    seen: std::sync::Mutex<Vec<(u64, bool)>>,
}

#[cfg(feature = "conn_raw_api")]
impl crate::LogReplicator for PreparingWatch {
    fn takes_commits(&self) -> bool {
        true
    }

    fn replicate(&self, frame: crate::ReplicatedFrame) -> crate::Completion {
        let store = self
            .store
            .get()
            .expect("the store is set before the first commit");
        let preparing = store.txs.iter().any(|entry| {
            entry.value().state.load()
                == crate::mvcc::database::TransactionState::Preparing(frame.commit_ts)
        });
        self.seen.lock().unwrap().push((frame.commit_ts, preparing));
        crate::Completion::new_yield()
    }
}

/// A group-commit leader that writes a waiter's frame after the waiter's commit was dropped
/// finishes the waiter's commit itself; the replicator still gets the frame before the waiter
/// counts as committed.
#[cfg(feature = "conn_raw_api")]
#[test]
fn an_abandoned_waiter_s_frame_reaches_the_replicator_while_it_is_still_preparing() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(
        crate::DatabaseOpts::new()
            .with_experimental_mvcc_passive_checkpoint(true)
            .with_mvcc_log_dropped_on_open(true),
    );
    let watch = Arc::new(PreparingWatch::default());
    let store = db.get_mvcc_store();
    assert!(watch.store.set(store.clone()).is_ok());
    db.db
        .as_ref()
        .unwrap()
        .set_log_replicator(watch.clone())
        .unwrap();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    setup.close().unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 1)").unwrap();
    for conn in [&conn_a, &conn_b] {
        conn.set_yield_injector(Some(FixedYieldInjector::new([
            CommitYieldPoint::LogRecordPrepared.point(),
            CommitYieldPoint::LogicalLogWriteIssued.point(),
        ])));
    }
    let coordinator = &store.commit_coordinator;
    assert!(coordinator.pager_commit_lock.write());
    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    assert!(matches!(
        step_until_yield_or_done(&mut commit_a),
        StepResult::Yield
    ));
    assert!(matches!(
        step_until_yield_or_done(&mut commit_b),
        StepResult::Yield
    ));
    assert!(matches!(commit_a.step().unwrap(), StepResult::IO));
    assert!(matches!(commit_b.step().unwrap(), StepResult::IO));
    coordinator.unlock_pager_commit_lock();
    assert!(matches!(
        step_until_yield_or_done(&mut commit_a),
        StepResult::Yield
    ));
    assert!(store.last_group_commit_size() >= 2);

    drop(commit_b);
    conn_a.set_yield_injector(None);
    step_until_done(&mut commit_a);

    let seen = watch.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3, "the table and both rows: {seen:?}");
    assert!(
        seen.iter().all(|(_, preparing)| *preparing),
        "a frame reached the replicator after its transaction counted as committed: {seen:?}"
    );
    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT pk FROM t ORDER BY pk"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
    );
}
