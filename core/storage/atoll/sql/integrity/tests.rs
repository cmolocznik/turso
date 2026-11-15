//! The page arm of an integrity check: a database in use accounts for every page, through
//! `PRAGMA integrity_check` and directly, and each fault planted behind the trees' backs is found.

use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::Check;
use crate::storage::atoll::drive;
use crate::storage::atoll::page::PageId;
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::sql::AtollDb;
use crate::sync::Arc;
use crate::{ConnectOpts, Connection, Database, OpenOptions, SqliteDialect, Value};

fn open(io: &Arc<dyn IO>, path: &str) -> Arc<Database> {
    Database::open(
        io.clone(),
        path,
        OpenOptions::new(Arc::new(SqliteDialect)).atoll(config()),
    )
    .unwrap()
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn integrity(conn: &Arc<Connection>) -> Vec<Vec<Value>> {
    rows(conn, "PRAGMA integrity_check")
}

fn ok() -> Vec<Vec<Value>> {
    vec![vec![Value::build_text("ok")]]
}

/// A database with tables, an index, values out of line, versions those values replaced, a
/// dropped table still on its list, a creation rolled back onto the abandoned list and rows
/// deleted.
fn used(path: &str) -> (Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, path);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE INDEX t_v ON t(substr(v, 1, 8))",
        "INSERT INTO t SELECT value, printf('%08d', value) || zeroblob(6000) FROM generate_series(1, 200)",
        "UPDATE t SET v = printf('%08d', id) || zeroblob(9000) WHERE id % 3 = 0",
        "DELETE FROM t WHERE id % 7 = 0",
        "CREATE TABLE gone(x)",
        "INSERT INTO gone SELECT zeroblob(5000) FROM generate_series(1, 20)",
        "DROP TABLE gone",
        "BEGIN",
        "CREATE TABLE undone(x)",
        "INSERT INTO undone VALUES (1)",
        "ROLLBACK",
    ] {
        conn.execute(sql).unwrap();
    }
    (db, conn)
}

fn atoll(db: &Database) -> &Arc<AtollDb> {
    db.atoll.as_ref().expect("Atoll mode")
}

/// A database in use accounts for every page: the system trees, every tree the catalog names,
/// the trees on both lists, every value's pages, and the pages values replaced, retired.
#[test]
fn a_database_in_use_accounts_for_every_page() {
    let (_db, conn) = used("/db/integrity-used");
    assert_eq!(integrity(&conn), ok());
    assert_eq!(rows(&conn, "PRAGMA quick_check"), ok());
}

/// A page allocated that nothing reaches is found as never used, through the pragma.
#[test]
fn a_leaked_page_is_found() {
    let (db, conn) = used("/db/integrity-leak");
    let store = atoll(&db).atoll().store().clone();
    let io = atoll(&db).io().clone();
    let class = crate::storage::atoll::page::SizeClass::new(0).unwrap();
    let pid = drive(&io, || store.allocate(class)).unwrap();
    let found = integrity(&conn);
    let text = format!("{found:?}");
    assert!(
        text.contains(&format!("page {} is allocated and never used", pid.raw())),
        "{text}"
    );
}

/// Table `name`'s catalog row.
fn entry_of(
    conn: &Arc<Connection>,
    adb: &AtollDb,
    name: &str,
) -> crate::storage::atoll::sql::catalog::Entry {
    use crate::storage::atoll::txn::versioned::Get;
    use crate::storage::atoll::txn::Isolation;
    let id = rows(
        conn,
        &format!("SELECT rootpage FROM sqlite_schema WHERE name = '{name}'"),
    )[0][0]
        .clone();
    let Value::Numeric(crate::numeric::Numeric::Integer(id)) = id else {
        panic!("{id:?}");
    };
    let txns = adb.atoll().transactions();
    let mut txn = drive(adb.io(), || txns.begin(Isolation::Snapshot)).unwrap();
    let mut get = Get::new(&super::super::catalog::key(id as u64));
    let entry = drive(adb.io(), || {
        adb.catalog().get(txns, adb.versions(), &mut txn, &mut get)
    })
    .unwrap()
    .expect("a catalog row");
    txns.abort(txn);
    entry
}

/// The node pages of the tree rooted at `root`, written back, each with its copy's slot.
fn copies(adb: &AtollDb, root: PageId) -> Vec<(PageId, u64)> {
    crate::storage::atoll::sql::mode::full_checkpoint(adb).unwrap();
    let tree = adb
        .open_plain(root)
        .with_head(crate::storage::atoll::txn::versioned::HEADER);
    let mut op = Check::default();
    let stats = drive(adb.io(), || tree.check(&mut op)).unwrap();
    let store = adb.atoll().store();
    stats
        .pages
        .into_iter()
        .filter_map(|pid| {
            let e = drive(adb.io(), || store.entry(pid)).unwrap();
            e.checked_sub(crate::storage::atoll::radix::SLOT_BASE)
                .map(|slot| (pid, slot))
        })
        .collect()
}

fn found(conn: &Arc<Connection>, wanted: &[String]) {
    let text = format!("{:?}", integrity(conn));
    for w in wanted {
        assert!(text.contains(w.as_str()), "{w:?} not in {text}");
    }
}

/// A copy in a slot free space marks free is found, through the pragma.
#[test]
fn a_copy_in_a_slot_free_space_marks_free_is_found() {
    let (db, conn) = used("/db/integrity-slot-free");
    let adb = atoll(&db);
    let (pid, slot) = copies(adb, entry_of(&conn, adb, "t").root)[0];
    let store = adb.atoll().store();
    drive(adb.io(), || store.plant_slot(pid.class(), slot, false)).unwrap();
    found(
        &conn,
        &[format!(
            "page {}'s copy is in slot {slot} of class {}",
            pid.raw(),
            pid.class().index()
        )],
    );
}

/// Two pages whose entries name one slot are found, and so is the slot the second one's entry
/// named before, in use and holding no page's copy.
#[test]
fn two_copies_in_one_slot_are_found() {
    let (db, conn) = used("/db/integrity-slot-shared");
    let adb = atoll(&db);
    let pages = copies(adb, entry_of(&conn, adb, "t").root);
    let ((p, slot), (q, old)) = (pages[0], pages[1]);
    let store = adb.atoll().store();
    let entry = slot + crate::storage::atoll::radix::SLOT_BASE;
    drive(adb.io(), || store.plant_entry(q, entry)).unwrap();
    let c = p.class().index();
    found(
        &conn,
        &[
            format!("slot {slot} of class {c} holds the copies of pages"),
            format!("slot {old} of class {c} is in use and holds no page's copy"),
        ],
    );
    let text = format!("{:?}", integrity(&conn));
    assert!(text.contains(&p.raw().to_string()) && text.contains(&q.raw().to_string()));
}

/// A database whose page table has two levels: a table of values out of line, each one 4 KiB
/// page, past the 509 numbers a page-table leaf holds.
fn big(path: &str) -> (Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, path);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    conn.execute("INSERT INTO big SELECT value, zeroblob(2000) FROM generate_series(1, 600)")
        .unwrap();
    assert!(atoll(&db).atoll().store().heights()[0][0] >= 2);
    (db, conn)
}

fn first_page() -> PageId {
    PageId::new(crate::storage::atoll::page::SizeClass::new(0).unwrap(), 0)
}

/// A count above the leaves of the page table that differs from its leaf is found.
#[test]
fn a_miscount_above_the_leaves_is_found() {
    let (db, conn) = big("/db/integrity-miscount");
    let adb = atoll(&db);
    let store = adb.atoll().store();
    drive(adb.io(), || store.plant_miscount(first_page(), 1)).unwrap();
    found(
        &conn,
        &["counts 1 in use below node 0 of level 0".to_string()],
    );
}

/// The pages under a count of zero, which the census does not read, are still found allocated
/// where a tree reaches them, by their own entries.
#[test]
fn pages_under_a_count_of_zero_are_found_by_their_entries() {
    let (db, conn) = big("/db/integrity-zero-count");
    let adb = atoll(&db);
    let store = adb.atoll().store();
    drive(adb.io(), || store.plant_miscount(first_page(), 0)).unwrap();
    found(
        &conn,
        &["and allocated, under a count that says none is".to_string()],
    );
}

/// A page a tree reaches that is marked retired is found.
#[test]
fn a_reached_page_marked_retired_is_found() {
    let (db, conn) = used("/db/integrity-retired");
    let adb = atoll(&db);
    let (pid, _) = copies(adb, entry_of(&conn, adb, "t").root)[0];
    let store = adb.atoll().store();
    store.retire(pid);
    found(
        &conn,
        &[format!("page {} is reached by a node of tree", pid.raw())],
    );
    let text = format!("{:?}", integrity(&conn));
    assert!(text.contains("and retired"), "{text}");
}

/// A list naming a page that is no tree's root is found, by the tree's own check.
#[test]
fn a_list_naming_no_tree_is_found() {
    let (db, conn) = used("/db/integrity-no-tree");
    let adb = atoll(&db);
    let pages = copies(adb, entry_of(&conn, adb, "t").root);
    let (leaf, _) = *pages.last().unwrap();
    let list = adb.open_plain(super::super::catalog::System::Abandoned.root());
    let mut put = crate::storage::atoll::btree::Put::new(
        &leaf.raw().to_be_bytes(),
        Vec::new(),
        crate::storage::atoll::btree::Mode::Insert,
    );
    drive(adb.io(), || list.put(&mut put)).unwrap();
    found(&conn, &[format!("tree {}: ", leaf.raw())]);
}

/// A tree the catalog names that a list also holds, to be freed, is found.
#[test]
fn a_tree_the_catalog_names_and_a_list_holds_is_found() {
    let (db, conn) = used("/db/integrity-listed");
    let adb = atoll(&db);
    let root = entry_of(&conn, adb, "t").root;
    let list = adb.open_plain(super::super::catalog::System::Abandoned.root());
    let mut put = crate::storage::atoll::btree::Put::new(
        &root.raw().to_be_bytes(),
        Vec::new(),
        crate::storage::atoll::btree::Mode::Insert,
    );
    drive(adb.io(), || list.put(&mut put)).unwrap();
    found(
        &conn,
        &[format!(
            "tree {} is named by the catalog and on the Abandoned list",
            root.raw()
        )],
    );
}

/// A page a tree reaches, freed behind its back once written back, is found as reached and free.
#[test]
fn a_freed_page_a_tree_reaches_is_found() {
    let (db, conn) = used("/db/integrity-freed");
    let adb = atoll(&db);
    let (store, io) = (adb.atoll().store().clone(), adb.io().clone());
    let root = rows(&conn, "SELECT rootpage FROM sqlite_schema WHERE name = 't'")[0][0].clone();
    let Value::Numeric(crate::numeric::Numeric::Integer(id)) = root else {
        panic!("{root:?}");
    };
    let entry = {
        use crate::storage::atoll::txn::versioned::Get;
        use crate::storage::atoll::txn::Isolation;
        let txns = adb.atoll().transactions();
        let mut txn = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
        let mut get = Get::new(&super::super::catalog::key(id as u64));
        let entry = drive(&io, || {
            adb.catalog().get(txns, adb.versions(), &mut txn, &mut get)
        })
        .unwrap()
        .expect("t's catalog row");
        txns.abort(txn);
        entry
    };
    let tree = adb
        .open_plain(entry.root)
        .with_head(crate::storage::atoll::txn::versioned::HEADER);
    let mut op = Check::default();
    let stats = drive(&io, || tree.check(&mut op)).unwrap();
    let leaf = *stats
        .pages
        .iter()
        .find(|&&p| p != entry.root)
        .expect("t has more than its root");
    // Written back first: a dirty frame of a page freed behind its tree is refused at writeback.
    crate::storage::atoll::sql::mode::full_checkpoint(adb).unwrap();
    drive(&io, || store.free(leaf)).unwrap();
    let text = format!("{:?}", integrity(&conn));
    assert!(
        text.contains(&format!("page {} is reached by a node of tree", leaf.raw()))
            && text.contains("and free"),
        "{text}"
    );
}

/// A table dropped while a reader's snapshot still sees it stays on the dropped list, its tree
/// whole, and the check reaches it there: its pages are accounted for, not never used.
#[test]
fn a_dropped_tree_a_snapshot_still_sees_is_reached_on_its_list() {
    let (db, conn) = used("/db/integrity-held-drop");
    conn.execute("CREATE TABLE held(x)").unwrap();
    conn.execute("INSERT INTO held SELECT zeroblob(5000) FROM generate_series(1, 30)")
        .unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    reader.execute("BEGIN").unwrap();
    assert_eq!(
        rows(&reader, "SELECT count(*) FROM held"),
        vec![vec![Value::from_i64(30)]]
    );
    conn.execute("DROP TABLE held").unwrap();
    let adb = atoll(&db);
    let list = adb.open_plain(super::super::catalog::System::Dropped.root());
    let mut cursor = crate::storage::atoll::btree::Cursor::new();
    assert!(
        drive(adb.io(), || list.seek(
            &mut cursor,
            &[],
            crate::storage::atoll::btree::Seek::Ge
        ))
        .unwrap(),
        "the dropped tree waits on its list for the reader"
    );
    assert_eq!(integrity(&conn), ok());
    reader.execute("COMMIT").unwrap();
}

/// The check takes the write gate exclusive, so it waits for a writer held open: with no busy
/// timeout, Busy at once, and once the writer commits it runs.
#[test]
fn an_integrity_check_waits_for_an_open_writer() {
    let (db, conn) = used("/db/integrity-waits");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    writer.execute("BEGIN").unwrap();
    writer.execute("INSERT INTO t VALUES (1000, 'w')").unwrap();
    let mut check = conn.prepare("PRAGMA integrity_check").unwrap();
    let busy = loop {
        match check.step() {
            Ok(crate::StepResult::IO) => check._io().step().unwrap(),
            other => break other,
        }
    };
    assert!(
        matches!(
            busy,
            Ok(crate::StepResult::Busy) | Err(crate::LimboError::Busy)
        ),
        "{busy:?}"
    );
    drop(check);
    writer.execute("COMMIT").unwrap();
    assert_eq!(integrity(&conn), ok());
}

// ---------------------------------------------------------------- in frames (ASYNC1 D2e (iii-d))

/// [`used`], opened with its reads queued, as a frame's are, with a worker stepping the
/// checkpoint's task, as a walk in a frame needs; and with enough of each thing an integrity walk
/// reads that each of its steps reads a page of its own: catalog rows on several leaves, both
/// lists' entries on several leaves, and a value split into chunks, whose directory page is read.
fn used_with_worker(path: &str) -> (Arc<dyn IO>, Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut cfg = config();
    cfg.checkpointing = crate::storage::atoll::checkpoint::Checkpointing::Thread;
    cfg.buffer.budget = 64 << 20;
    let db = crate::storage::atoll::requests::opened_async(|| {
        Database::open(
            io.clone(),
            path,
            OpenOptions::new(Arc::new(SqliteDialect)).atoll(cfg),
        )
        .unwrap()
    });
    let conn = db.connect(ConnectOpts::new()).unwrap();
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE INDEX t_v ON t(substr(v, 1, 8))",
        "INSERT INTO t SELECT value, printf('%08d', value) || zeroblob(6000) FROM generate_series(1, 200)",
        "UPDATE t SET v = printf('%08d', id) || zeroblob(9000) WHERE id % 3 = 0",
        "DELETE FROM t WHERE id % 7 = 0",
        "CREATE TABLE chunked(v BLOB)",
    ] {
        conn.execute(sql).unwrap();
    }
    let chunked = crate::storage::atoll::btree::value::chunk() + 7;
    conn.execute(format!(
        "INSERT INTO chunked VALUES (randomblob({chunked}))"
    ))
    .unwrap();
    for i in 0..300 {
        conn.execute(format!("CREATE TABLE kept_{i}(x)")).unwrap();
    }
    for i in 0..300 {
        conn.execute(format!("CREATE TABLE gone_{i}(x)")).unwrap();
        conn.execute(format!("INSERT INTO gone_{i} VALUES (zeroblob(100))"))
            .unwrap();
    }
    for i in 0..300 {
        conn.execute(format!("DROP TABLE gone_{i}")).unwrap();
    }
    for i in 0..300 {
        conn.execute("BEGIN").unwrap();
        conn.execute(format!("CREATE TABLE undone_{i}(x)")).unwrap();
        conn.execute("ROLLBACK").unwrap();
    }
    (io, db, conn)
}

/// What an integrity check stepped in frames came to, and where its walk stood, with its owner's
/// tenure, at each of its returns for IO.
struct Walked {
    out: std::result::Result<Vec<Vec<Value>>, String>,
    at_io: Vec<(&'static str, usize)>,
}

/// Steps `stmt` in test frames until it is done or fails, at most `bound` calls. Every page nothing
/// keeps is evicted first, and again as the walk returns IO at a step it had not just returned at,
/// and the IO thread is held through each call, so each step of the walk reads cold and returns IO;
/// each return's wait is waited for outside the frame.
fn walked_in_frames(
    io: &Arc<dyn IO>,
    db: &Database,
    stmt: &mut crate::Statement,
    bound: usize,
) -> Walked {
    use crate::storage::atoll::sql::mode::WALKED_AT_IO;
    WALKED_AT_IO.with(|w| w.borrow_mut().clear());
    let taken = || WALKED_AT_IO.with(|w| std::mem::take(&mut *w.borrow_mut()));
    chill(db);
    let mut rows = Vec::new();
    for calls in 1..=bound {
        let walked = WALKED_AT_IO.with(|w| w.borrow().len());
        let held = held_io(db);
        let stepped = {
            let _frame = crate::storage::atoll::cooperative::Frame::enter();
            stmt.step()
        };
        drop(held);
        match stepped {
            Err(e) => {
                return Walked {
                    out: Err(e.to_string()),
                    at_io: taken(),
                }
            }
            Ok(crate::StepResult::Row) => {
                rows.push(stmt.row().unwrap().get_values().cloned().collect())
            }
            Ok(crate::StepResult::Done) => {
                return Walked {
                    out: Ok(rows),
                    at_io: taken(),
                }
            }
            Ok(crate::StepResult::Yield) => {}
            Ok(crate::StepResult::IO) => {
                let c = stmt.pending_io_for_test().expect("the statement's wait");
                if !waited(io, &c) || !retired(db) {
                    return Walked {
                        out: Err(format!("call {calls}: its wait never finished")),
                        at_io: taken(),
                    };
                }
                // The walk returned this IO at a step it had not returned at just before: what it
                // does not keep is evicted again, so that step reads cold. Within one step it is
                // not, so a descent of several levels goes on.
                let new_step = WALKED_AT_IO.with(|w| {
                    let w = w.borrow();
                    w.len() > walked && (w.len() < 2 || w[w.len() - 1].0 != w[w.len() - 2].0)
                });
                if new_step {
                    chill(db);
                }
            }
            Ok(other) => {
                return Walked {
                    out: Err(format!("call {calls}: {other:?}")),
                    at_io: taken(),
                }
            }
        }
    }
    Walked {
        out: Err(format!("not done in {bound} calls")),
        at_io: taken(),
    }
}

/// The database's IO thread held before its next idle, until the sender returned drops.
fn held_io(db: &Database) -> std::sync::mpsc::Sender<()> {
    crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
        atoll(db).atoll().requests_for_test(),
    )
    .expect("the IO thread held before its idle")
}

/// Waits until every request on the database's files has retired: whether they did.
fn retired(db: &Database) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while atoll(db).atoll().outstanding_requests() != 0 {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

/// Waits outside any frame for `c`, which the IO thread or the checkpoint's worker finishes:
/// whether it finished.
fn waited(io: &Arc<dyn IO>, c: &crate::Completion) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !c.finished() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        let _ = io.step();
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

/// Evicts every page nothing holds or keeps, the pool's and the metadata cache's, the last
/// metadata read's kept request first (`meta::LAST_READ`).
fn chill(db: &Database) {
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    let adb = atoll(db).atoll();
    adb.pool().evict_all_clean();
    adb.store().meta().evict_all();
}

/// Plants `n` pages allocated that nothing reaches.
fn leaked(db: &Database, n: usize) {
    let store = atoll(db).atoll().store().clone();
    let io = atoll(db).io().clone();
    let class = crate::storage::atoll::page::SizeClass::new(0).unwrap();
    for _ in 0..n {
        drive(&io, || store.allocate(class)).unwrap();
    }
}

/// v2 §5 proof 1: a cold integrity check in frames returns IO at each step of its walk and goes
/// on where it stopped, under one owner; it finds what the synchronous check finds, on a clean
/// database and on planted leaks, doubles nothing, and cuts where the synchronous check cuts.
#[test]
fn a_cold_integrity_check_in_frames_returns_io_at_each_step_and_finds_what_the_synchronous_one_does(
) {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_cold_integrity_check_in_frames_returns_io_at_each_step_and_finds_what_the_synchronous_one_does"
        ),
        || {
            let (io, db, conn) = used_with_worker("/db/integrity-frames");
            let walk = |sql: &str| {
                let mut stmt = conn.prepare(sql).unwrap();
                let walked = walked_in_frames(&io, &db, &mut stmt, 100_000);
                drop(stmt);
                walked
            };
            let clean_sync = integrity(&conn);
            let clean = walk("PRAGMA integrity_check");
            leaked(&db, 3);
            let found_sync = integrity(&conn);
            let found = walk("PRAGMA integrity_check");
            let cut_sync = [1, 2].map(|max| rows(&conn, &format!("PRAGMA integrity_check({max})")));
            let cut = [1, 2].map(|max| walk(&format!("PRAGMA integrity_check({max})")).out);
            // Pages under a count of zero are found by their own entries, which the walk reads.
            let tall = atoll(&db).atoll().store().heights()[0][0] >= 2;
            let store = atoll(&db).atoll().store().clone();
            drive(atoll(&db).io(), || store.plant_miscount(first_page(), 0)).unwrap();
            let zero_sync = integrity(&conn);
            let zero = walk("PRAGMA integrity_check");
            let places: std::collections::BTreeSet<_> = [&clean, &found, &zero]
                .iter()
                .flat_map(|w| w.at_io.iter().map(|(at, _)| *at))
                .collect();
            let owners = [&clean, &found, &zero].map(|w| {
                w.at_io
                    .iter()
                    .map(|(_, owner)| *owner)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
            });
            assert_eq!(clean_sync, ok(), "the clean database's synchronous check");
            assert_eq!(clean.out, Ok(clean_sync), "the clean walk in frames");
            assert!(tall, "the page table has one level: no count above its leaves");
            // A catalog entry is 21 bytes, always in line: its load reads no page.
            for at in [
                "catalog.seek",
                "catalog.next",
                "lists.seek",
                "lists.next",
                "trees.check",
                "trees.values",
                "account.map",
                "account.space",
                "account.entries",
            ] {
                assert!(
                    places.contains(at),
                    "the walk never returned IO at {at}: {places:?}"
                );
            }
            assert_eq!(owners, [1; 3], "a walk's owner changed across its returns");
            assert_ne!(found_sync, ok(), "the leaks were not found");
            assert_eq!(found.out, Ok(found_sync.clone()), "the walk in frames found otherwise");
            let lines = found_sync.iter().collect::<std::collections::BTreeSet<_>>();
            assert_eq!(lines.len(), found_sync.len(), "a finding doubled: {found_sync:?}");
            assert_eq!(cut, cut_sync.map(Ok), "a capped walk cut elsewhere");
            assert_ne!(zero_sync, found_sync, "the zero count was not found");
            assert_eq!(zero.out, Ok(zero_sync), "the walk under a zero count found otherwise");
        },
    );
}

/// A small database opened as [`used_with_worker`] opens its own.
fn small_with_worker(path: &str) -> (Arc<dyn IO>, Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut cfg = config();
    cfg.checkpointing = crate::storage::atoll::checkpoint::Checkpointing::Thread;
    let db = crate::storage::atoll::requests::opened_async(|| {
        Database::open(
            io.clone(),
            path,
            OpenOptions::new(Arc::new(SqliteDialect)).atoll(cfg),
        )
        .unwrap()
    });
    let conn = db.connect(ConnectOpts::new()).unwrap();
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE INDEX t_v ON t(substr(v, 1, 8))",
        "INSERT INTO t SELECT value, printf('%08d', value) || zeroblob(6000) FROM generate_series(1, 50)",
    ] {
        conn.execute(sql).unwrap();
    }
    (io, db, conn)
}

/// What one call of a statement in a test frame came to.
enum Called {
    /// It returned IO, with where its walk stood if the walk returned it.
    Io(crate::Completion, Option<&'static str>),
    Row(Vec<Value>),
    Done,
    Failed(String),
}

/// One call of `stmt` in a test frame, the IO thread held through it.
fn called(db: &Database, stmt: &mut crate::Statement) -> Called {
    use crate::storage::atoll::sql::mode::WALKED_AT_IO;
    let walked = WALKED_AT_IO.with(|w| w.borrow().len());
    let held = held_io(db);
    let stepped = {
        let _frame = crate::storage::atoll::cooperative::Frame::enter();
        stmt.step()
    };
    drop(held);
    match stepped {
        Ok(crate::StepResult::IO) => {
            let place = WALKED_AT_IO.with(|w| {
                let w = w.borrow();
                (w.len() > walked).then(|| w[w.len() - 1].0)
            });
            Called::Io(
                stmt.pending_io_for_test().expect("the statement's wait"),
                place,
            )
        }
        Ok(crate::StepResult::Row) => {
            Called::Row(stmt.row().unwrap().get_values().cloned().collect())
        }
        Ok(crate::StepResult::Done) => Called::Done,
        Ok(other) => Called::Failed(format!("{other:?}")),
        Err(e) => Called::Failed(e.to_string()),
    }
}

/// Calls `stmt` until its walk returns IO at `place`, waiting for every other return's IO: that
/// IO's wait, unwaited; else why not, within `bound` calls.
fn called_to(
    io: &Arc<dyn IO>,
    db: &Database,
    stmt: &mut crate::Statement,
    place: &str,
    bound: usize,
) -> std::result::Result<crate::Completion, String> {
    for calls in 1..=bound {
        match called(db, stmt) {
            Called::Io(c, Some(at)) if at == place => return Ok(c),
            Called::Io(c, _) => {
                if !waited(io, &c) || !retired(db) {
                    return Err(format!("call {calls}: its wait never finished"));
                }
            }
            Called::Row(row) => return Err(format!("call {calls}: a row {row:?}")),
            Called::Done => return Err(format!("call {calls}: done")),
            Called::Failed(e) => return Err(format!("call {calls}: {e}")),
        }
    }
    Err(format!("not at {place} in {bound} calls"))
}

/// Calls `stmt` to its end, waiting for each return's IO: its rows, or why not.
fn called_out(
    io: &Arc<dyn IO>,
    db: &Database,
    stmt: &mut crate::Statement,
    bound: usize,
) -> std::result::Result<Vec<Vec<Value>>, String> {
    let mut rows = Vec::new();
    for calls in 1..=bound {
        match called(db, stmt) {
            Called::Io(c, _) => {
                if !waited(io, &c) || !retired(db) {
                    return Err(format!("call {calls}: its wait never finished"));
                }
            }
            Called::Row(row) => rows.push(row),
            Called::Done => return Ok(rows),
            Called::Failed(e) => return Err(format!("call {calls}: {e}")),
        }
    }
    Err(format!("not done in {bound} calls"))
}

/// A thread holding the checkpointer's state while told to: `true` holds it, waiting while a step
/// holds it, `false` lets it go; each order is answered on its own channel once done.
struct Holder {
    tell: std::sync::mpsc::Sender<(bool, std::sync::mpsc::Sender<()>)>,
    thread: std::thread::JoinHandle<()>,
}

impl Holder {
    fn new(db: &Database) -> Self {
        let cp = atoll(db).atoll().checkpointer_shared().clone();
        let (tell, orders) = std::sync::mpsc::channel::<(bool, std::sync::mpsc::Sender<()>)>();
        let thread = std::thread::spawn(move || {
            let cp = cp;
            let mut held = None;
            for (hold, answer) in orders {
                held = None;
                if hold {
                    held = Some(cp.hold_waiting());
                }
                let _ = answer.send(());
            }
            drop(held);
        });
        Self { tell, thread }
    }

    /// Holds the state, or lets it go: whether the thread answered.
    fn hold(&self, hold: bool) -> bool {
        order(&self.tell, hold)
    }

    /// Lets the state go and ends the thread.
    fn end(self) {
        drop(self.tell);
        let _ = self.thread.join();
    }
}

/// Gives a [`Holder`] its order and waits for its answer: whether it answered.
fn order(tell: &std::sync::mpsc::Sender<(bool, std::sync::mpsc::Sender<()>)>, hold: bool) -> bool {
    let (answer, answered) = std::sync::mpsc::channel();
    tell.send((hold, answer)).is_ok()
        && answered
            .recv_timeout(std::time::Duration::from_secs(20))
            .is_ok()
}

/// ASYNC1 D2e (iii-d) E4 proof 8: a walk in a frame asking for its lease while the checkpointer's
/// state is held returns its one wait, unfinished and listed as the state's contender; the let-go
/// finishes it, and the next call holds the state and takes the lease. A let-go between the walk's
/// try and its registration is seen at its retry, which takes the lease with no wait. Both walks
/// then find what the synchronous check finds (mutant `thread-contender`).
#[test]
fn a_walk_meeting_the_checkpointers_state_held_waits_on_its_one_registered_wait() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_walk_meeting_the_checkpointers_state_held_waits_on_its_one_registered_wait"
        ),
        || {
            let (io, db, conn) = small_with_worker("/db/integrity-lease-wait");
            let cp = atoll(&db).atoll().checkpointer_shared().clone();
            let sync = integrity(&conn);
            let holder = Holder::new(&db);
            // Held across the ask.
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            let checkpointed =
                called_to(&io, &db, &mut stmt, "checkpoint", 1_000).map(|c| waited(&io, &c));
            let held = holder.hold(true);
            let asked = called(&db, &mut stmt);
            let (wait, at) = match &asked {
                Called::Io(c, at) => (Some(c.clone()), *at),
                _ => (None, None),
            };
            let unfinished = wait.as_ref().is_some_and(|c| !c.finished());
            let listed = wait.as_ref().is_some_and(|c| cp.contends_wait_for_test(c));
            // At once: a wait not registered is never finished by the let-go, so the steps after it
            // would only wait out their bounds before saying so.
            assert!(
                listed,
                "the walk's wait was not registered as the state's contender"
            );
            let leased_while_held = cp.units_for_test().2;
            let let_go = holder.hold(false);
            let finished = wait.as_ref().is_some_and(|c| c.finished());
            // Cold, so the call returns IO in its page walk, the lease still held.
            chill(&db);
            let next = called(&db, &mut stmt);
            let leased_after = cp.units_for_test().2;
            let first = match next {
                Called::Io(c, _) => {
                    if waited(&io, &c) {
                        called_out(&io, &db, &mut stmt, 100_000)
                    } else {
                        Err("its wait never finished".to_string())
                    }
                }
                Called::Row(row) => called_out(&io, &db, &mut stmt, 100_000).map(|mut rows| {
                    rows.insert(0, row);
                    rows
                }),
                Called::Done => Ok(Vec::new()),
                Called::Failed(e) => Err(e),
            };
            drop(stmt);
            // Let go between the try and the registration.
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            let checkpointed_again =
                called_to(&io, &db, &mut stmt, "checkpoint", 1_000).map(|c| waited(&io, &c));
            let held_again = holder.hold(true);
            let released = std::rc::Rc::new(std::cell::Cell::new(false));
            let (tell, seen) = (holder.tell.clone(), released.clone());
            crate::storage::atoll::guarded::before_contending_for_test(move || {
                seen.set(order(&tell, false));
            });
            let retried = called(&db, &mut stmt);
            let released = released.get();
            let no_wait_at_lease = !matches!(retried, Called::Io(_, Some("lease")));
            let second = match retried {
                Called::Io(c, _) => {
                    if waited(&io, &c) {
                        called_out(&io, &db, &mut stmt, 100_000)
                    } else {
                        Err("its wait never finished".to_string())
                    }
                }
                Called::Row(row) => called_out(&io, &db, &mut stmt, 100_000).map(|mut rows| {
                    rows.insert(0, row);
                    rows
                }),
                Called::Done => Ok(Vec::new()),
                Called::Failed(e) => Err(e),
            };
            drop(stmt);
            holder.end();
            assert_eq!(checkpointed, Ok(true), "the first walk's full checkpoint");
            assert!(held, "the state was not held");
            assert_eq!(
                at,
                Some("lease"),
                "the first walk did not wait at its lease"
            );
            assert!(unfinished, "the walk's wait was finished at its return");
            assert!(
                !leased_while_held,
                "a lease was taken while the state was held"
            );
            assert!(
                let_go && finished,
                "the let-go did not finish the walk's wait"
            );
            assert!(leased_after, "the call after the let-go took no lease");
            assert_eq!(first, Ok(sync.clone()), "the first walk");
            assert_eq!(
                checkpointed_again,
                Ok(true),
                "the second walk's full checkpoint"
            );
            assert!(held_again, "the state was not held for the second walk");
            assert!(released, "the second walk never reached its registration");
            assert!(
                no_wait_at_lease,
                "a let-go before the registration was not seen at the retry"
            );
            assert_eq!(second, Ok(sync), "the second walk");
        },
    );
}

/// ASYNC1 D2e (iii-d) E4 proof 9: with the checkpointer's state held, five integrity statements in
/// turn each register a wait of their own and are reset while it is registered; each reset takes it
/// back, so only the task may still be listed. Once the state is let go a sixth walk takes the
/// lease and finishes (mutant `no-withdraw`).
#[test]
fn a_reset_walk_takes_its_registered_wait_back() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reset_walk_takes_its_registered_wait_back"
        ),
        || {
            let (io, db, conn) = small_with_worker("/db/integrity-withdraw");
            let cp = atoll(&db).atoll().checkpointer_shared().clone();
            let sync = integrity(&conn);
            let holder = Holder::new(&db);
            let mut resets = Vec::new();
            for _ in 0..5 {
                let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
                let checkpointed =
                    called_to(&io, &db, &mut stmt, "checkpoint", 1_000).map(|c| waited(&io, &c));
                let held = holder.hold(true);
                let asked = called(&db, &mut stmt);
                let wait = match &asked {
                    Called::Io(c, Some("lease")) => Some(c.clone()),
                    _ => None,
                };
                let listed = wait.as_ref().is_some_and(|c| cp.contends_wait_for_test(c));
                stmt.reset().unwrap();
                let withdrawn = wait.as_ref().is_some_and(|c| !cp.contends_wait_for_test(c));
                let others = cp.contenders_for_test();
                drop(stmt);
                let let_go = holder.hold(false);
                resets.push((
                    checkpointed,
                    held,
                    wait.is_some(),
                    listed,
                    withdrawn,
                    others,
                    let_go,
                ));
            }
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            let sixth = called_out(&io, &db, &mut stmt, 100_000);
            drop(stmt);
            holder.end();
            for (i, (checkpointed, held, waited, listed, withdrawn, others, let_go)) in
                resets.into_iter().enumerate()
            {
                assert_eq!(checkpointed, Ok(true), "walk {i}: its full checkpoint");
                assert!(
                    held && let_go,
                    "walk {i}: the state was not held and let go"
                );
                assert!(
                    waited && listed,
                    "walk {i}: no wait registered at its lease"
                );
                assert!(
                    withdrawn,
                    "a reset integrity check stayed a contender (walk {i})"
                );
                assert!(others <= 1, "walk {i}: {others} contenders after its reset");
            }
            assert_eq!(sixth, Ok(sync), "the sixth walk");
        },
    );
}

/// What a call came to, in words.
fn said(called: &Called) -> String {
    match called {
        Called::Io(c, at) => format!("IO at {at:?}, finished {}", c.finished()),
        Called::Row(row) => format!("a row {row:?}"),
        Called::Done => "done".to_string(),
        Called::Failed(e) => format!("failed: {e}"),
    }
}

/// Calls `stmt`, cold, until its walk returns IO at a step of its page walk, so it holds its
/// lease: that IO's wait, unwaited.
fn called_to_pages(
    io: &Arc<dyn IO>,
    db: &Database,
    stmt: &mut crate::Statement,
) -> std::result::Result<crate::Completion, String> {
    chill(db);
    for calls in 1..=1_000 {
        match called(db, stmt) {
            Called::Io(c, Some(at)) if at.contains('.') => return Ok(c),
            Called::Io(c, _) => {
                if !waited(io, &c) || !retired(db) {
                    return Err(format!("call {calls}: its wait never finished"));
                }
            }
            other => return Err(format!("call {calls}: {}", said(&other))),
        }
    }
    Err("not in its page walk in 1000 calls".to_string())
}

/// Waits for `c`, then calls `stmt` to its end: its rows, or why not.
fn finished_after(
    io: &Arc<dyn IO>,
    db: &Database,
    stmt: &mut crate::Statement,
    c: std::result::Result<crate::Completion, String>,
) -> std::result::Result<Vec<Vec<Value>>, String> {
    let c = c?;
    if !waited(io, &c) {
        return Err("its wait never finished".to_string());
    }
    called_out(io, db, stmt, 100_000)
}

/// The words of a statement's refusal as it starts beside a cooperative check of its connection,
/// from its page walk to its first row.
const START_REFUSED: &str = "cannot start a statement while an integrity check in a cooperative \
                             frame is under way on the connection, before its first row";

/// A walk's first entry, called in a frame as the integrity opcode calls it, with no statement of
/// its own: what it came to, the walk it kept let go in a frame.
fn entered_in_frame(conn: &Arc<Connection>) -> Called {
    let mut walk = None;
    let out = {
        let _frame = crate::storage::atoll::cooperative::Frame::enter();
        conn.atoll().unwrap().check_pages_in(&mut walk, 100)
    };
    {
        let _frame = crate::storage::atoll::cooperative::Frame::enter();
        drop(walk);
    }
    match out {
        Ok(crate::types::IOResult::IO(_)) => Called::Failed("IO".to_string()),
        Ok(crate::types::IOResult::Done((found, _))) => Called::Failed(format!("done: {found:?}")),
        Err(e) => Called::Failed(e.to_string()),
    }
}

/// The words of a cooperative walk's refusal beside another walk of its connection.
const SIBLING_REFUSED: &str =
    "an integrity check while another runs on this connection, in a cooperative frame";

/// ASYNC1 D2e (iii-d) F2 proofs 1 and 2: beside a walk of the same connection, holding its lease or
/// registered as the state's contender, a second walk in a frame is refused at its first entry,
/// changing neither the lease, the contenders nor the full checkpoints asked for; the first walk
/// then finishes with what the synchronous check finds (mutant `sibling-admitted`).
#[test]
fn a_sibling_walk_is_refused_at_its_first_entry() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_sibling_walk_is_refused_at_its_first_entry"
        ),
        || {
            let (io, db, conn) = small_with_worker("/db/integrity-sibling");
            let cp = atoll(&db).atoll().checkpointer_shared().clone();
            let sync = integrity(&conn);
            // 1: the first walk holds its lease.
            let mut one = conn.prepare("PRAGMA integrity_check").unwrap();
            let past = called_to_pages(&io, &db, &mut one);
            let leased = cp.lease_and_fulls_for_test();
            let listed = cp.contenders_for_test();
            // As a statement: refused as it starts, beside the walk.
            let mut two = conn.prepare("PRAGMA integrity_check").unwrap();
            let refused_start = called(&db, &mut two);
            drop(two);
            // Its entry, called as its opcode calls it: refused by the admission word.
            let refused = entered_in_frame(&conn);
            // At once: a sibling admitted beside the walk leaves the connection's admission word to
            // two walks, so ending either trips the admission's release assertion first.
            assert!(
                matches!(&refused, Called::Failed(e) if e.contains(SIBLING_REFUSED)),
                "a sibling integrity check was admitted: {}",
                said(&refused)
            );
            let after = (cp.lease_and_fulls_for_test(), cp.contenders_for_test());
            let first = finished_after(&io, &db, &mut one, past);
            drop(one);
            let walking_after_first = conn.atoll().unwrap().walking_for_test();
            // 2: the first walk registered as the state's contender.
            let holder = Holder::new(&db);
            let mut one = conn.prepare("PRAGMA integrity_check").unwrap();
            let checkpointed =
                called_to(&io, &db, &mut one, "checkpoint", 1_000).map(|c| waited(&io, &c));
            let held = holder.hold(true);
            let asked = called(&db, &mut one);
            let wait = match &asked {
                Called::Io(c, Some("lease")) => Some(c.clone()),
                _ => None,
            };
            let fulls = cp.lease_and_fulls_for_test();
            let mut two = conn.prepare("PRAGMA integrity_check").unwrap();
            let refused_waiting_start = called(&db, &mut two);
            drop(two);
            let refused_waiting = entered_in_frame(&conn);
            assert!(
                matches!(&refused_waiting, Called::Failed(e) if e.contains(SIBLING_REFUSED)),
                "a sibling integrity check was admitted beside a waiting walk: {}",
                said(&refused_waiting)
            );
            let listed_waiting = (
                wait.as_ref().is_some_and(|c| cp.contends_wait_for_test(c)),
                cp.contenders_for_test(),
                cp.lease_and_fulls_for_test(),
            );
            let let_go = holder.hold(false);
            let second = finished_after(&io, &db, &mut one, wait.ok_or_else(|| said(&asked)));
            drop(one);
            holder.end();
            let walking = conn.atoll().unwrap().walking_for_test();
            assert_ne!(leased.0, 0, "the first walk held no lease in its page walk");
            for (case, refused) in [
                ("holding", &refused_start),
                ("waiting", &refused_waiting_start),
            ] {
                assert!(
                    matches!(refused, Called::Failed(e) if e.contains(START_REFUSED)),
                    "a statement started beside a walk {case} its lease: {}",
                    said(refused)
                );
            }
            assert_eq!(
                after,
                (leased, listed),
                "the refused walk changed the lease, the contenders or the full checkpoints asked for"
            );
            assert_eq!(first, Ok(sync.clone()), "the first walk");
            assert_eq!(
                walking_after_first, 0,
                "the first walk's admission outlived it"
            );
            assert_eq!(checkpointed, Ok(true), "the waiting walk's full checkpoint");
            assert!(held && let_go, "the state was not held and let go");
            assert!(
                listed_waiting.0 && listed_waiting.1 <= 2,
                "the contenders beside the refused walk: {listed_waiting:?}"
            );
            assert_eq!(
                listed_waiting.2, fulls,
                "the refused walk took a lease or asked for a full checkpoint"
            );
            assert_eq!(second, Ok(sync), "the waiting walk");
            assert_eq!(walking, 0, "a walk's admission outlived it");
        },
    );
}

/// ASYNC1 D2e (iii-d) F2 proof 3: once a walk has ended, done or reset at its full checkpoint, its
/// lease or its page walk, a new walk of the same connection is admitted and finishes, and no
/// admission is left after each (mutant `claim-kept`).
#[test]
fn a_walk_after_an_ended_one_is_admitted() {
    crate::storage::atoll::testing::cleanly(
        concat!(module_path!(), "::a_walk_after_an_ended_one_is_admitted"),
        || {
            let (io, db, conn) = small_with_worker("/db/integrity-after");
            let sync = integrity(&conn);
            let holder = Holder::new(&db);
            let mut ends = Vec::new();
            for end in ["done", "checkpoint", "lease", "pages"] {
                let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
                let stopped = match end {
                    "done" => called_out(&io, &db, &mut stmt, 100_000).map(|_| ()),
                    "checkpoint" => called_to(&io, &db, &mut stmt, "checkpoint", 1_000).map(|_| ()),
                    "lease" => called_to(&io, &db, &mut stmt, "checkpoint", 1_000).and_then(|c| {
                        let held = waited(&io, &c) && holder.hold(true);
                        let asked = called(&db, &mut stmt);
                        let let_go = holder.hold(false);
                        match asked {
                            Called::Io(_, Some("lease")) if held && let_go => Ok(()),
                            other => Err(said(&other)),
                        }
                    }),
                    _ => called_to_pages(&io, &db, &mut stmt).map(|_| ()),
                };
                stmt.reset().unwrap();
                drop(stmt);
                let walking = conn.atoll().unwrap().walking_for_test();
                let mut next = conn.prepare("PRAGMA integrity_check").unwrap();
                let after = called_out(&io, &db, &mut next, 100_000);
                drop(next);
                ends.push((end, stopped, walking, after));
            }
            holder.end();
            for (end, stopped, walking, after) in ends {
                assert_eq!(stopped, Ok(()), "{end}: the walk did not stop there");
                assert!(
                    !after.as_ref().is_err_and(|e| e.contains(SIBLING_REFUSED)),
                    "a walk after an ended one was refused ({end})"
                );
                assert_eq!(walking, 0, "{end}: the ended walk's admission was left");
                assert_eq!(after, Ok(sync.clone()), "{end}: the walk after it");
            }
        },
    );
}

/// The words of a host check's refusal beside a cooperative walk of its connection.
const HOST_REFUSED: &str = "an integrity check while a cooperative one runs on this connection";

/// A host integrity check of `conn` on a thread of its own: its result, sent once it is done, and
/// whether it reached its full checkpoint, set by a hook there.
fn host_check(conn: &Arc<Connection>) -> HostCheck {
    host_check_as(conn, false)
}

/// A host integrity check as [`host_check`] runs it, or, `direct`, its entry called with no
/// statement of its own, as the integrity opcode calls it outside a frame.
fn host_check_as(conn: &Arc<Connection>, direct: bool) -> HostCheck {
    let (done, out) = std::sync::mpsc::channel();
    let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (conn, seen) = (conn.clone(), reached.clone());
    let thread = std::thread::spawn(move || {
        crate::race::at("integrity.host_checkpoint", move || {
            seen.store(true, std::sync::atomic::Ordering::Release)
        });
        let rows = if direct {
            conn.atoll()
                .unwrap()
                .check_pages(100)
                .map(|found| {
                    vec![vec![Value::build_text(if found.is_empty() {
                        "ok".to_string()
                    } else {
                        found.join("\n")
                    })]]
                })
                .map_err(|e| e.to_string())
        } else {
            conn.prepare("PRAGMA integrity_check")
                .and_then(|mut stmt| stmt.run_collect_rows())
                .map_err(|e| e.to_string())
        };
        let _ = done.send(rows);
    });
    (out, reached, thread)
}

/// A host check on a thread of its own: its result, sent once done, and whether it reached its
/// full checkpoint.
type HostCheck = (
    std::sync::mpsc::Receiver<std::result::Result<Vec<Vec<Value>>, String>>,
    Arc<std::sync::atomic::AtomicBool>,
    std::thread::JoinHandle<()>,
);

/// The host check's answer, polled until it comes or the check reaches its full checkpoint, a
/// minute at most: the answer, if it came.
fn answer_or_reached(
    out: &std::sync::mpsc::Receiver<std::result::Result<Vec<Vec<Value>>, String>>,
    reached: &std::sync::atomic::AtomicBool,
) -> Option<std::result::Result<Vec<Vec<Value>>, String>> {
    let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if let Ok(answer) = out.try_recv() {
            return Some(answer);
        }
        if reached.load(std::sync::atomic::Ordering::Acquire) || std::time::Instant::now() >= end {
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// ASYNC1 D2e (iii-d) G2 proofs 1 and 2: beside a cooperative walk holding its lease, or registered
/// as the state's contender, a host check on another thread is refused at its words: it asks for no
/// full checkpoint, registers nothing and lets the slot go; the walk then finishes with what the
/// synchronous check finds (mutant `host-unadmitted`).
#[test]
fn a_host_check_beside_a_cooperative_walk_is_refused() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_host_check_beside_a_cooperative_walk_is_refused"
        ),
        || {
            let (io, db, conn) = small_with_worker("/db/integrity-host-beside");
            let cp = atoll(&db).atoll().checkpointer_shared().clone();
            let sync = integrity(&conn);
            let bound = std::time::Duration::from_secs(20);
            // 1: the walk holds its lease.
            let mut x = conn.prepare("PRAGMA integrity_check").unwrap();
            let past = called_to_pages(&io, &db, &mut x);
            let before = (cp.lease_and_fulls_for_test(), cp.contenders_for_test());
            // As a statement: refused as it starts, beside the walk.
            let (out, _, y) = host_check(&conn);
            let refused_start = out.recv_timeout(bound).ok();
            let _ = y.join();
            // Its entry, called as its opcode calls it: refused by the admission word.
            let (out, reached, y) = host_check_as(&conn, true);
            let refused = answer_or_reached(&out, &reached);
            let went_on = reached.load(std::sync::atomic::Ordering::Acquire);
            // At once: a host check that went on holds the slot while its full checkpoint waits on
            // the walk's lease, and the walk's statement, dropped in autocommit, rolls back through
            // that slot, so no cleanup could end either.
            assert!(
                !went_on,
                "a host integrity check went on while a cooperative walk held the lease"
            );
            let after = (cp.lease_and_fulls_for_test(), cp.contenders_for_test());
            let slot_free = conn.atoll().unwrap().cell.try_lock_for_test().is_some();
            // A host check that went on holds the slot while its full checkpoint waits on the
            // walk's lease: the walk is dropped, not called, so its lease goes and the check ends.
            let first = match went_on {
                true => Err("dropped, the host check having gone on".to_string()),
                false => finished_after(&io, &db, &mut x, past),
            };
            drop(x);
            let refused = refused.or_else(|| out.recv_timeout(bound).ok());
            let _ = y.join();
            // 2: the walk registered as the state's contender.
            let holder = Holder::new(&db);
            let mut x = conn.prepare("PRAGMA integrity_check").unwrap();
            let checkpointed =
                called_to(&io, &db, &mut x, "checkpoint", 1_000).map(|c| waited(&io, &c));
            let held = holder.hold(true);
            let asked = called(&db, &mut x);
            let wait = match &asked {
                Called::Io(c, Some("lease")) => Some(c.clone()),
                _ => None,
            };
            let (out, _, y) = host_check(&conn);
            let refused_waiting_start = out.recv_timeout(bound).ok();
            let _ = y.join();
            let (out, reached, y) = host_check_as(&conn, true);
            let refused_waiting = answer_or_reached(&out, &reached);
            let went_on_waiting = reached.load(std::sync::atomic::Ordering::Acquire);
            assert!(
                !went_on_waiting,
                "a host integrity check went on beside a waiting cooperative walk"
            );
            let listed = (
                wait.as_ref().is_some_and(|c| cp.contends_wait_for_test(c)),
                cp.contenders_for_test(),
            );
            let let_go = holder.hold(false);
            let second = finished_after(&io, &db, &mut x, wait.ok_or_else(|| said(&asked)));
            drop(x);
            let refused_waiting = refused_waiting.or_else(|| out.recv_timeout(bound).ok());
            let _ = y.join();
            holder.end();
            for (case, refused) in [
                ("holding", &refused_start),
                ("waiting", &refused_waiting_start),
            ] {
                assert!(
                    matches!(refused, Some(Err(e)) if e.contains(START_REFUSED)),
                    "a host check's statement started beside a walk {case} its lease: {refused:?}"
                );
            }
            assert!(
                matches!(&refused, Some(Err(e)) if e.contains(HOST_REFUSED)),
                "the host check beside the walk: {refused:?}"
            );
            assert_eq!(
                after, before,
                "the refused host check took a lease, asked for a full checkpoint or registered"
            );
            assert!(slot_free, "the refused host check kept the slot");
            assert_eq!(first, Ok(sync.clone()), "the walk");
            assert_eq!(checkpointed, Ok(true), "the waiting walk's full checkpoint");
            assert!(held && let_go, "the state was not held and let go");
            assert!(
                matches!(&refused_waiting, Some(Err(e)) if e.contains(HOST_REFUSED)),
                "the host check beside the waiting walk: {refused_waiting:?}"
            );
            assert!(listed.0 && listed.1 <= 2, "the contenders: {listed:?}");
            assert_eq!(second, Ok(sync), "the waiting walk");
        },
    );
}

/// ASYNC1 D2e (iii-d) G2 proofs 3 and 4: a host check paused inside its walk on another thread
/// refuses a cooperative walk's first entry at its words, before the walk enters, asks or
/// registers; the host check then finishes, and a new cooperative walk is admitted and finishes.
/// Two host checks on two threads of one connection both finish, the second waiting on the slot,
/// and find the same (mutant `coop-ignores-host`).
#[test]
fn a_cooperative_walk_beside_a_host_check_is_refused() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_cooperative_walk_beside_a_host_check_is_refused"
        ),
        || {
            let (io, db, conn) = small_with_worker("/db/integrity-coop-beside");
            let cp = atoll(&db).atoll().checkpointer_shared().clone();
            let sync = integrity(&conn);
            let bound = std::time::Duration::from_secs(20);
            let (at_walk, walking) = std::sync::mpsc::channel::<()>();
            let (go, gone) = std::sync::mpsc::channel::<()>();
            let (done, out) = std::sync::mpsc::channel();
            let y_conn = conn.clone();
            let y = std::thread::spawn(move || {
                crate::race::at("integrity.host_walking", move || {
                    let _ = at_walk.send(());
                    let _ = gone.recv_timeout(std::time::Duration::from_secs(20));
                });
                let rows = y_conn
                    .prepare("PRAGMA integrity_check")
                    .and_then(|mut stmt| stmt.run_collect_rows())
                    .map_err(|e| e.to_string());
                let _ = done.send(rows);
            });
            let paused = walking.recv_timeout(bound).is_ok();
            let before = (cp.lease_and_fulls_for_test(), cp.contenders_for_test());
            // The walk's first entry, called as its opcode calls it: a statement could not get
            // there, its transaction's begin waiting on the slot the host check holds.
            // On a thread of its own, so a walk wrongly admitted, which then waits in `enter` on the
            // slot the host check holds, is seen by its admission word at once, not by its return.
            let (entered, entry) = std::sync::mpsc::channel();
            let entering = {
                let conn = conn.clone();
                std::thread::spawn(move || {
                    let mut walk = None;
                    let out = {
                        let _frame = crate::storage::atoll::cooperative::Frame::enter();
                        conn.atoll().unwrap().check_pages_in(&mut walk, 100)
                    };
                    let out = match out {
                        Ok(crate::types::IOResult::IO(_)) => Called::Failed("IO".to_string()),
                        Ok(crate::types::IOResult::Done((found, _))) => {
                            Called::Failed(format!("done: {found:?}"))
                        }
                        Err(e) => Called::Failed(e.to_string()),
                    };
                    let kept = walk.is_some();
                    {
                        let _frame = crate::storage::atoll::cooperative::Frame::enter();
                        drop(walk);
                    }
                    let _ = entered.send((out, kept));
                })
            };
            let both = |w: u64| w >> 63 == 1 && w & ((1 << 63) - 1) != 0;
            let mut answer = None;
            let mut word = conn.atoll().unwrap().walking_for_test();
            let end = std::time::Instant::now() + bound;
            while answer.is_none() && !both(word) && std::time::Instant::now() < end {
                answer = entry.try_recv().ok();
                word = conn.atoll().unwrap().walking_for_test();
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            // At once: a walk admitted beside the host check shares the word with it, and letting
            // the host check go would trip the admission's release assertion before these words.
            assert!(
                !both(word),
                "a cooperative integrity check was admitted beside a host one: word {word:#x}"
            );
            let (refused, kept) = answer.unwrap_or((Called::Failed("no answer".into()), true));
            let after = (cp.lease_and_fulls_for_test(), cp.contenders_for_test());
            let _ = entering.join();
            let _ = go.send(());
            let host = out.recv_timeout(bound).ok();
            let _ = y.join();
            let mut x = conn.prepare("PRAGMA integrity_check").unwrap();
            let admitted = called_out(&io, &db, &mut x, 100_000);
            drop(x);
            // Two host checks, the second waiting on the slot while the first holds it: a schedule
            // outside the one-call rule (two calls of one connection at once, the first paused in
            // its walk), kept as a regression of what holds in an explicit transaction. In
            // autocommit the first's halt ends the transaction the second joined while it waited,
            // as it does without this slice.
            conn.execute("BEGIN").unwrap();
            let (at_walk, walking) = std::sync::mpsc::channel::<()>();
            let (go, gone) = std::sync::mpsc::channel::<()>();
            let (done, out) = std::sync::mpsc::channel();
            let y_conn = conn.clone();
            let first_host = std::thread::spawn(move || {
                crate::race::at("integrity.host_walking", move || {
                    let _ = at_walk.send(());
                    let _ = gone.recv_timeout(std::time::Duration::from_secs(20));
                });
                let rows = y_conn
                    .prepare("PRAGMA integrity_check")
                    .and_then(|mut stmt| stmt.run_collect_rows())
                    .map_err(|e| e.to_string());
                let _ = done.send(rows);
            });
            let first_paused = walking.recv_timeout(bound).is_ok();
            let (second_out, _, second_host) = host_check(&conn);
            let second_waited = second_out
                .recv_timeout(std::time::Duration::from_millis(200))
                .is_err();
            let _ = go.send(());
            let hosts = (
                out.recv_timeout(bound).ok(),
                second_out.recv_timeout(bound).ok(),
            );
            let _ = first_host.join();
            let _ = second_host.join();
            assert!(paused, "the host check never reached its walk");
            assert!(
                matches!(&refused, Called::Failed(e) if e.contains(SIBLING_REFUSED)),
                "the walk beside a host one was not refused at its words: {}",
                said(&refused)
            );
            assert!(!kept, "the refused walk was kept");
            assert_eq!(
                after, before,
                "the refused walk took a lease, asked for a full checkpoint or registered"
            );
            assert_eq!(host, Some(Ok(sync.clone())), "the host check");
            assert_eq!(admitted, Ok(sync.clone()), "the walk after the host check");
            assert!(first_paused, "the first host check never reached its walk");
            assert!(
                second_waited,
                "the second host check did not wait on the first"
            );
            assert_eq!(
                hosts,
                (Some(Ok(sync.clone())), Some(Ok(sync))),
                "the two host checks"
            );
        },
    );
}

/// [`small_with_worker`], with enough rows that the page table has a level above its leaves.
fn tall_with_worker(path: &str) -> (Arc<dyn IO>, Arc<Database>, Arc<Connection>) {
    let (io, db, conn) = small_with_worker(path);
    conn.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    conn.execute("INSERT INTO big SELECT value, zeroblob(2000) FROM generate_series(1, 600)")
        .unwrap();
    (io, db, conn)
}

/// v2 §5 proof 2: errors a walk in frames meets. A tree's check failing is a finding, and the
/// walk goes on; a value's read failing outside the value ends the walk with its error; a value's
/// pages holding another value is a finding; a census or an entry failing ends the walk with its
/// error, no findings returned over it. After each the lease is released, the full checkpoint's
/// demand let go, and the statement stands at the opcode it failed at.
#[test]
fn errors_a_walk_in_frames_meets_are_findings_or_end_it() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::errors_a_walk_in_frames_meets_are_findings_or_end_it"
        ),
        || {
            use crate::storage::atoll::sql::integrity::WALK_FAILS_AT;
            let (io, db, conn) = tall_with_worker("/db/integrity-errors");
            let cp = atoll(&db).atoll().checkpointer_shared().clone();
            let tall = atoll(&db).atoll().store().heights()[0][0] >= 2;
            let store = atoll(&db).atoll().store().clone();
            drive(atoll(&db).io(), || store.plant_miscount(first_page(), 0)).unwrap();
            let mut seen = Vec::new();
            for at in [
                "tree.check",
                "value.pages",
                "value.changed",
                "census",
                "entry",
            ] {
                WALK_FAILS_AT.with(|f| f.set(Some(at)));
                let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
                chill(&db);
                let (mut rows, mut pc_at_io, mut out) = (Vec::new(), None, None);
                for calls in 1..=100_000 {
                    match called(&db, &mut stmt) {
                        Called::Io(c, place) => {
                            if place.is_some() {
                                pc_at_io = Some(stmt.pc_for_test());
                            }
                            if !waited(&io, &c) || !retired(&db) {
                                out = Some(Err(format!("call {calls}: its wait never finished")));
                                break;
                            }
                        }
                        Called::Row(row) => rows.push(row),
                        Called::Done => {
                            out = Some(Ok(std::mem::take(&mut rows)));
                            break;
                        }
                        Called::Failed(e) => {
                            out = Some(Err(e));
                            break;
                        }
                    }
                }
                let pc = stmt.pc_for_test();
                drop(stmt);
                let fired = WALK_FAILS_AT.with(|f| f.take()).is_none();
                let after = (cp.lease_and_fulls_for_test().0, cp.task_class());
                seen.push((at, out, fired, pc_at_io == Some(pc), after));
            }
            assert!(
                tall,
                "the page table has one level: no count above its leaves"
            );
            for (at, out, fired, same_pc, (lease, class)) in seen {
                let words = format!("an injected failure at {at}");
                assert!(fired, "{at}: the walk never reached the step");
                match at {
                    "tree.check" | "value.changed" => {
                        let found = format!("{out:?}");
                        let wanted = if at == "tree.check" {
                            words.as_str()
                        } else {
                            "its pages hold another value"
                        };
                        assert!(
                            matches!(out, Some(Ok(_))) && found.contains(wanted),
                            "{at}: not a finding of a walk that went on: {found}"
                        );
                    }
                    _ => {
                        assert!(
                            matches!(&out, Some(Err(e)) if e.contains(&words)),
                            "{at}: the walk did not end with its error: {out:?}"
                        );
                        assert!(same_pc, "{at}: the statement moved past the failed opcode");
                    }
                }
                assert_eq!(lease, 0, "{at}: the lease outlived the walk");
                assert_ne!(
                    class,
                    Some(crate::storage::atoll::executor::Class::Relief),
                    "{at}: the full checkpoint's demand outlived the walk"
                );
            }
        },
    );
}

/// Claims of the checkpoint's task attempted, and steps of its state taken, on this thread.
fn checkpoint_work_here() -> (u64, u64) {
    use crate::storage::atoll::checkpoint::{CLAIM_ATTEMPTS, WRITER_STEPS};
    (
        CLAIM_ATTEMPTS.with(|n| n.get()),
        WRITER_STEPS.with(|n| n.get()),
    )
}

/// ASYNC1 D2e (iii-d) v2 §5 proof 3: a full checkpoint in a frame, with the worker stepping the
/// checkpoint's task, waits on the worker's end, the task held off by a thread holding its state:
/// the frame's thread attempts no claim and steps no unit; once the state is let go the worker
/// ends it and the statement finishes. A second asks afresh. With writers stepping the task, a
/// frame's full checkpoint is refused at its words, claiming nothing (mutant `forced-caller`).
#[test]
fn a_full_checkpoint_in_a_frame_waits_on_the_worker_and_steps_nothing() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_full_checkpoint_in_a_frame_waits_on_the_worker_and_steps_nothing"
        ),
        || {
            let (io, db, conn) = small_with_worker("/db/integrity-checkpoint-frame");
            let cp = atoll(&db).atoll().checkpointer_shared().clone();
            let holder = Holder::new(&db);
            let held = holder.hold(true);
            let before = (checkpoint_work_here(), cp.lease_and_fulls_for_test().1);
            let mut stmt = conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            let first = called(&db, &mut stmt);
            let waiting = matches!(&first, Called::Io(c, _) if !c.finished());
            let first_words = said(&first);
            let during = checkpoint_work_here();
            // At once: a claim's wait is not the worker's end, so the steps after it would only
            // wait out their bounds before saying so.
            assert_eq!(
                during.0, before.0 .0,
                "the frame's checkpoint attempted a claim"
            );
            let let_go = holder.hold(false);
            let out = match first {
                Called::Io(c, _) => {
                    if waited(&io, &c) {
                        called_out(&io, &db, &mut stmt, 1_000)
                    } else {
                        Err("its wait never finished".to_string())
                    }
                }
                other => Err(said(&other)),
            };
            let after = checkpoint_work_here();
            drop(stmt);
            let mut again = conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            let second = called_out(&io, &db, &mut again, 1_000);
            drop(again);
            let asked = cp.lease_and_fulls_for_test().1 - before.1;
            holder.end();
            // Writers stepping the checkpoint's task: refused in a frame.
            let (_, wdb, wconn) = small_writers("/db/integrity-checkpoint-writers");
            let wbefore = checkpoint_work_here();
            let mut stmt = wconn.prepare("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            let refused = called(&wdb, &mut stmt);
            drop(stmt);
            let wafter = checkpoint_work_here();
            assert!(held && let_go, "the state was not held and let go");
            assert!(
                waiting,
                "the frame's full checkpoint did not wait: {first_words}"
            );
            assert_eq!(
                during.1, before.0 .1,
                "the frame's checkpoint stepped a unit"
            );
            assert!(out.is_ok(), "the statement after the worker's end: {out:?}");
            assert_eq!(after, during, "the statement's thread did checkpoint work");
            assert!(second.is_ok(), "the second full checkpoint: {second:?}");
            assert_eq!(asked, 2, "the two statements did not each ask for one");
            assert!(
                matches!(&refused, Called::Failed(e)
                    if e.contains("a full checkpoint in a cooperative frame needs the checkpoint worker")),
                "a frame's full checkpoint with writers stepping the task: {}",
                said(&refused)
            );
            assert_eq!(
                wafter, wbefore,
                "the refused checkpoint did checkpoint work"
            );
        },
    );
}

/// A small database as [`small_with_worker`] makes, its checkpoint's task stepped by writers.
fn small_writers(path: &str) -> (Arc<dyn IO>, Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = crate::storage::atoll::requests::opened_async(|| {
        Database::open(
            io.clone(),
            path,
            OpenOptions::new(Arc::new(SqliteDialect)).atoll(config()),
        )
        .unwrap()
    });
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t SELECT value, zeroblob(6000) FROM generate_series(1, 50)")
        .unwrap();
    (io, db, conn)
}

/// ASYNC1 D2e (iii-d) v2 §5 proof 5: a walk reset at its full checkpoint lets the checkpoint's
/// demand go; reset in its page walk it releases its lease, so a later checkpoint runs; and a walk
/// whose transaction ended and another began between its calls is refused at its next call
/// (mutants `hold-kept`, `identity-unchecked`).
#[test]
fn a_reset_walk_lets_go_and_a_walk_in_another_transaction_is_refused() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reset_walk_lets_go_and_a_walk_in_another_transaction_is_refused"
        ),
        || {
            use crate::storage::atoll::executor::Class;
            use crate::storage::atoll::sql::mode::RETAINED_LEASE;
            let (io, db, conn) = small_with_worker("/db/integrity-reset");
            let cp = atoll(&db).atoll().checkpointer_shared().clone();
            let bound = std::time::Duration::from_secs(20);
            // Reset at its full checkpoint, the worker held off.
            let holder = Holder::new(&db);
            let held = holder.hold(true);
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            let at_checkpoint = matches!(called(&db, &mut walk), Called::Io(_, Some("checkpoint")));
            let waiting = cp.task_class();
            walk.reset().unwrap();
            let after_reset = cp.task_class();
            drop(walk);
            let let_go = holder.hold(false);
            holder.end();
            // Reset in its page walk.
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            let past = called_to_pages(&io, &db, &mut walk);
            let leased = cp.lease_and_fulls_for_test().0 != 0;
            walk.reset().unwrap();
            let lease_after = cp.lease_and_fulls_for_test().0;
            let (done, later) = std::sync::mpsc::channel();
            let checkpointing = {
                let conn = conn.clone();
                std::thread::spawn(move || {
                    let out = conn
                        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
                        .and_then(|mut stmt| stmt.run_collect_rows())
                        .map(|_| ())
                        .map_err(|e| e.to_string());
                    let _ = done.send(out);
                })
            };
            // A lease kept past the reset holds the checkpoint off: no wait for it then.
            let ran = (lease_after == 0)
                .then(|| later.recv_timeout(bound).ok())
                .flatten();
            // A lease kept past the reset is let go here, so the checkpoint ends either way.
            drop(RETAINED_LEASE.with(|r| r.borrow_mut().take()));
            let ran = ran.or_else(|| later.recv_timeout(bound).ok());
            let _ = checkpointing.join();
            drop(walk);
            // Its transaction ended and another begun between its calls.
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            let past_again = called_to_pages(&io, &db, &mut walk);
            let waited_again = past_again.as_ref().is_ok_and(|c| waited(&io, c));
            let ended = conn
                .atoll()
                .unwrap()
                .end(false)
                .map(|mut ending| ending.finish().map_err(|e| e.to_string()));
            // Begun directly: no statement starts while the walk lives.
            let other_began = matches!(
                conn.atoll().unwrap().begin_reading(
                    crate::storage::atoll::sql::mode::Begin::Read,
                    &[],
                    &mut None,
                    None
                ),
                Ok(crate::types::IOResult::Done(_))
            );
            let resumed = called(&db, &mut walk);
            drop(walk);
            let other_ended = conn
                .atoll()
                .unwrap()
                .end(false)
                .map(|mut ending| ending.finish().map_err(|e| e.to_string()));
            assert!(
                held && let_go && at_checkpoint,
                "the walk did not wait at its full checkpoint"
            );
            assert_eq!(
                waiting,
                Some(Class::Relief),
                "the waiting walk held no demand"
            );
            assert_ne!(
                after_reset,
                Some(Class::Relief),
                "a reset walk kept its demand"
            );
            assert!(
                past.is_ok() && leased,
                "the walk held no lease in its page walk"
            );
            assert_eq!(lease_after, 0, "a reset walk kept its lease");
            assert_eq!(
                ran,
                Some(Ok(())),
                "a checkpoint after the reset did not run"
            );
            assert!(waited_again, "the third walk never reached its page walk");
            assert_eq!(ended, Some(Ok(())), "the walk's transaction did not end");
            assert!(other_began, "no other transaction began");
            // The refused walk's statement ends the transaction it met, or the end here does.
            assert!(
                matches!(other_ended, None | Some(Ok(()))),
                "the other transaction did not end: {other_ended:?}"
            );
            assert!(
                matches!(&resumed, Called::Failed(e) if e.contains("the integrity check's transaction changed")),
                "a walk resumed in another transaction was not refused: {}",
                said(&resumed)
            );
        },
    );
}

/// ASYNC1 D2e (iii-d), the admission word at its bounds (acceptance condition 1). With the host
/// count one short of its mask, one more host check is counted and finishes; with the count at the
/// mask, the next is refused at its words, as a refusal of statements in progress, and the word
/// is left as it was, never gaining the cooperative bit; a cooperative walk is refused beside it.
#[test]
fn the_admission_word_refuses_a_host_check_past_its_count() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::the_admission_word_refuses_a_host_check_past_its_count"
        ),
        || {
            const COOP: u64 = 1 << 63;
            let mask = COOP - 1;
            let (_io, _db, conn) = small_with_worker("/db/integrity-word-bounds");
            let sync = integrity(&conn);
            let check = || {
                conn.prepare("PRAGMA integrity_check")
                    .and_then(|mut stmt| stmt.run_collect_rows())
                    .map_err(|e| e.to_string())
            };
            let atoll = conn.atoll().unwrap();
            atoll.set_walking_for_test(mask - 1);
            let one_short = check();
            let after_one_short = atoll.walking_for_test();
            atoll.set_walking_for_test(mask);
            let at_mask = check();
            let after_at_mask = atoll.walking_for_test();
            let mut walk = None;
            let coop = {
                let _frame = crate::storage::atoll::cooperative::Frame::enter();
                atoll.check_pages_in(&mut walk, 100).map(|_| ())
            }
            .map_err(|e| e.to_string());
            let kept = walk.is_some();
            {
                let _frame = crate::storage::atoll::cooperative::Frame::enter();
                drop(walk);
            }
            let after_coop = atoll.walking_for_test();
            atoll.set_walking_for_test(0);
            let after = check();
            assert_eq!(
                one_short,
                Ok(sync.clone()),
                "a host check one short of the mask"
            );
            assert_eq!(after_one_short, mask - 1, "the word after it");
            assert!(
                at_mask.as_ref().is_err_and(|e| e
                    .contains("too many integrity checks on one connection")
                    && e.contains("SQL statements in progress")),
                "a host check at the mask: {at_mask:?}"
            );
            assert_eq!(
                after_at_mask, mask,
                "the refused host check changed the word"
            );
            // With no statement of its own, the walk is refused by its statements' admission before
            // the word: either refusal leaves the word as it was.
            assert!(
                coop.as_ref()
                    .is_err_and(|e| e.contains("SQL statements in progress"))
                    && !kept,
                "a cooperative walk at the mask: {coop:?}"
            );
            assert_eq!(after_coop, mask, "the refused walk changed the word");
            assert_eq!(after, Ok(sync), "a host check once the word is clear");
        },
    );
}

/// Calls `stmt` in frames until its walk returns IO at `place`, every page nothing keeps evicted
/// as the walk reaches each new step, so each step reads cold (the auditor's probe for III-D-I1).
fn called_to_cold(
    io: &Arc<dyn IO>,
    db: &Database,
    stmt: &mut crate::Statement,
    place: &str,
) -> std::result::Result<crate::Completion, String> {
    chill(db);
    let mut last = "";
    for calls in 1..=100_000 {
        match called(db, stmt) {
            Called::Io(c, Some(at)) if at == place => return Ok(c),
            Called::Io(c, at) => {
                if !waited(io, &c) || !retired(db) {
                    return Err(format!("call {calls}: its wait never finished"));
                }
                if let Some(at) = at {
                    if at != last {
                        last = at;
                        chill(db);
                    }
                }
            }
            other => return Err(format!("call {calls}: {}", said(&other))),
        }
    }
    Err(format!("not at {place}"))
}

/// What a walk beside one statement of its own connection came to.
struct Beside {
    name: &'static str,
    before: Vec<Vec<Value>>,
    reached: std::result::Result<(), String>,
    other: std::result::Result<(), String>,
    walk: std::result::Result<Vec<Vec<Value>>, String>,
    after: Vec<Vec<Value>>,
}

/// A walk of `conn` in frames to `place`, then `other` run on the same connection, then the walk
/// to its end; `explicit` in a transaction begun before the walk, with `setup` run in it first.
fn walked_beside(
    name: &'static str,
    explicit: bool,
    setup: &[&str],
    place: &'static str,
    other: &str,
) -> Beside {
    let (io, db, conn) = small_with_worker(&format!("/db/integrity-beside-{name}"));
    let before = integrity(&conn);
    if explicit {
        conn.execute("BEGIN").unwrap();
    }
    for sql in setup {
        conn.execute(*sql).unwrap();
    }
    let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
    let at = called_to_cold(&io, &db, &mut walk, place);
    let reached = at.as_ref().map(|_| ()).map_err(|e| e.clone());
    let other = conn.execute(other).map_err(|e| e.to_string());
    let walk_out = finished_after(&io, &db, &mut walk, at);
    drop(walk);
    if explicit {
        let _ = conn.execute("COMMIT");
    }
    let after = integrity(&conn);
    Beside {
        name,
        before,
        reached,
        other,
        walk: walk_out,
        after,
    }
}

/// The auditor's III-D-I1: from a cooperative walk's admission to its end, no other statement of
/// its connection starts, so nothing changes what its transaction sees between its calls. A write
/// at the tree check or the page table's census, a delete, a table created, and a write in
/// autocommit are each refused as they start; a ROLLBACK TO a savepoint taken before earlier writes
/// is refused the same way, and works once the walk has ended. Each walk ends with no finding on a
/// clean database (mutant `walk-not-exclusive`).
#[test]
fn a_walk_in_frames_keeps_its_connections_other_statements_out() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_walk_in_frames_keeps_its_connections_other_statements_out"
        ),
        || {
            const GROW: &str = "INSERT INTO t SELECT value, printf('%08d', value) || zeroblob(6000) FROM generate_series(1000, 1100)";
            const EARLIER: &str = "INSERT INTO t SELECT value, printf('%08d', value) || zeroblob(6000) FROM generate_series(2000, 2040)";
            let seen = [
                walked_beside("explicit-trees-grow", true, &[], "trees.check", GROW),
                walked_beside("explicit-account-grow", true, &[], "account.map", GROW),
                walked_beside(
                    "explicit-trees-shrink",
                    true,
                    &[],
                    "trees.check",
                    "DELETE FROM t WHERE id <= 25",
                ),
                walked_beside(
                    "explicit-account-create",
                    true,
                    &[],
                    "account.map",
                    "CREATE TABLE z(a)",
                ),
                walked_beside("autocommit-trees-grow", false, &[], "trees.check", GROW),
                walked_beside(
                    "explicit-rollback-to",
                    true,
                    &["SAVEPOINT s", EARLIER],
                    "account.map",
                    "ROLLBACK TO s",
                ),
            ];
            for s in &seen {
                assert!(
                    !matches!(&s.walk, Ok(rows) if *rows != ok()),
                    "an integrity check in frames reported findings on a clean database, beside \
                     a write of its own connection ({}): {:?}",
                    s.name,
                    s.walk
                );
            }
            for s in &seen {
                assert_eq!(
                    s.before,
                    ok(),
                    "{}: the database was not clean before",
                    s.name
                );
                assert_eq!(
                    s.reached,
                    Ok(()),
                    "{}: the walk never reached its step",
                    s.name
                );
                assert!(
                    s.other.as_ref().is_err_and(|e| e.contains(START_REFUSED)),
                    "{}: the other statement was not refused as it started: {:?}",
                    s.name,
                    s.other
                );
                assert_eq!(s.walk, Ok(ok()), "{}: the walk", s.name);
                assert_eq!(
                    s.after,
                    ok(),
                    "{}: the database was not clean after",
                    s.name
                );
            }
        },
    );
}

/// The auditor's III-D-I1: a statement of the connection suspended before the walk, at a row it
/// returned, keeps the walk out. Beside a reader, and beside a writer in a transaction, the walk's
/// first entry is refused at its words, so nothing of the walk runs and its word stays clear. Each
/// time the other statement then finishes, and a walk after it is admitted and finds nothing
/// (mutant `admission-ignores-others`).
#[test]
fn a_walk_beside_a_suspended_statement_of_its_connection_is_refused() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_walk_beside_a_suspended_statement_of_its_connection_is_refused"
        ),
        || {
            use crate::storage::atoll::sql::mode::WALKED_AT_IO;
            let (io, db, conn) = small_with_worker("/db/integrity-beside-suspended");
            let finish = |stmt: &mut crate::Statement| loop {
                match stmt.step() {
                    Ok(crate::StepResult::Row) => {}
                    Ok(crate::StepResult::Done) => break Ok(()),
                    Ok(crate::StepResult::IO) => stmt._io().step().unwrap(),
                    Ok(other) => break Err(format!("{other:?}")),
                    Err(e) => break Err(e.to_string()),
                }
            };
            // Beside a reader.
            let mut reader = conn.prepare("SELECT id FROM t").unwrap();
            let reading = matches!(reader.step(), Ok(crate::StepResult::Row));
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            chill(&db);
            let refused = called(&db, &mut walk);
            // At once: a walk admitted beside the reader holds the connection's word for a walk
            // another statement of its connection may yet follow.
            assert!(
                matches!(&refused, Called::Failed(e)
                    if e.contains("an integrity check while another statement of this connection is active")),
                "a walk was admitted beside a suspended statement: {}",
                said(&refused)
            );
            drop(walk);
            let read = finish(&mut reader);
            drop(reader);
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            let after_reader = called_out(&io, &db, &mut walk, 100_000);
            drop(walk);
            // Beside a writer, in a transaction.
            conn.execute("BEGIN").unwrap();
            let mut writer = conn
                .prepare("INSERT INTO t SELECT value, zeroblob(6000) FROM generate_series(3000, 3100) RETURNING id")
                .unwrap();
            let writing = matches!(writer.step(), Ok(crate::StepResult::Row));
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            WALKED_AT_IO.with(|w| w.borrow_mut().clear());
            let waited_out = called(&db, &mut walk);
            let walked = WALKED_AT_IO.with(|w| w.borrow().len());
            let word = conn.atoll().unwrap().walking_for_test();
            drop(walk);
            let wrote = finish(&mut writer);
            drop(writer);
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            let after_writer = called_out(&io, &db, &mut walk, 100_000);
            drop(walk);
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            assert!(reading, "the reader did not stop at its first row");
            assert_eq!(read, Ok(()), "the reader after the refused walk");
            assert_eq!(after_reader, Ok(ok()), "the walk after the reader");
            assert!(writing, "the writer did not stop at its first row");
            assert!(
                matches!(&waited_out, Called::Failed(e)
                    if e.contains("an integrity check while another statement of this connection is active")),
                "a walk was admitted beside a suspended writer: {}",
                said(&waited_out)
            );
            assert_eq!(
                (walked, word),
                (0, 0),
                "the walk ran beside a suspended writer"
            );
            assert_eq!(wrote, Ok(()), "the writer after the waiting walk");
            assert_eq!(after_writer, Ok(ok()), "the walk after the writer");
            assert_eq!(committed, Ok(()), "the commit");
        },
    );
}

/// ASYNC1 D2e (iii-d) v3, the statements' hold as one byte (#5721, #5723): an explicit
/// checkpoint's guard and a cooperative walk's guard each refuse a statement's start with their
/// own words, and each leaves nothing holding the connection's statements once it is let go.
#[test]
fn each_hold_on_a_connections_statements_refuses_with_its_own_words_and_leaves_none() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::each_hold_on_a_connections_statements_refuses_with_its_own_words_and_leaves_none"
        ),
        || {
            let (io, db, conn) = small_with_worker("/db/integrity-statements-hold");
            let start = |conn: &Arc<Connection>| {
                conn.prepare("SELECT id FROM t LIMIT 1")
                    .and_then(|mut stmt| stmt.run_collect_rows())
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            };
            let at_first = conn.exclusive_for_test();
            // An explicit checkpoint's hold, taken as `Connection::checkpoint` takes it.
            let checkpoint = conn
                .begin_explicit_checkpoint(conn.pager.load().clone(), false)
                .map_err(|e| e.to_string());
            let during_checkpoint = conn.exclusive_for_test();
            let beside_checkpoint = start(&conn);
            drop(checkpoint);
            let after_checkpoint = conn.exclusive_for_test();
            let after_checkpoint_start = start(&conn);
            // A cooperative walk's hold, its page walk under way.
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            let past = called_to_pages(&io, &db, &mut walk);
            let during_walk = conn.exclusive_for_test();
            let beside_walk = start(&conn);
            let walked = finished_after(&io, &db, &mut walk, past);
            drop(walk);
            let after_walk = conn.exclusive_for_test();
            let after_walk_start = start(&conn);
            assert_eq!(at_first, "none", "a hold before any was taken");
            assert_eq!(during_checkpoint, "checkpoint", "the checkpoint's hold");
            assert!(
                beside_checkpoint.as_ref().is_err_and(
                    |e| e.contains("cannot start a statement while a checkpoint is active")
                ),
                "a start beside the checkpoint: {beside_checkpoint:?}"
            );
            assert_eq!(after_checkpoint, "none", "the checkpoint's hold was left");
            assert_eq!(
                after_checkpoint_start,
                Ok(()),
                "a start after the checkpoint"
            );
            assert_eq!(during_walk, "integrity walk", "the walk's hold");
            assert!(
                beside_walk
                    .as_ref()
                    .is_err_and(|e| e.contains(START_REFUSED)),
                "a start beside the walk: {beside_walk:?}"
            );
            assert_eq!(walked, Ok(ok()), "the walk");
            assert_eq!(after_walk, "none", "the walk's hold was left");
            assert_eq!(after_walk_start, Ok(()), "a start after the walk");
        },
    );
}

/// What a check in frames came to beside a statement of its own connection tried in its row and
/// index pass.
struct RowPass {
    name: String,
    before: Vec<Vec<Value>>,
    /// IO returns of the check after its page walk ended, in the whole run.
    returns: usize,
    /// What held the connection's statements at the `k`th such return.
    hold: &'static str,
    tried: Option<std::result::Result<(), String>>,
    check: std::result::Result<Vec<Vec<Value>>, String>,
    after: Vec<Vec<Value>>,
}

/// One check in frames on a fresh clean database. Once its page walk has ended (an IO return its
/// walk did not make, after one it made) every page nothing keeps is evicted before each call, so
/// each read of its row and index pass returns IO. After the `k`th such return, its IO waited for,
/// `other` is tried on the same connection outside any frame; then the check is called to its end
/// (the auditor's probe for III-D-I2, #5729).
fn row_pass_beside(tag: &str, explicit: bool, k: usize, other: &str) -> RowPass {
    let name = format!(
        "{tag}-{}-k{k}",
        if explicit { "explicit" } else { "autocommit" }
    );
    let (io, db, conn) = small_with_worker(&format!("/db/integrity-row-pass-{name}"));
    let before = integrity(&conn);
    if explicit {
        conn.execute("BEGIN").unwrap();
    }
    let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
    chill(&db);
    let mut walked = false;
    let mut past = false;
    let mut returns = 0usize;
    let mut hold = "not reached";
    let mut tried = None;
    let mut rows = Vec::new();
    let mut check = Err("not done in 200000 calls".to_string());
    for calls in 1..=200_000 {
        if past {
            chill(&db);
        }
        match called(&db, &mut stmt) {
            Called::Io(c, at) => {
                walked |= at.is_some();
                past |= walked && at.is_none();
                if !waited(&io, &c) || !retired(&db) {
                    check = Err(format!("call {calls}: its wait never finished"));
                    break;
                }
                if past {
                    returns += 1;
                    if returns == k {
                        hold = conn.exclusive_for_test();
                        tried = Some(conn.execute(other).map_err(|e| e.to_string()));
                    }
                }
            }
            Called::Row(row) => rows.push(row),
            Called::Done => {
                check = Ok(std::mem::take(&mut rows));
                break;
            }
            Called::Failed(e) => {
                check = Err(format!("call {calls}: {e}"));
                break;
            }
        }
    }
    drop(stmt);
    if explicit {
        let _ = conn.execute("COMMIT");
    }
    let after = integrity(&conn);
    RowPass {
        name,
        before,
        returns,
        hold,
        tried,
        check,
        after,
    }
}

/// The auditor's III-D-I2: a check in frames keeps its connection's statements from its page walk
/// to its first row, not only through its page walk. A delete or an insert of its own connection
/// tried at a return of its row and index pass, near its start, its middle and its end, is refused
/// as it starts, in a transaction and in autocommit, so the check says ok on a clean database
/// (mutant `hold-ends-with-the-page-walk`).
#[test]
fn a_check_in_frames_keeps_its_connections_statements_out_through_its_row_pass() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_check_in_frames_keeps_its_connections_statements_out_through_its_row_pass"
        ),
        || {
            const WRITES: [(&str, &str); 2] = [
                ("delete-low", "DELETE FROM t WHERE id <= 10"),
                (
                    "insert-high",
                    "INSERT INTO t SELECT value, printf('%08d', value) || zeroblob(6000) FROM generate_series(1000, 1010)",
                ),
            ];
            let mut controls = Vec::new();
            let mut seen = Vec::new();
            for explicit in [true, false] {
                // The control tries nothing; its count of returns places the series.
                let control = row_pass_beside("control", explicit, usize::MAX, "SELECT 1");
                let b = control.returns;
                controls.push(control);
                let mut ks = vec![1, 2, 4, b / 4, b / 2];
                ks.extend((0..5).map(|back| b.saturating_sub(back)));
                ks.retain(|k| (1..=b).contains(k));
                ks.sort_unstable();
                ks.dedup();
                for (tag, write) in WRITES {
                    for &k in &ks {
                        seen.push(row_pass_beside(tag, explicit, k, write));
                    }
                }
            }
            for s in &seen {
                assert!(
                    matches!(&s.tried, Some(Err(e)) if e.contains(START_REFUSED)),
                    "a statement of the check's own connection started in its row and index pass \
                     ({}, the hold then {:?}): {:?}",
                    s.name,
                    s.hold,
                    s.tried
                );
            }
            for c in &controls {
                assert!(c.returns > 0, "{}: no return after the page walk", c.name);
            }
            for s in controls.iter().chain(&seen) {
                assert_eq!(
                    s.before,
                    ok(),
                    "{}: the database was not clean before",
                    s.name
                );
                assert_eq!(s.check, Ok(ok()), "{}: the check in frames", s.name);
                assert_eq!(
                    s.after,
                    ok(),
                    "{}: the database was not clean after",
                    s.name
                );
            }
            for s in &seen {
                assert_eq!(
                    s.hold, "integrity walk",
                    "{}: the hold in the row pass",
                    s.name
                );
            }
        },
    );
}

/// Calls `stmt`, a check in frames, until its first IO return after its page walk ended (one its
/// walk did not make, after one it made), every page nothing keeps evicted as the walk reaches
/// each new step, so what its row and index pass reads first is cold; that return's IO waited for,
/// or why not.
fn called_past_the_walk(
    io: &Arc<dyn IO>,
    db: &Database,
    stmt: &mut crate::Statement,
) -> std::result::Result<(), String> {
    let mut last = None;
    chill(db);
    for calls in 1..=100_000 {
        match called(db, stmt) {
            Called::Io(c, at) => {
                if !waited(io, &c) || !retired(db) {
                    return Err(format!("call {calls}: its wait never finished"));
                }
                if at.is_none() && last.is_some() {
                    return Ok(());
                }
                if at.is_some() && at != last {
                    last = at;
                    chill(db);
                }
            }
            other => return Err(format!("call {calls}: {}", said(&other))),
        }
    }
    Err("no return after the page walk".to_string())
}

/// The auditor's III-D-I2, its other side: a check in frames lets go of its connection's
/// statements at its first row, which a synchronous check never held. In its row and index pass a
/// second walk's entry is refused at the sibling's words, so it takes no second hold. On a clean
/// database, at its "ok" row before it is called to its end, the hold is none and a write of its
/// connection's transaction and a read in autocommit each start; on a database with a page
/// nothing reaches, at its first row, the page walk's finding, before its row and index pass
/// (mutants `row-keeps-the-hold`, `second-walk-under-the-hold`).
#[test]
fn a_check_in_frames_lets_its_connection_go_at_its_first_row() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_check_in_frames_lets_its_connection_go_at_its_first_row"
        ),
        || {
            let read = |conn: &Arc<Connection>| {
                conn.prepare("SELECT count(*) FROM t")
                    .and_then(|mut stmt| stmt.run_collect_rows())
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            };
            // Clean, in a transaction: a write at the "ok" row.
            let (io, db, conn) = small_with_worker("/db/integrity-first-row-explicit");
            conn.execute("BEGIN").unwrap();
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            let past = called_past_the_walk(&io, &db, &mut stmt);
            // At once: a check past its row has let its claim go, so a second walk's entry below
            // would find nothing to refuse it.
            assert_eq!(past, Ok(()), "the check never passed its page walk");
            let in_pass = conn.exclusive_for_test();
            let second = entered_in_frame(&conn);
            // At once: a second walk admitted under the kept hold takes a second guard over the
            // same byte, and its release would leave the check's guard to release nothing.
            assert!(
                matches!(&second, Called::Failed(e) if e.contains(SIBLING_REFUSED)),
                "a second walk was admitted in a check's row and index pass: {}",
                said(&second)
            );
            let mut first = None;
            for _ in 0..100_000 {
                match called(&db, &mut stmt) {
                    Called::Io(c, _) => {
                        if !waited(&io, &c) || !retired(&db) {
                            break;
                        }
                    }
                    other => {
                        first = Some(other);
                        break;
                    }
                }
            }
            let at_row = conn.exclusive_for_test();
            let wrote = conn
                .execute("INSERT INTO t VALUES (5000, '00005000')")
                .map_err(|e| e.to_string());
            let rest = called_out(&io, &db, &mut stmt, 100_000);
            drop(stmt);
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            let explicit_after = integrity(&conn);
            drop(conn);
            drop(db);
            // Clean, in autocommit: a read at the "ok" row.
            let (io, db, conn) = small_with_worker("/db/integrity-first-row-autocommit");
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            let auto_past = called_past_the_walk(&io, &db, &mut stmt);
            let mut auto_first = None;
            for _ in 0..100_000 {
                match called(&db, &mut stmt) {
                    Called::Io(c, _) => {
                        if !waited(&io, &c) || !retired(&db) {
                            break;
                        }
                    }
                    other => {
                        auto_first = Some(other);
                        break;
                    }
                }
            }
            let auto_at_row = conn.exclusive_for_test();
            let auto_read = read(&conn);
            let auto_rest = called_out(&io, &db, &mut stmt, 100_000);
            drop(stmt);
            drop(conn);
            drop(db);
            // A page nothing reaches: the page walk's finding is the first row.
            let (io, db, conn) = small_with_worker("/db/integrity-first-row-finding");
            leaked(&db, 1);
            let found_sync = integrity(&conn);
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            chill(&db);
            let mut finding = None;
            for _ in 0..100_000 {
                match called(&db, &mut stmt) {
                    Called::Io(c, _) => {
                        if !waited(&io, &c) || !retired(&db) {
                            break;
                        }
                    }
                    other => {
                        finding = Some(other);
                        break;
                    }
                }
            }
            let finding_hold = conn.exclusive_for_test();
            let finding_read = read(&conn);
            let finding_rest = called_out(&io, &db, &mut stmt, 100_000);
            drop(stmt);
            assert_eq!(in_pass, "integrity walk", "the hold in the row pass");
            assert!(
                matches!(&first, Some(Called::Row(row)) if *row == ok()[0]),
                "the check's first row: {:?}",
                first.as_ref().map(said)
            );
            assert_eq!(at_row, "none", "the hold at the check's first row");
            assert_eq!(wrote, Ok(()), "a write after the check's first row");
            assert_eq!(rest, Ok(vec![]), "the check after its first row");
            assert_eq!(committed, Ok(()), "the commit");
            assert_eq!(explicit_after, ok(), "the database after");
            assert_eq!(
                auto_past,
                Ok(()),
                "the autocommit check never passed its page walk"
            );
            assert!(
                matches!(&auto_first, Some(Called::Row(row)) if *row == ok()[0]),
                "the autocommit check's first row: {:?}",
                auto_first.as_ref().map(said)
            );
            assert_eq!(
                auto_at_row, "none",
                "the hold at the autocommit check's first row"
            );
            assert_eq!(
                auto_read,
                Ok(()),
                "a read after the autocommit check's first row"
            );
            assert_eq!(
                auto_rest,
                Ok(vec![]),
                "the autocommit check after its first row"
            );
            assert_ne!(found_sync, ok(), "the leaked page was not found");
            assert!(
                matches!(&finding, Some(Called::Row(row)) if *row == found_sync[0]),
                "the check's first row on a leaked page: {:?}, the synchronous check's {:?}",
                finding.as_ref().map(said),
                found_sync
            );
            assert_eq!(
                finding_hold, "none",
                "the hold at the check's first finding"
            );
            assert_eq!(
                finding_read,
                Ok(()),
                "a read after the check's first finding"
            );
            assert_eq!(
                finding_rest,
                Ok(found_sync[1..].to_vec()),
                "the check after its first finding"
            );
        },
    );
}

/// The auditor's III-D-I2, its ends: a check in frames that ends in its row and index pass, before
/// any row, lets go of its connection's statements as it ends. Reset, dropped, or interrupted
/// through its connection (its abort, before any reset), the hold is none and a statement of its
/// connection starts (mutants `reset-keeps-the-hold`, `abort-keeps-the-hold`).
#[test]
fn a_check_in_frames_ended_in_its_row_pass_lets_its_connection_go() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_check_in_frames_ended_in_its_row_pass_lets_its_connection_go"
        ),
        || {
            let read = |conn: &Arc<Connection>| {
                conn.prepare("SELECT count(*) FROM t")
                    .and_then(|mut stmt| stmt.run_collect_rows())
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            };
            let (io, db, conn) = small_with_worker("/db/integrity-row-pass-ends");
            // Reset.
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            let reset_past = called_past_the_walk(&io, &db, &mut stmt);
            let reset_in_pass = conn.exclusive_for_test();
            let reset = stmt.reset().map_err(|e| e.to_string());
            let reset_hold = conn.exclusive_for_test();
            let reset_read = read(&conn);
            let again = called_out(&io, &db, &mut stmt, 100_000);
            drop(stmt);
            // Dropped.
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            let drop_past = called_past_the_walk(&io, &db, &mut stmt);
            let drop_in_pass = conn.exclusive_for_test();
            drop(stmt);
            let drop_hold = conn.exclusive_for_test();
            let drop_read = read(&conn);
            // Interrupted: its abort lets go, before any reset.
            let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
            let interrupt_past = called_past_the_walk(&io, &db, &mut stmt);
            let interrupt_in_pass = conn.exclusive_for_test();
            conn.interrupt();
            let mut interrupted = None;
            for _ in 0..100_000 {
                match called(&db, &mut stmt) {
                    Called::Io(c, _) => {
                        if !waited(&io, &c) || !retired(&db) {
                            break;
                        }
                    }
                    other => {
                        interrupted = Some(other);
                        break;
                    }
                }
            }
            let interrupt_hold = conn.exclusive_for_test();
            let interrupt_read = read(&conn);
            drop(stmt);
            let after = integrity(&conn);
            for (case, past, in_pass) in [
                ("reset", &reset_past, reset_in_pass),
                ("dropped", &drop_past, drop_in_pass),
                ("interrupted", &interrupt_past, interrupt_in_pass),
            ] {
                assert_eq!(
                    past,
                    &Ok(()),
                    "{case}: the check never passed its page walk"
                );
                assert_eq!(
                    in_pass, "integrity walk",
                    "{case}: the hold in the row pass"
                );
            }
            assert_eq!(reset, Ok(()), "the reset");
            assert_eq!(reset_hold, "none", "the hold after the check's reset");
            assert_eq!(reset_read, Ok(()), "a read after the check's reset");
            assert_eq!(again, Ok(ok()), "the check called again after its reset");
            assert_eq!(drop_hold, "none", "the hold after the check was dropped");
            assert_eq!(drop_read, Ok(()), "a read after the check was dropped");
            assert!(
                matches!(&interrupted, Some(Called::Failed(e)) if e == "Interrupt"),
                "the check after its interrupt: {:?}",
                interrupted.as_ref().map(said)
            );
            assert_eq!(
                interrupt_hold, "none",
                "the hold after the check's interrupt"
            );
            assert_eq!(interrupt_read, Ok(()), "a read after the check's interrupt");
            assert_eq!(after, ok(), "the database after");
        },
    );
}
