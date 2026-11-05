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

/// A page allocated and adopted that nothing reaches is found as never used, through the pragma.
#[test]
fn a_leaked_page_is_found() {
    let (db, conn) = used("/db/integrity-leak");
    let store = atoll(&db).atoll().store().clone();
    let io = atoll(&db).io().clone();
    let class = crate::storage::atoll::page::SizeClass::new(0).unwrap();
    let pid = drive(&io, || store.allocate(class)).unwrap();
    drive(&io, || store.adopt(&[pid])).unwrap();
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
    drive(adb.io(), || store.plant_retired(pid)).unwrap();
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

/// A page an operation holds provisional, not yet adopted by the record that links it, is
/// accounted for: it is neither reached nor never used.
#[test]
fn a_provisional_page_is_accounted_for() {
    let (db, conn) = used("/db/integrity-provisional");
    let adb = atoll(&db);
    let store = adb.atoll().store().clone();
    let class = crate::storage::atoll::page::SizeClass::new(0).unwrap();
    let pid = drive(adb.io(), || store.allocate(class)).unwrap();
    assert!(store.provisional_pages().contains(&pid));
    assert_eq!(integrity(&conn), ok());
}
