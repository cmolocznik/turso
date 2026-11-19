//! The page arm of an integrity check: a database in use accounts for every page, through
//! `PRAGMA integrity_check` and directly, and each fault planted behind the trees' backs is found.

use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::Check;
use crate::storage::atoll::drive;
use crate::storage::atoll::page::PageId;
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::sql::mode::TxnOf;
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
        .with_head(crate::storage::atoll::txn::versioned::HEADER)
        .with_reserves();
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

/// A tree the catalog names that the dropped list also holds is found with what both say
/// (DROPPED-LIST1): the tree id the catalog names it by at the check's snapshot, the check's
/// transaction, whether that snapshot sees the list's entry, and the entry as stored. The entry is
/// a real one, a dropped table's, kept from its drain by a reader's snapshot and copied under the
/// named tree's root, so the list's versioned read takes it as it takes any committed drop.
#[test]
fn a_tree_the_catalog_names_and_the_dropped_list_holds_is_found_with_both_sides() {
    // In a child: a failure unwinds through the reader's open transaction, an Atoll frame, which
    // ends the process; the parent shows the child's words.
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_tree_the_catalog_names_and_the_dropped_list_holds_is_found_with_both_sides"
        ),
        || {
            let (db, conn) = used("/db/integrity-dropped-both");
            let adb = atoll(&db);
            conn.execute("CREATE TABLE u(x)").unwrap();
            conn.execute("INSERT INTO u VALUES (1)").unwrap();
            let gone = entry_of(&conn, adb, "u").root;
            let reader = db.connect(ConnectOpts::new()).unwrap();
            reader.execute("BEGIN").unwrap();
            assert_eq!(rows(&reader, "SELECT count(*) FROM u").len(), 1);
            conn.execute("DROP TABLE u").unwrap();
            let list = adb.dropped().tree();
            let stored = {
                let mut cursor = crate::storage::atoll::btree::Cursor::new();
                let key = gone.raw().to_be_bytes();
                let found = drive(adb.io(), || {
                    list.seek(&mut cursor, &key, crate::storage::atoll::btree::Seek::Ge)
                })
                .unwrap();
                assert!(
                    found && cursor.key() == Some(&key[..]),
                    "u's root on the dropped list"
                );
                cursor.value().unwrap().to_vec()
            };
            let root = entry_of(&conn, adb, "t").root;
            let Value::Numeric(crate::numeric::Numeric::Integer(id)) =
                rows(&conn, "SELECT rootpage FROM sqlite_schema WHERE name = 't'")[0][0].clone()
            else {
                panic!("t's rootpage");
            };
            // The stored entry starts with its version header, the tree's head of 24 bytes.
            let mut put = crate::storage::atoll::btree::Put::with_entry(
                &root.raw().to_be_bytes(),
                stored,
                24,
                crate::storage::atoll::btree::Mode::Insert,
            );
            drive(adb.io(), || list.put(&mut put)).unwrap();
            found(
                &conn,
                &[
                    format!(
                    "tree {} is named by the catalog and on the Dropped list, to be freed: the \
                     catalog names it as tree id {id} at the check's snapshot (transaction ",
                    root.raw()
                ),
                    "the list's entry is seen by that snapshot, its stored value ".to_string(),
                ],
            );
            reader.execute("ROLLBACK").unwrap();
        },
    );
}

/// A list entry the check's snapshot does not see is found with what decided it (GATE1): the
/// check's snapshot, the horizon, the newest commit of any slot and the schema's, and the entry's
/// slot's commit log as that snapshot reads it. The entry is a dropped table's, copied under a
/// named tree's root with its start moved past every commit of its slot.
#[test]
fn a_dropped_list_entry_the_check_does_not_see_is_found_with_what_decided_it() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_dropped_list_entry_the_check_does_not_see_is_found_with_what_decided_it"
        ),
        || {
            use crate::storage::atoll::txn::versioned::{Version, HEADER};
            let (db, conn) = used("/db/integrity-dropped-unseen");
            let adb = atoll(&db);
            conn.execute("CREATE TABLE u(x)").unwrap();
            conn.execute("INSERT INTO u VALUES (1)").unwrap();
            let gone = entry_of(&conn, adb, "u").root;
            let reader = db.connect(ConnectOpts::new()).unwrap();
            reader.execute("BEGIN").unwrap();
            assert_eq!(rows(&reader, "SELECT count(*) FROM u").len(), 1);
            conn.execute("DROP TABLE u").unwrap();
            let list = adb.dropped().tree();
            let mut stored = {
                let mut cursor = crate::storage::atoll::btree::Cursor::new();
                let key = gone.raw().to_be_bytes();
                let found = drive(adb.io(), || {
                    list.seek(&mut cursor, &key, crate::storage::atoll::btree::Seek::Ge)
                })
                .unwrap();
                assert!(
                    found && cursor.key() == Some(&key[..]),
                    "u's root on the dropped list"
                );
                cursor.value().unwrap().to_vec()
            };
            let mut version = Version::decode(&stored[..HEADER]).expect("a version header");
            version.ts += 1 << 40;
            stored[..HEADER].copy_from_slice(&version.encode());
            let root = entry_of(&conn, adb, "t").root;
            let mut put = crate::storage::atoll::btree::Put::with_entry(
                &root.raw().to_be_bytes(),
                stored,
                HEADER,
                crate::storage::atoll::btree::Mode::Insert,
            );
            drive(adb.io(), || list.put(&mut put)).unwrap();
            found(
                &conn,
                &[
                    "the list's entry is not seen by that snapshot".to_string(),
                    format!(
                        "tree {}'s entry (slot {}, start {}): the check's snapshot ",
                        root.raw(),
                        version.slot,
                        version.ts
                    ),
                    ", the horizon ".to_string(),
                    ", the newest commit of any slot ".to_string(),
                    format!(
                        "; slot {}: the last start committed by that snapshot ",
                        version.slot
                    ),
                ],
            );
            reader.execute("ROLLBACK").unwrap();
        },
    );
}

/// HAND-BACK1 (the hunt's fast seed 11): a schema statement refused with BusySnapshot inside a
/// transaction that already holds the write gate exclusive, having written under it, keeps the
/// hold: another connection's check meanwhile is Busy, not a walk of the transaction's uncommitted
/// drop. A: BEGIN and a read (its snapshot); B: a committed write to t; A: DROP INDEX of another
/// table's index (the gate taken, its entry on the Dropped list); A: CREATE INDEX on t, refused.
#[test]
fn a_schema_write_refused_in_a_transaction_that_holds_the_gate_keeps_it() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_schema_write_refused_in_a_transaction_that_holds_the_gate_keeps_it"
        ),
        || {
            let (db, a) = used("/db/hand-back");
            a.execute("CREATE TABLE u(x)").unwrap();
            a.execute("CREATE INDEX u_x ON u(x)").unwrap();
            let b = db.connect(ConnectOpts::new()).unwrap();
            let c = db.connect(ConnectOpts::new()).unwrap();
            a.execute("BEGIN").unwrap();
            assert_eq!(rows(&a, "SELECT count(*) FROM t").len(), 1);
            b.execute("INSERT INTO t(v) VALUES ('after the snapshot')")
                .unwrap();
            a.execute("DROP INDEX u_x").unwrap();
            let refused = a
                .execute("CREATE INDEX t_again ON t(substr(v, 1, 4))")
                .map_err(|e| e.to_string());
            let checked = c
                .prepare("PRAGMA integrity_check")
                .and_then(|mut s| s.run_collect_rows())
                .map_err(|e| e.to_string());
            let _ = a.execute("ROLLBACK");
            let after = integrity(&c);
            assert!(
                matches!(&refused, Err(e) if e.contains("snapshot")),
                "the CREATE INDEX was not refused for its stale snapshot: {refused:?}"
            );
            assert!(
                matches!(&checked, Err(e) if e.contains("busy") || e.contains("Busy")),
                "a check beside a transaction holding the gate with an uncommitted DROP INDEX \
                 did not wait for it: {checked:?}"
            );
            assert_eq!(after, ok(), "the check after the ROLLBACK");
        },
    );
}

/// HAND-BACK1's state: A in a transaction that dropped u's index under the gate, its CREATE INDEX
/// on t refused for a stale snapshot; B and C other connections.
fn a_refused_schema_write_beside_its_drop(
    path: &str,
) -> (
    Arc<Database>,
    Arc<Connection>,
    Arc<Connection>,
    Result<(), String>,
) {
    let (db, a) = used(path);
    a.execute("CREATE TABLE u(x)").unwrap();
    a.execute("INSERT INTO u VALUES (1), (2)").unwrap();
    a.execute("CREATE INDEX u_x ON u(x)").unwrap();
    let b = db.connect(ConnectOpts::new()).unwrap();
    let c = db.connect(ConnectOpts::new()).unwrap();
    a.execute("BEGIN").unwrap();
    assert_eq!(rows(&a, "SELECT count(*) FROM t").len(), 1);
    b.execute("INSERT INTO t(v) VALUES ('after the snapshot')")
        .unwrap();
    a.execute("DROP INDEX u_x").unwrap();
    let refused = a
        .execute("CREATE INDEX t_again ON t(substr(v, 1, 4))")
        .map_err(|e| e.to_string());
    (db, a, c, refused)
}

/// What the catalog names and what the dropped list holds, in words.
fn schema_and_list(db: &Arc<Database>, conn: &Arc<Connection>) -> String {
    format!(
        "catalog {:?}; dropped {}",
        rows(
            conn,
            "SELECT type, name FROM sqlite_schema WHERE name LIKE 'u%' ORDER BY name"
        ),
        dropped_keys(db).len()
    )
}

/// HAND-BACK1, the other face: another connection's schema statement beside A's uncommitted DROP
/// INDEX waits for A (Busy), whether A then rolls back or commits.
#[test]
fn a_schema_statement_beside_a_refused_transactions_drop_waits() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_schema_statement_beside_a_refused_transactions_drop_waits"
        ),
        || {
            for end in ["ROLLBACK", "COMMIT"] {
                let (db, a, c, refused) =
                    a_refused_schema_write_beside_its_drop(&format!("/db/hand-back-ddl-{end}"));
                let drop_table = c.execute("DROP TABLE u").map_err(|e| e.to_string());
                let create = c
                    .execute("CREATE TABLE beside(x)")
                    .map_err(|e| e.to_string());
                let ended = a.execute(end).map_err(|e| e.to_string());
                let after = schema_and_list(&db, &c);
                let check = integrity(&c);
                eprintln!(
                    "HAND-BACK1 {end}: refused {refused:?}; C's DROP TABLE {drop_table:?}; C's \
                     CREATE TABLE {create:?}; A's {end} {ended:?}; after {after}; check {check:?}"
                );
                let busy = |r: &Result<(), String>| matches!(r, Err(e) if e.to_lowercase().contains("busy"));
                assert!(
                    busy(&drop_table) && busy(&create),
                    "{end}: another connection's DDL beside A's uncommitted DROP INDEX: DROP \
                     TABLE {drop_table:?}, CREATE TABLE {create:?}"
                );
                assert_eq!(check, ok(), "{end}: the check after A's {end}");
            }
        },
    );
}

/// HAND-BACK1 (c): A, refused with BusySnapshot on one statement, keeps what it wrote before:
/// its COMMIT commits the DROP INDEX, its ROLLBACK takes it back; the gate is free after either.
#[test]
fn a_transaction_refused_one_schema_statement_commits_or_rolls_back_the_rest() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_transaction_refused_one_schema_statement_commits_or_rolls_back_the_rest"
        ),
        || {
            for (end, index_left) in [("COMMIT", 0), ("ROLLBACK", 1)] {
                let (db, a, c, refused) =
                    a_refused_schema_write_beside_its_drop(&format!("/db/hand-back-end-{end}"));
                let ended = a.execute(end).map_err(|e| e.to_string());
                let index = rows(&c, "SELECT count(*) FROM sqlite_schema WHERE name = 'u_x'");
                let free = c
                    .execute("CREATE TABLE gate_free(x)")
                    .map_err(|e| e.to_string());
                eprintln!(
                    "HAND-BACK1 end {end}: refused {refused:?}; ended {ended:?}; u_x {index:?}; \
                     another DDL after {free:?}; {}",
                    schema_and_list(&db, &c)
                );
                assert_eq!(ended, Ok(()), "A's {end} after its refused CREATE INDEX");
                assert_eq!(
                    index,
                    vec![vec![Value::from_i64(index_left)]],
                    "u_x after A's {end}"
                );
                assert_eq!(free, Ok(()), "the gate after A's {end}");
                assert_eq!(integrity(&c), ok(), "the check after A's {end}");
            }
        },
    );
}

/// HAND-BACK1 (#6540): a transaction that wrote only rows under a shared hold is refused its first
/// schema statement for a stale snapshot before its exclusive take (the check ahead of the take):
/// it keeps its shared hold alone, so another writer goes beside it, and its ROLLBACK undoes its
/// row.
#[test]
fn a_row_writer_refused_its_first_schema_statement_keeps_only_its_shared_hold() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_row_writer_refused_its_first_schema_statement_keeps_only_its_shared_hold"
        ),
        || {
            let (db, a) = used("/db/hand-back-rows");
            a.execute("CREATE TABLE u(x)").unwrap();
            let b = db.connect(ConnectOpts::new()).unwrap();
            let c = db.connect(ConnectOpts::new()).unwrap();
            a.execute("BEGIN").unwrap();
            assert_eq!(rows(&a, "SELECT count(*) FROM t").len(), 1);
            b.execute("INSERT INTO t(v) VALUES ('after the snapshot')")
                .unwrap();
            a.execute("INSERT INTO u VALUES (1)").unwrap();
            let refused = a
                .execute("CREATE INDEX t_again ON t(substr(v, 1, 4))")
                .map_err(|e| e.to_string());
            let beside = c
                .execute("INSERT INTO u VALUES (2)")
                .map_err(|e| e.to_string());
            let ended = a.execute("ROLLBACK").map_err(|e| e.to_string());
            let after = c
                .execute("INSERT INTO u VALUES (3)")
                .map_err(|e| e.to_string());
            assert!(
                matches!(&refused, Err(e) if e.contains("snapshot")),
                "the CREATE INDEX was not refused for its stale snapshot: {refused:?}"
            );
            assert_eq!(
                (beside, ended, after),
                (Ok(()), Ok(()), Ok(())),
                "another writer beside the refused row writer, A's ROLLBACK, the writer again"
            );
            assert_eq!(
                rows(&c, "SELECT x FROM u ORDER BY x"),
                vec![vec![Value::from_i64(2)], vec![Value::from_i64(3)]]
            );
            assert_eq!(integrity(&c), ok());
        },
    );
}

/// HAND-BACK1 (#6544): an exclusive take MADE and then refused. A wrote a row under its shared
/// hold; its CREATE INDEX on t passes the stale check ahead of the take and waits for B, another
/// shared holder, to drain; B's commit (a row of t) makes A's read stale; A's take is made and the
/// check after it refuses. A has written, so it keeps the exclusive hold until it ends: C's INSERT
/// is Busy meanwhile and goes after A's ROLLBACK.
#[test]
fn an_exclusive_take_made_and_refused_by_a_row_writer_keeps_the_hold_until_it_ends() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::an_exclusive_take_made_and_refused_by_a_row_writer_keeps_the_hold_until_it_ends"
        ),
        || {
            let (db, a) = used("/db/hand-back-drain");
            a.execute("CREATE TABLE u(x)").unwrap();
            let b = db.connect(ConnectOpts::new()).unwrap();
            let c = db.connect(ConnectOpts::new()).unwrap();
            a.execute("PRAGMA busy_timeout = 60000").unwrap();
            a.execute("BEGIN").unwrap();
            assert_eq!(rows(&a, "SELECT count(*) FROM t").len(), 1);
            a.execute("INSERT INTO u VALUES (1)").unwrap();
            b.execute("BEGIN").unwrap();
            b.execute("INSERT INTO t(v) VALUES ('in the drain')")
                .unwrap();
            let mut create = a
                .prepare("CREATE INDEX t_again ON t(substr(v, 1, 4))")
                .unwrap();
            let mut waited = 0;
            let first = loop {
                match create.step() {
                    Ok(crate::StepResult::IO) if waited < 3 => {
                        waited += 1;
                        create._io().step().unwrap();
                    }
                    Ok(crate::StepResult::Yield) => {}
                    other => break other.map(|r| format!("{r:?}")).map_err(|e| e.to_string()),
                }
            };
            assert_eq!(
                (waited, first),
                (3, Ok("IO".to_string())),
                "A's CREATE INDEX did not wait in the drain for B"
            );
            b.execute("COMMIT").unwrap();
            let refused = loop {
                match create.step() {
                    Ok(crate::StepResult::IO) => create._io().step().unwrap(),
                    Ok(crate::StepResult::Yield) => {}
                    Ok(crate::StepResult::Done) => break Ok(()),
                    Ok(other) => break Err(format!("{other:?}")),
                    Err(e) => break Err(e.to_string()),
                }
            };
            drop(create);
            let beside = c
                .execute("INSERT INTO u VALUES (2)")
                .map_err(|e| e.to_string());
            let ended = a.execute("ROLLBACK").map_err(|e| e.to_string());
            let after = c
                .execute("INSERT INTO u VALUES (3)")
                .map_err(|e| e.to_string());
            assert!(
                matches!(&refused, Err(e) if e.contains("snapshot")),
                "A's CREATE INDEX after the drain was not refused for its stale snapshot: \
                 {refused:?}"
            );
            assert!(
                matches!(&beside, Err(e) if e.to_lowercase().contains("busy")),
                "a writer beside A, which made its exclusive take and wrote before: {beside:?}"
            );
            assert_eq!(
                (ended, after),
                (Ok(()), Ok(())),
                "A's ROLLBACK, then C's INSERT"
            );
            assert_eq!(rows(&c, "SELECT x FROM u"), vec![vec![Value::from_i64(3)]]);
            assert_eq!(integrity(&c), ok());
        },
    );
}

/// GATE1 (#6551): a statement parked in the schema retry's wait (below a published DROP INDEX, the
/// horizon held) and then dropped holds nothing: once the horizon goes, the drop commits, the
/// connection runs another statement and closes, and a check finds nothing.
#[test]
fn a_statement_dropped_at_the_schema_retrys_wait_holds_nothing() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_statement_dropped_at_the_schema_retrys_wait_holds_nothing"
        ),
        || {
            let (db, conn) = used("/db/hg-dropped-at-wait");
            let other = db.connect(ConnectOpts::new()).unwrap();
            let txns = atoll(&db).atoll().transactions();
            let cookie = || {
                atoll(&db)
                    .header()
                    .committed_at(super::super::header::Field::SchemaCookie)
            };
            let before = cookie();
            txns.hold_horizon(true);
            let dropper = std::thread::spawn(move || {
                other.execute("DROP INDEX t_v").map_err(|e| e.to_string())
            });
            let mut published = false;
            for _ in 0..10_000_000 {
                if cookie() > before {
                    published = true;
                    break;
                }
                std::thread::yield_now();
            }
            assert!(published, "the drop's commit was never published");
            let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
            let parked = loop {
                match stmt.step() {
                    Ok(crate::StepResult::Yield) => {}
                    other => break other.map(|r| format!("{r:?}")).map_err(|e| e.to_string()),
                }
            };
            assert_eq!(
                parked,
                Ok("IO".to_string()),
                "the statement did not park at the wait"
            );
            assert!(
                stmt.pending_io_for_test().is_some(),
                "the parked statement holds no completion"
            );
            // Dropped at the wait: its reset runs the idle check (no transaction left in the
            // connection's cells), and nothing it held keeps the horizon or the drop waiting.
            drop(stmt);
            txns.hold_horizon(false);
            let next = conn.execute("INSERT INTO t(v) VALUES ('after the drop of the parked one')");
            assert_eq!(dropper.join().unwrap(), Ok(()), "the drop");
            conn.close().unwrap();
            let reader = db.connect(ConnectOpts::new()).unwrap();
            assert_eq!(
                (next.map_err(|e| e.to_string()), integrity(&reader)),
                (Ok(()), ok()),
                "the connection's next statement, then a check"
            );
        },
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
        .with_head(crate::storage::atoll::txn::versioned::HEADER)
        .with_reserves();
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
            Ok(crate::StepResult::Yield) => check._io().step().unwrap(),
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
    holder: Option<&Holder>,
) -> Walked {
    use crate::storage::atoll::sql::mode::WALKED_AT_IO;
    WALKED_AT_IO.with(|w| w.borrow_mut().clear());
    let taken = || WALKED_AT_IO.with(|w| std::mem::take(&mut *w.borrow_mut()));
    // With `holder`, the checkpointer's state held until the walk returns at its full checkpoint,
    // so the worker cannot end that checkpoint before the walk looks at it; let go on every way out.
    let holding = holder.map(Holding::new);
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
                let at_checkpoint = WALKED_AT_IO.with(|w| {
                    let w = w.borrow();
                    w.len() > walked && w[w.len() - 1].0 == "checkpoint"
                });
                if let Some(h) = holding.as_ref().filter(|_| at_checkpoint) {
                    if let Err(e) = h.let_go_after(&c) {
                        return Walked {
                            out: Err(format!("call {calls}: {e}")),
                            at_io: taken(),
                        };
                    }
                }
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

/// The checkpointer's state held for [`walked_in_frames`] until the walk's return at its full
/// checkpoint, and let go there or when the walk ends.
struct Holding<'a> {
    holder: &'a Holder,
    rested: bool,
    held: bool,
    let_go: std::cell::Cell<bool>,
}

impl<'a> Holding<'a> {
    /// Holds the state, from the checkpoint worker at rest ([`Holder::worker_at_rest`]).
    fn new(holder: &'a Holder) -> Self {
        let rested = holder.worker_at_rest();
        let held = rested && holder.hold(true);
        Self {
            holder,
            rested,
            held,
            let_go: std::cell::Cell::new(false),
        }
    }

    /// Lets the state go at the walk's return at its full checkpoint, `wait` that return's: the
    /// wait was found unfinished with the state still held, or why not.
    fn let_go_after(&self, wait: &crate::Completion) -> std::result::Result<(), String> {
        let unfinished = !wait.finished();
        let let_go = !self.let_go.replace(true) && self.holder.hold(false);
        if self.held && unfinished && let_go {
            Ok(())
        } else {
            Err(format!(
                "the full checkpoint not held at the walk's return: rested {}, held {}, \
                 unfinished {unfinished}, let go {let_go}",
                self.rested, self.held
            ))
        }
    }
}

impl Drop for Holding<'_> {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        if !self.let_go.replace(true) {
            self.holder.hold(false);
        }
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
            let holder = Holder::new(&db);
            let walk = |sql: &str| {
                let mut stmt = conn.prepare(sql).unwrap();
                let walked = walked_in_frames(&io, &db, &mut stmt, 100_000, Some(&holder));
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
            holder.end();
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

/// One call of `stmt` in a test frame, the IO thread held through it. A yield is called again, as a
/// poller calls again a statement that yielded: a cooperative statement may yield before its first
/// IO, while a checkpoint it waits for runs on another thread, and under load it does.
fn called(db: &Database, stmt: &mut crate::Statement) -> Called {
    use crate::storage::atoll::sql::mode::WALKED_AT_IO;
    let walked = WALKED_AT_IO.with(|w| w.borrow().len());
    let end = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let stepped = loop {
        let held = held_io(db);
        let stepped = {
            let _frame = crate::storage::atoll::cooperative::Frame::enter();
            stmt.step()
        };
        drop(held);
        if !matches!(stepped, Ok(crate::StepResult::Yield)) {
            break stepped;
        }
        if std::time::Instant::now() >= end {
            return Called::Failed("it yielded for 20 seconds".to_string());
        }
        std::thread::yield_now();
    };
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

/// [`called_to`] at the walk's full checkpoint, from the checkpoint worker at rest, the
/// checkpointer's state held through its calls so the worker cannot end that checkpoint before the
/// walk looks at it; the state let go after. The worker wakes a full checkpoint's waiters after it
/// lets the state go, so with a wake still owed, the wait of a walk that registers under the hold
/// would be finished by an earlier checkpoint's end. Its wait, found unfinished with the state
/// still held, then waited for, or why not.
fn called_to_checkpoint(
    io: &Arc<dyn IO>,
    db: &Database,
    holder: &Holder,
    stmt: &mut crate::Statement,
) -> std::result::Result<crate::Completion, String> {
    if !holder.worker_at_rest() {
        return Err("the checkpoint worker never came to rest".into());
    }
    let held = holder.hold(true);
    let at = called_to(io, db, stmt, "checkpoint", 1_000);
    let unfinished = at.as_ref().is_ok_and(|c| !c.finished());
    let let_go = holder.hold(false);
    let c = at?;
    if !(held && unfinished && let_go) {
        return Err(format!(
            "the full checkpoint not held at the walk's return: held {held}, unfinished \
             {unfinished}, let go {let_go}"
        ));
    }
    if !waited(io, &c) {
        return Err("the walk's full checkpoint never ended".into());
    }
    Ok(c)
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
    cp: Arc<crate::storage::atoll::checkpoint::Checkpointer>,
}

impl Holder {
    fn new(db: &Database) -> Self {
        let cp = atoll(db).atoll().checkpointer_shared().clone();
        let holds = cp.clone();
        let (tell, orders) = std::sync::mpsc::channel::<(bool, std::sync::mpsc::Sender<()>)>();
        let thread = std::thread::spawn(move || {
            let cp = holds;
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
        Self { tell, thread, cp }
    }

    /// Holds the state, or lets it go: whether the thread answered.
    fn hold(&self, hold: bool) -> bool {
        order(&self.tell, hold)
    }

    /// Waits, with a deadline, for the checkpoint worker at rest: every full checkpoint asked for
    /// done, and its task's last recorded step "idle". A step is recorded after its hold's drop,
    /// which wakes the waiters of a full checkpoint it ended, so none of those wakes is still owed;
    /// and nothing but this thread wakes the worker before the next hold. Whether it came to rest.
    fn worker_at_rest(&self) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let rests = self.cp.fulls_done() >= self.cp.fulls_requested()
                && self
                    .cp
                    .task_steps_for_test()
                    .last()
                    .is_some_and(|(said, _)| *said == "idle");
            if rests {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
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
                called_to_checkpoint(&io, &db, &holder, &mut stmt).map(|c| waited(&io, &c));
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
                called_to_checkpoint(&io, &db, &holder, &mut stmt).map(|c| waited(&io, &c));
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
                    called_to_checkpoint(&io, &db, &holder, &mut stmt).map(|c| waited(&io, &c));
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
        conn.atoll()
            .unwrap()
            .check_pages_in(TxnOf::Connection, &mut walk, 100)
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
                called_to_checkpoint(&io, &db, &holder, &mut one).map(|c| waited(&io, &c));
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
                    "checkpoint" => called_to_checkpoint(&io, &db, &holder, &mut stmt).map(|_| ()),
                    "lease" => called_to_checkpoint(&io, &db, &holder, &mut stmt).and_then(|c| {
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
                .check_pages(TxnOf::Connection, 100)
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
            let slot_free = conn
                .atoll()
                .unwrap()
                .own()
                .cell
                .try_lock_for_test()
                .is_some();
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
                called_to_checkpoint(&io, &db, &holder, &mut x).map(|c| waited(&io, &c));
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
                        conn.atoll()
                            .unwrap()
                            .check_pages_in(TxnOf::Connection, &mut walk, 100)
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
            let at_checkpoint = called_to(&io, &db, &mut walk, "checkpoint", 1_000).is_ok();
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
                .end(TxnOf::Connection, false)
                .map(|mut ending| ending.finish().map_err(|e| e.to_string()));
            // Begun directly: no statement starts while the walk lives.
            let other_began = matches!(
                conn.atoll().unwrap().begin_reading(
                    TxnOf::Connection,
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
                .end(TxnOf::Connection, false)
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
                atoll
                    .check_pages_in(TxnOf::Connection, &mut walk, 100)
                    .map(|_| ())
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

/// Calls `stmt`, a check in frames, past every return it makes before its walk's first entry (an
/// IO its walk did not make: its begin's, or a yield of the memory backend), waiting for each, and
/// counting them in `before`: the first return its walk's entry decides, or why not.
fn called_to_its_walk(
    io: &Arc<dyn IO>,
    db: &Database,
    stmt: &mut crate::Statement,
    before: &mut usize,
) -> Called {
    for _ in 0..1_000 {
        match called(db, stmt) {
            Called::Io(c, None) => {
                *before += 1;
                if !waited(io, &c) || !retired(db) {
                    return Called::Failed("a wait before the walk never finished".to_string());
                }
            }
            other => return other,
        }
    }
    Called::Failed("no return of the walk's entry in 1000 calls".to_string())
}

/// The auditor's III-D-I1: a statement of the connection suspended before the walk, at a row it
/// returned, keeps the walk out. Beside a reader, and beside a writer in a transaction, the walk's
/// first entry is refused at its words, so nothing of the walk runs and its word stays clear. Each
/// time the other statement then finishes, and a walk after it is admitted and finds nothing
/// (mutant `admission-ignores-others`). The check is called past what it returns before its walk's
/// entry (the auditor's #5746): a synchronous check first leaves every page clean, so the check's
/// begin beside the reader reads cold and returns IO first.
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
                    Ok(crate::StepResult::Yield) => stmt._io().step().unwrap(),
                    Ok(other) => break Err(format!("{other:?}")),
                    Err(e) => break Err(e.to_string()),
                }
            };
            // Beside a reader.
            let clean = integrity(&conn);
            let mut reader = conn.prepare("SELECT id FROM t").unwrap();
            let reading = matches!(reader.step(), Ok(crate::StepResult::Row));
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            chill(&db);
            let mut before_reader = 0;
            let refused = called_to_its_walk(&io, &db, &mut walk, &mut before_reader);
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
            let mut before_writer = 0;
            let waited_out = called_to_its_walk(&io, &db, &mut walk, &mut before_writer);
            let walked = WALKED_AT_IO.with(|w| w.borrow().len());
            let word = conn.atoll().unwrap().walking_for_test();
            drop(walk);
            let wrote = finish(&mut writer);
            drop(writer);
            let mut walk = conn.prepare("PRAGMA integrity_check").unwrap();
            let after_writer = called_out(&io, &db, &mut walk, 100_000);
            drop(walk);
            let committed = conn.execute("COMMIT").map_err(|e| e.to_string());
            eprintln!(
                "BESIDE_SUSPENDED returns before the walk's entry: reader {before_reader}, writer \
                 {before_writer}"
            );
            assert_eq!(clean, ok(), "the database was not clean before");
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
/// (mutant `row-keeps-the-hold`).
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

// ---------------------------------------------------------------- the table-valued forms (ICK-TVF1)

/// The forms of the check a statement can read: the two table-valued ones and the pragma.
const CHECK_FORMS: [&str; 3] = [
    "SELECT * FROM pragma_integrity_check",
    "SELECT * FROM pragma_quick_check",
    "PRAGMA integrity_check",
];

/// `sql` run to its end on `conn`, its waits for IO stepped in place: its rows, or "busy" for a
/// take of the write gate refused, or its error's words.
fn checked(conn: &Arc<Connection>, sql: &str) -> std::result::Result<Vec<Vec<Value>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    loop {
        match stmt.step() {
            Ok(crate::StepResult::IO) => stmt._io().step().unwrap(),
            Ok(crate::StepResult::Row) => {
                rows.push(stmt.row().unwrap().get_values().cloned().collect())
            }
            Ok(crate::StepResult::Done) => return Ok(rows),
            Ok(crate::StepResult::Busy) | Err(crate::LimboError::Busy) => {
                return Err("busy".to_string())
            }
            Ok(crate::StepResult::Yield) => stmt._io().step().unwrap(),
            Ok(other) => return Err(format!("{other:?}")),
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// ICK-TVF1: a statement reading a table-valued form of the check takes the write gate exclusive,
/// as the pragma does. Beside another connection's writer holding only its BEGIN every form says
/// ok; beside one with a write open every form is refused as the pragma is, where the forms used to
/// walk and report as wrong the pages the writer had not finished. Once the writer commits or
/// rolls back, every form says ok (mutant `tvf-unmarked`).
#[test]
fn a_table_valued_check_beside_an_open_writer_is_refused_as_the_pragma_is() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_table_valued_check_beside_an_open_writer_is_refused_as_the_pragma_is"
        ),
        || {
            let writes: [(&str, &[&str]); 6] = [
                ("begin only", &[]),
                (
                    "a table created",
                    &[
                        "CREATE TABLE z(x)",
                        "INSERT INTO z SELECT value FROM generate_series(1, 30)",
                    ],
                ),
                ("an index dropped", &["DROP INDEX t_v"]),
                ("a row inserted", &["INSERT INTO t VALUES (1000, 'w')"]),
                ("a row deleted", &["DELETE FROM t WHERE id = 1"]),
                ("a row updated", &["UPDATE t SET v = 'u' WHERE id = 2"]),
            ];
            let mut seen = Vec::new();
            for (n, (case, sqls)) in writes.iter().enumerate() {
                for end in ["COMMIT", "ROLLBACK"] {
                    let (db, conn) = used(&format!("/db/integrity-tvf-{n}-{end}"));
                    let writer = db.connect(ConnectOpts::new()).unwrap();
                    writer.execute("BEGIN").unwrap();
                    for sql in *sqls {
                        writer.execute(*sql).unwrap();
                    }
                    let beside: Vec<_> = CHECK_FORMS.iter().map(|f| checked(&conn, f)).collect();
                    writer.execute(end).unwrap();
                    let after: Vec<_> = CHECK_FORMS.iter().map(|f| checked(&conn, f)).collect();
                    seen.push((*case, end, beside, after));
                }
            }
            for (case, end, beside, after) in &seen {
                let wanted = if *case == "begin only" {
                    Ok(ok())
                } else {
                    Err("busy".to_string())
                };
                for (form, got) in CHECK_FORMS.iter().zip(beside) {
                    assert_eq!(
                        got, &wanted,
                        "{case}, then {end}: {form} beside the open writer"
                    );
                }
                for (form, got) in CHECK_FORMS.iter().zip(after) {
                    assert_eq!(
                        got,
                        &Ok(ok()),
                        "{case}, then {end}: {form} after the writer"
                    );
                }
            }
        },
    );
}

/// A page walk whose transaction does not hold the write gate exclusive is refused at its own
/// words: a trigger's body reading a table-valued form of the check, its subprogram compiled apart
/// from the statement that fires it, which so takes no exclusive hold. The firing statement fails
/// whole and the database stays as it was, synchronously and in test frames (mutants
/// `hold-untested` and `frame-hold-untested`).
#[test]
fn a_page_walk_without_the_write_gate_exclusive_is_refused() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_page_walk_without_the_write_gate_exclusive_is_refused"
        ),
        || {
            let trigger = [
                "CREATE TABLE log(r TEXT)",
                "CREATE TRIGGER t_checked AFTER INSERT ON t BEGIN \
                 INSERT INTO log SELECT integrity_check FROM pragma_integrity_check; END",
            ];
            let words = crate::storage::atoll::sql::mode::GATE_NOT_EXCLUSIVE;
            let (_db, conn) = used("/db/integrity-tvf-trigger");
            for sql in trigger {
                conn.execute(sql).unwrap();
            }
            let fired = conn
                .execute("INSERT INTO t VALUES (5000, 'fires')")
                .map_err(|e| e.to_string());
            let logged = rows(&conn, "SELECT count(*) FROM log");
            let kept = rows(&conn, "SELECT count(*) FROM t WHERE id = 5000");
            let after = integrity(&conn);

            let (io, db, conn) = small_with_worker("/db/integrity-tvf-trigger-frames");
            for sql in trigger {
                conn.execute(sql).unwrap();
            }
            let mut x = conn
                .prepare("INSERT INTO t VALUES (5000, 'fires')")
                .unwrap();
            let in_frames = walked_in_frames(&io, &db, &mut x, 10_000, None).out;
            drop(x);
            let logged_in_frames = rows(&conn, "SELECT count(*) FROM log");
            let kept_in_frames = rows(&conn, "SELECT count(*) FROM t WHERE id = 5000");
            let after_in_frames = integrity(&conn);

            let none = vec![vec![Value::from_i64(0)]];
            assert!(
                matches!(&fired, Err(e) if e.contains(words)),
                "a trigger's walk under a shared hold: {fired:?}"
            );
            assert_eq!(logged, none, "the refused statement's trigger logged");
            assert_eq!(kept, none, "the refused statement's row was kept");
            assert_eq!(after, ok(), "the database after the refusal");
            assert!(
                matches!(&in_frames, Err(e) if e.contains(words)),
                "a trigger's walk under a shared hold, in frames: {in_frames:?}"
            );
            assert_eq!(logged_in_frames, none, "in frames, the trigger logged");
            assert_eq!(kept_in_frames, none, "in frames, the row was kept");
            assert_eq!(
                after_in_frames,
                ok(),
                "the database after the refusal in frames"
            );
        },
    );
}

/// A check holds the write gate from its first step (integrity.rs, module comment): another
/// connection's DROP INDEX begun while the check is suspended at its first return for IO gets Busy,
/// whatever its transaction then does, and the check ends "ok" (DROPPED-LIST1, #6432-#6433).
fn a_drop_while_a_check_is_suspended(path: &str, ending: &str) {
    let (io, db, conn) = used_with_worker(path);
    let other = db.connect(ConnectOpts::new()).unwrap();
    other.execute("PRAGMA busy_timeout = 0").unwrap();
    let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
    let first = {
        let _frame = crate::storage::atoll::cooperative::Frame::enter();
        stmt.step()
    };
    assert!(
        matches!(first, Ok(crate::StepResult::IO)),
        "the check's first step: {first:?}"
    );
    other.execute("BEGIN").unwrap();
    let dropped = other.execute("DROP INDEX t_v").map_err(|e| e.to_string());
    assert_eq!(dropped, Err("Database is busy".to_string()), "{ending}");
    let _ = other.execute(match ending {
        "commit" => "COMMIT",
        _ => "ROLLBACK",
    });
    let mut out = Vec::new();
    for _ in 0..100_000 {
        let stepped = {
            let _frame = crate::storage::atoll::cooperative::Frame::enter();
            stmt.step()
        };
        match stepped.unwrap() {
            crate::StepResult::Row => out.push(
                stmt.row()
                    .unwrap()
                    .get_values()
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            crate::StepResult::Done => break,
            crate::StepResult::IO => {
                if let Some(c) = stmt.pending_io_for_test() {
                    io.wait(&c);
                }
            }
            crate::StepResult::Yield => {}
            other => panic!("the check stepped to {other:?}"),
        }
    }
    assert_eq!(out, ok(), "{ending}");
}

#[test]
fn a_drop_while_a_check_is_suspended_is_busy_and_the_check_finds_nothing_left_open() {
    a_drop_while_a_check_is_suspended("/db/drop-under-check-open", "open");
}

#[test]
fn a_drop_while_a_check_is_suspended_is_busy_and_the_check_finds_nothing_then_committed() {
    a_drop_while_a_check_is_suspended("/db/drop-under-check-commit", "commit");
}

#[test]
fn a_drop_while_a_check_is_suspended_is_busy_and_the_check_finds_nothing_then_rolled_back() {
    a_drop_while_a_check_is_suspended("/db/drop-under-check-rollback", "rollback");
}

/// DROPPED-LIST1, the auditor's #6434 item 1: a drop rolled back, then a check from another
/// connection, "ok" wanted. For DROP INDEX and DROP TABLE.
#[test]
fn a_drop_rolled_back_leaves_nothing_a_check_finds() {
    for (n, sql) in [(1, "DROP INDEX t_v"), (2, "DROP TABLE t")] {
        let (_io, db, conn) = used_with_worker(&format!("/db/drop-rolled-{n}"));
        let other = db.connect(ConnectOpts::new()).unwrap();
        other.execute("BEGIN").unwrap();
        other.execute(sql).unwrap();
        other.execute("ROLLBACK").unwrap();
        assert_eq!(integrity(&conn), ok(), "{sql} rolled back");
    }
}

/// HORIZON-GATE1 (#6487-#6490), the capture's road: a check whose snapshot, taken once it holds
/// the gate, is the horizon, and the horizon not yet over a DROP INDEX committed just before. The
/// catalog at that snapshot still names the index's tree, the list holds the drop's committed
/// entry, and the snapshot does not see it: the capture's words, with nothing left behind.
#[test]
fn a_check_whose_snapshot_lags_a_committed_drop_finds_nothing() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_check_whose_snapshot_lags_a_committed_drop_finds_nothing"
        ),
        || {
            let (db, conn) = used("/db/integrity-horizon-lag");
            let other = db.connect(ConnectOpts::new()).unwrap();
            let txns = atoll(&db).atoll().transactions();
            txns.hold_horizon(true);
            // The drop publishes its commit and lets the gate go, then waits for the horizon to cover it:
            // on a thread of its own, which the check runs beside.
            let dropper = std::thread::spawn(move || {
                other.execute("DROP INDEX t_v").map_err(|e| e.to_string())
            });
            // The drop's entry on the list, read raw, says its put landed; the check then runs once the
            // drop has let the gate go, Busy until then.
            let before = dropped_keys(&db).len();
            let mut landed = false;
            for _ in 0..1_000_000 {
                if dropped_keys(&db).len() > before {
                    landed = true;
                    break;
                }
                std::thread::yield_now();
            }
            // The check's snapshot is taken as it takes the gate; its own full checkpoint, which waits
            // for the horizon, comes after: the horizon is let go there.
            let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
            {
                let (db, released) = (db.clone(), released.clone());
                crate::race::at("integrity.host_checkpoint", move || {
                    atoll(&db).atoll().transactions().hold_horizon(false);
                    released.store(true, std::sync::atomic::Ordering::SeqCst);
                });
            }
            // Mended, the take waits for the horizon before its snapshot: let go there too.
            {
                let (db, released) = (Arc::downgrade(&db), released.clone());
                crate::race::at("txn.exclusive_covering", move || {
                    let db = db.upgrade().expect("the database is open at the take");
                    atoll(&db).atoll().transactions().hold_horizon(false);
                    released.store(true, std::sync::atomic::Ordering::SeqCst);
                });
            }
            let mut checked = Err("never ran".to_string());
            for _ in 0..100_000 {
                checked = conn
                    .prepare("PRAGMA integrity_check")
                    .and_then(|mut s| s.run_collect_rows())
                    .map_err(|e| e.to_string());
                if !matches!(&checked, Err(e) if e.contains("busy") || e.contains("Busy")) {
                    break;
                }
                std::thread::yield_now();
            }
            assert!(landed, "the drop's entry never reached the list");
            if !released.load(std::sync::atomic::Ordering::SeqCst) {
                txns.hold_horizon(false);
            }
            let drop_result = dropper.join().unwrap();
            let after = integrity(&conn);
            assert_eq!(
        (drop_result, format!("{checked:?}"), after),
        (Ok(()), format!("{:?}", Ok::<_, String>(ok())), ok()),
        "(the drop, the check run while the drop's commit waited on the horizon, the check after)"
    );
        },
    );
}

thread_local! {
    /// The reprepares the last `a_writer_below_a_committed_drop` counted for its statement.
    static LAST_REPREPARES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// HORIZON-GATE1's writers (#6489 item 2): DROP INDEX t_v committed on its own thread with the
/// horizon held, so a fresh writer on another connection takes the gate with a snapshot below the
/// drop's commit, which still names t_v; the horizon is let go at that writer's commit. What the
/// writer's statement came to, and the integrity check once both have ended.
fn a_writer_below_a_committed_drop(
    path: &str,
    sql: &'static str,
) -> (Result<(), String>, Vec<Vec<Value>>) {
    let (db, conn) = used(path);
    let other = db.connect(ConnectOpts::new()).unwrap();
    let txns = atoll(&db).atoll().transactions();
    let cookie = || {
        atoll(&db)
            .header()
            .committed_at(super::super::header::Field::SchemaCookie)
    };
    let before = cookie();
    txns.hold_horizon(true);
    let dropper =
        std::thread::spawn(move || other.execute("DROP INDEX t_v").map_err(|e| e.to_string()));
    // The drop's commit published (its schema cookie with it), the horizon still below it.
    let mut published = false;
    for _ in 0..10_000_000 {
        if cookie() > before {
            published = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(published, "the drop's commit was never published");
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        // The database held weakly: a hook the writer never reached is dropped at the thread's end.
        let (db, released) = (Arc::downgrade(&db), released.clone());
        crate::race::at("txn.before_reserved_at", move || {
            let db = db
                .upgrade()
                .expect("the database is open at the writer's commit");
            atoll(&db).atoll().transactions().hold_horizon(false);
            released.store(true, std::sync::atomic::Ordering::SeqCst);
        });
    }
    {
        let (db, released) = (Arc::downgrade(&db), released.clone());
        crate::race::at("txn.exclusive_covering", move || {
            let db = db.upgrade().expect("the database is open at the take");
            atoll(&db).atoll().transactions().hold_horizon(false);
            released.store(true, std::sync::atomic::Ordering::SeqCst);
        });
    }
    {
        let (db, released) = (Arc::downgrade(&db), released.clone());
        crate::race::at("statement.schema_cover_wait", move || {
            let db = db.upgrade().expect("the database is open at the wait");
            atoll(&db).atoll().transactions().hold_horizon(false);
            released.store(true, std::sync::atomic::Ordering::SeqCst);
        });
    }
    crate::statement::REPREPARES.with(|n| n.set(0));
    let mut out = Err("never ran".to_string());
    for _ in 0..100_000 {
        out = conn.execute(sql).map_err(|e| e.to_string());
        if !matches!(&out, Err(e) if e.to_lowercase().contains("busy")) {
            break;
        }
        std::thread::yield_now();
    }
    if !released.load(std::sync::atomic::Ordering::SeqCst) {
        txns.hold_horizon(false);
    }
    let reprepares = crate::statement::REPREPARES.with(|n| n.get());
    eprintln!("HG {sql}: the writer's statement came to {out:?} after {reprepares} reprepares");
    LAST_REPREPARES.with(|n| n.set(reprepares));
    assert_eq!(dropper.join().unwrap(), Ok(()), "the drop");
    eprintln!("HG {sql}: the drop joined");
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let check = reader
        .prepare("PRAGMA integrity_check")
        .and_then(|mut s| s.run_collect_rows());
    eprintln!("HG {sql}: the check after came to {check:?}");
    (out, check.unwrap_or_default())
}

#[test]
fn a_writer_below_a_committed_drop_dropping_the_index_again() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_below_a_committed_drop_dropping_the_index_again"
        ),
        || {
            let (out, check) =
                a_writer_below_a_committed_drop("/db/hg-drop-again", "DROP INDEX t_v");
            assert_eq!(
                (out, check),
                (Err("No such index: t_v".to_string()), ok()),
                "(a second DROP INDEX below the drop's commit, the check after)"
            );
        },
    );
}

#[test]
fn a_writer_below_a_committed_drop_dropping_its_table() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_below_a_committed_drop_dropping_its_table"
        ),
        || {
            let (out, check) = a_writer_below_a_committed_drop("/db/hg-drop-table", "DROP TABLE t");
            assert_eq!(
                (out, check),
                (Ok(()), ok()),
                "(DROP TABLE of the index's table below the drop's commit, the check after)"
            );
        },
    );
}

#[test]
fn a_writer_below_a_committed_drop_inserting_into_its_table() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_writer_below_a_committed_drop_inserting_into_its_table"
        ),
        || {
            let (out, check) = a_writer_below_a_committed_drop(
                "/db/hg-insert",
                "INSERT INTO t(v) VALUES ('below the drop')",
            );
            assert_eq!(
                (out, check, LAST_REPREPARES.with(|n| n.get())),
                (Ok(()), ok(), 1),
                "(an INSERT below the drop's commit, the check after, its reprepares)"
            );
        },
    );
}

/// GATE1 (#6549): a reader begun below a published DROP INDEX, its snapshot below the drop's
/// commit, waits once for the horizon to cover the commit that published the schema it holds, and
/// reads after one reprepare.
#[test]
fn a_reader_below_a_committed_drop_reads_after_one_reprepare() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_reader_below_a_committed_drop_reads_after_one_reprepare"
        ),
        || {
            let (out, check) =
                a_writer_below_a_committed_drop("/db/hg-select", "SELECT count(*) FROM t");
            assert_eq!(
                (out, check, LAST_REPREPARES.with(|n| n.get())),
                (Ok(()), ok(), 1),
                "(a SELECT below the drop's commit, the check after, its reprepares)"
            );
        },
    );
}

/// The keys on the Dropped list, read raw.
fn dropped_keys(db: &Database) -> Vec<Vec<u8>> {
    let adb = atoll(db);
    let list = adb.dropped().tree();
    let mut out = Vec::new();
    let mut cursor = crate::storage::atoll::btree::Cursor::new();
    let mut from: Vec<u8> = Vec::new();
    loop {
        let found = drive(adb.io(), || {
            list.seek(&mut cursor, &from, crate::storage::atoll::btree::Seek::Gt)
        })
        .unwrap();
        if !found {
            return out;
        }
        from = cursor.key().unwrap().to_vec();
        out.push(from.clone());
    }
}

/// The CREATE INDEX of a name another connection dropped, with nothing held back: does the
/// connection's own schema let it through once the drop has committed?
#[test]
fn a_create_index_of_a_name_another_connection_dropped_goes() {
    let (db, conn) = used("/db/create-after-drop");
    let other = db.connect(ConnectOpts::new()).unwrap();
    other.execute("DROP INDEX t_v").unwrap();
    let out = conn
        .execute("CREATE INDEX t_v ON t(substr(v, 1, 8))")
        .map_err(|e| e.to_string());
    assert_eq!(
        out,
        Ok(()),
        "CREATE INDEX of the dropped name, after the drop committed"
    );
}

/// The capture's road with the gate on and no hook (#6504 item 2 (b)): a check inside an explicit
/// transaction whose snapshot was taken by a read before another connection's statement put a
/// tree on the Dropped list and committed. Wanted: SchemaConflict, or "ok".
fn a_check_inside_an_old_read_after(path: &str, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    let (db, conn) = used(path);
    let other = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(rows(&conn, "SELECT count(*) FROM t").len(), 1);
    other.execute(sql).unwrap();
    let checked = conn
        .prepare("PRAGMA integrity_check")
        .and_then(|mut s| s.run_collect_rows())
        .map_err(|e| e.to_string());
    let _ = conn.execute("ROLLBACK");
    eprintln!("OLD-READ {sql}: {checked:?}");
    checked
}

fn no_finding(checked: &Result<Vec<Vec<Value>>, String>) -> bool {
    match checked {
        Ok(rows) => *rows == ok(),
        Err(e) => e.contains("conflict") || e.contains("Conflict") || e.contains("schema"),
    }
}

#[test]
fn a_check_inside_an_old_read_after_a_drop_index() {
    let checked = a_check_inside_an_old_read_after("/db/old-read-drop-index", "DROP INDEX t_v");
    assert!(no_finding(&checked), "{checked:?}");
}

#[test]
fn a_check_inside_an_old_read_after_a_drop_table() {
    let checked = a_check_inside_an_old_read_after("/db/old-read-drop-table", "DROP TABLE t");
    assert!(no_finding(&checked), "{checked:?}");
}

#[test]
fn a_check_inside_an_old_read_after_a_delete_of_every_row() {
    let checked = a_check_inside_an_old_read_after("/db/old-read-delete", "DELETE FROM t");
    assert!(no_finding(&checked), "{checked:?}");
}

/// The Clear: a DELETE of every row of a table with no index replaces its tree under the same
/// name, its old root on the Dropped list.
#[test]
fn a_check_inside_an_old_read_after_a_clear() {
    let (db, conn) = used("/db/old-read-clear");
    conn.execute("CREATE TABLE plain(x)").unwrap();
    conn.execute("INSERT INTO plain SELECT value FROM generate_series(1, 50)")
        .unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(rows(&conn, "SELECT count(*) FROM plain").len(), 1);
    other.execute("DELETE FROM plain").unwrap();
    let checked = conn
        .prepare("PRAGMA integrity_check")
        .and_then(|mut s| s.run_collect_rows())
        .map_err(|e| e.to_string());
    let _ = conn.execute("ROLLBACK");
    eprintln!("OLD-READ clear: {checked:?}");
    assert!(no_finding(&checked), "{checked:?}");
}

#[test]
fn a_check_inside_an_old_read_after_a_reindex() {
    let checked = a_check_inside_an_old_read_after("/db/old-read-reindex", "REINDEX t_v");
    assert!(no_finding(&checked), "{checked:?}");
}

/// GATE-LEAK1 (#6495-#6498): a statement that meets a schema-cookie mismatch at each of its 51
/// attempts gives up with "Database schema changed". Its connection, back in autocommit and idle,
/// must hold nothing: another connection's INSERT goes. The mismatches are forced on this thread
/// (the only road found in one process is a snapshot held behind the horizon).
fn a_statement_that_gives_up_on_the_schema_leaves_the_gate_free(path: &str, sql: &str) {
    let (db, conn) = used(path);
    conn.execute("CREATE TABLE u(x)").unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    crate::vdbe::execute::FORCED_SCHEMA_MISMATCHES.with(|n| n.set(51));
    let out = conn.execute(sql).map_err(|e| e.to_string());
    crate::vdbe::execute::FORCED_SCHEMA_MISMATCHES.with(|n| n.set(0));
    let insert = other
        .execute("INSERT INTO t(v) VALUES ('after the give-up')")
        .map_err(|e| e.to_string());
    assert_eq!(
        (out, insert),
        (Err("Database schema changed".to_string()), Ok(())),
        "({sql} forced through every retry, another connection's INSERT after it)"
    );
}

#[test]
fn a_drop_table_that_gives_up_on_the_schema_leaves_the_gate_free() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_drop_table_that_gives_up_on_the_schema_leaves_the_gate_free"
        ),
        || {
            a_statement_that_gives_up_on_the_schema_leaves_the_gate_free(
                "/db/gl-drop-table",
                "DROP TABLE u",
            );
        },
    );
}

#[test]
fn a_drop_index_that_gives_up_on_the_schema_leaves_the_gate_free() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_drop_index_that_gives_up_on_the_schema_leaves_the_gate_free"
        ),
        || {
            a_statement_that_gives_up_on_the_schema_leaves_the_gate_free(
                "/db/gl-drop-index",
                "DROP INDEX t_v",
            );
        },
    );
}

#[test]
fn a_create_table_that_gives_up_on_the_schema_leaves_the_gate_free() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_create_table_that_gives_up_on_the_schema_leaves_the_gate_free"
        ),
        || {
            a_statement_that_gives_up_on_the_schema_leaves_the_gate_free(
                "/db/gl-create-table",
                "CREATE TABLE w(x)",
            );
        },
    );
}

#[test]
fn a_create_index_that_gives_up_on_the_schema_leaves_the_gate_free() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_create_index_that_gives_up_on_the_schema_leaves_the_gate_free"
        ),
        || {
            a_statement_that_gives_up_on_the_schema_leaves_the_gate_free(
                "/db/gl-create-index",
                "CREATE INDEX t_id ON t(id)",
            );
        },
    );
}

#[test]
fn an_alter_table_that_gives_up_on_the_schema_leaves_the_gate_free() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::an_alter_table_that_gives_up_on_the_schema_leaves_the_gate_free"
        ),
        || {
            a_statement_that_gives_up_on_the_schema_leaves_the_gate_free(
                "/db/gl-alter",
                "ALTER TABLE t ADD COLUMN extra",
            );
        },
    );
}

#[test]
fn an_integrity_check_that_gives_up_on_the_schema_leaves_the_gate_free() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::an_integrity_check_that_gives_up_on_the_schema_leaves_the_gate_free"
        ),
        || {
            a_statement_that_gives_up_on_the_schema_leaves_the_gate_free(
                "/db/gl-check",
                "PRAGMA integrity_check",
            );
        },
    );
}

#[test]
fn an_insert_that_gives_up_on_the_schema_leaves_the_gate_free() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::an_insert_that_gives_up_on_the_schema_leaves_the_gate_free"
        ),
        || {
            a_statement_that_gives_up_on_the_schema_leaves_the_gate_free(
                "/db/gl-insert",
                "INSERT INTO t(v) VALUES ('given up')",
            );
        },
    );
}

/// DROPPED-LIST1, #6434 items 2 and 3: an autocommit DROP INDEX stepped in frames and abandoned
/// (its statement dropped) at its k-th return for IO, for every k it reaches, leaving its
/// statement's rollback to be settled; then at once a check from another connection with no busy
/// timeout. Busy or "ok" each time; any finding is the cause. The pool is chilled once, before
/// the drop's first step, so each stop is one the drop meets on its own.
#[test]
fn a_drop_abandoned_at_each_of_its_stops_leaves_nothing_a_check_finds() {
    let mut outcomes = Vec::new();
    for k in 1..=2_000usize {
        let (io, db, conn) = used_with_worker(&format!("/db/drop-abandoned-{k}"));
        let other = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA busy_timeout = 0").unwrap();
        let mut stmt = other.prepare("DROP INDEX t_v").unwrap();
        let mut stops = 0;
        let mut finished = false;
        chill(&db);
        loop {
            let stepped = {
                let _frame = crate::storage::atoll::cooperative::Frame::enter();
                stmt.step()
            };
            match stepped {
                Ok(crate::StepResult::IO) => {
                    stops += 1;
                    if stops == k {
                        break;
                    }
                    if let Some(c) = stmt.pending_io_for_test() {
                        io.wait(&c);
                    }
                }
                Ok(crate::StepResult::Done) | Err(_) => {
                    finished = true;
                    break;
                }
                Ok(crate::StepResult::Yield) => {}
                Ok(other) => panic!("the drop stepped to {other:?}"),
            }
        }
        if finished {
            outcomes.push(format!("k={k}: the drop finished at {stops} stops"));
            break;
        }
        drop(stmt);
        let checked = conn
            .prepare("PRAGMA integrity_check")
            .and_then(|mut s| s.run_collect_rows())
            .map_err(|e| e.to_string());
        let after = format!("{:?}", integrity(&other));
        let line = format!("k={k}: check {checked:?}; other's check after {after}");
        let bad = |t: &str| t.contains("named by the catalog");
        outcomes.push(line.clone());
        assert!(
            !bad(&format!("{checked:?}")) && !bad(&after),
            "an abandoned drop left an entry a check finds: {line}"
        );
    }
    eprintln!(
        "the sweep: {} abandonments, then {}",
        outcomes.len() - 1,
        outcomes.last().map_or("nothing", |o| o.as_str())
    );
    assert!(
        outcomes.last().is_some_and(|o| o.contains("finished")),
        "the drop never finished within the bound: {outcomes:?}"
    );
}
