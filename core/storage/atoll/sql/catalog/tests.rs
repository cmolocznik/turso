use super::*;
use crate::io::crash::CrashIO;
use crate::io::MemoryIO;
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET, WRITEBACK_BYTES};
use crate::storage::atoll::db::AtollConfig;
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::page::CLASS_COUNT;
use crate::storage::atoll::sql::AtollDb;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::{Commit, Isolation, TxnConfig};
use std::collections::BTreeSet;

const BASE: &str = "/db/catalog";

/// A small database's configuration, for every test of the SQL layer's trees.
pub(crate) fn config() -> AtollConfig {
    AtollConfig {
        log: LogConfig {
            segment_bytes: 1 << 20,
            buffer_bytes: 256 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            direct: true,
        },
        store: StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        buffer: BufferConfig {
            budget: MIN_BUDGET,
            writeback_bytes: WRITEBACK_BYTES,
            kept_bytes: None,
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        txn: TxnConfig { slots: 4 },
    }
}

/// Every page the store has allocated, of every class.
fn allocated(db: &AtollDb, io: &Arc<dyn IO>) -> BTreeSet<PageId> {
    let mut out = BTreeSet::new();
    for c in 0..CLASS_COUNT {
        let class = SizeClass::new(c).unwrap();
        for number in 0..crate::storage::atoll::testing::page_numbers(db.atoll().store(), class) {
            let pid = PageId::new(class, number);
            if drive(io, || db.atoll().store().entry(pid)).unwrap() != FREE {
                out.insert(pid);
            }
        }
    }
    out
}

/// Tree `id`'s row as a transaction begun now reads it.
fn row(db: &AtollDb, io: &Arc<dyn IO>, id: u64) -> Option<Entry> {
    let txns = db.atoll().transactions();
    let mut txn = drive(io, || txns.begin(Isolation::Snapshot)).unwrap();
    txns.statement(&mut txn).unwrap();
    let mut get = Get::new(&key(id));
    let entry = drive(io, || {
        db.catalog().get(txns, db.versions(), &mut txn, &mut get)
    })
    .unwrap();
    txns.abort(txn);
    entry
}

fn system_roots() -> BTreeSet<PageId> {
    System::ALL.iter().map(|s| s.root()).collect()
}

/// What every open of a database must leave: each system tree at its fixed root and nothing else
/// allocated, `sqlite_schema`'s row naming its root, and the next tree id past it.
fn check_created(db: &AtollDb, io: &Arc<dyn IO>, label: &str) {
    assert_eq!(
        allocated(db, io),
        system_roots(),
        "{label}: pages allocated"
    );
    assert_eq!(
        row(db, io, SCHEMA_ID),
        Some(Entry {
            root: System::Schema.root(),
            layout: Layout::Table,
            encoding: 0,
            created_by: 0,
        }),
        "{label}: sqlite_schema's row"
    );
    assert_eq!(
        db.draw_tree_id(),
        SCHEMA_ID + 1,
        "{label}: the next tree id"
    );
}

#[test]
fn a_new_database_has_its_system_trees_at_their_roots_and_reopening_makes_none() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    check_created(&db, &io, "new");
    drop(db);
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    check_created(&db, &io, "reopened");
}

/// A creation crashed at any point, before or after its superblock, between its system trees or
/// before `sqlite_schema`'s row is durable, completes on the next open at the same pages, leaking
/// none.
#[test]
fn a_creation_a_crash_cut_short_completes_at_the_same_pages() {
    for seed in 0..3u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        crash_io.set_durable_writes(seed % 2 == 0);
        let io: Arc<dyn IO> = crash_io.clone();
        crash_io.capture(|_| true, 1, seed);
        drop(AtollDb::open(io, BASE, config()).unwrap());
        let mut images = crash_io.captured();
        images.push(crash_io.crash(seed));
        let (mut fresh, mut partial) = (0, 0);
        for (i, image) in images.into_iter().enumerate() {
            let image: Arc<dyn IO> = Arc::new(image);
            let db = AtollDb::open(image.clone(), BASE, config()).unwrap();
            if db.atoll().recovered_end().is_some() {
                partial += 1;
            } else {
                fresh += 1;
            }
            check_created(&db, &image, &format!("seed {seed}, image {i}"));
        }
        // The crashes fell on both sides of the creation's superblock: some images held none,
        // and their open created the database; others held a database begun and not finished,
        // and their open recovered it.
        assert!(fresh > 0, "seed {seed}: no image before the superblock");
        assert!(partial > 0, "seed {seed}: no image recovered");
    }
}

/// Catalog rows are versioned: a snapshot reads the row it saw, a rolled-back change leaves none,
/// and the next id is drawn above every id a committed row holds.
#[test]
fn catalog_rows_are_versioned_and_ids_are_drawn_above_them() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    let txns = db.atoll().transactions();
    let entry = |n: u64, layout| Entry {
        root: PageId::new(System::class(), 100 + n),
        layout,
        encoding: n as u32,
        created_by: 0,
    };
    let write = |txn: &mut Txn, id: u64, e: Option<Entry>| {
        let mut op = match e {
            Some(e) => Catalog::put_op(id, &e, Mode::Upsert),
            None => Catalog::delete_op(id),
        };
        let w = drive(&io, || {
            db.catalog().write(txns, db.versions(), txn, &mut op)
        })
        .unwrap();
        assert!(matches!(w, Write::Done(_)), "{w:?}");
    };
    let begin = || {
        let mut t = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
        crate::storage::atoll::testing::share(txns, &mut t);
        txns.statement(&mut t).unwrap();
        t
    };
    let commit = |t: Txn| {
        let mut op = Commit::new(t);
        drive(&io, || txns.commit(&mut op)).unwrap()
    };
    let id = db.draw_tree_id();
    let mut a = begin();
    write(&mut a, id, Some(entry(1, Layout::Index)));
    commit(a);
    let mut before = begin();
    let mut b = begin();
    write(&mut b, id, Some(entry(2, Layout::Table)));
    let rolled = db.draw_tree_id();
    write(&mut b, rolled, Some(entry(3, Layout::Table)));
    let mut rollback = crate::storage::atoll::txn::Abort::new(b);
    drive(&io, || txns.rollback(db.versions(), &mut rollback)).unwrap();
    assert_eq!(row(&db, &io, id), Some(entry(1, Layout::Index)));
    assert_eq!(row(&db, &io, rolled), None, "a rolled-back row");
    let mut c = begin();
    write(&mut c, id, None);
    commit(c);
    let mut get = Get::new(&key(id));
    let seen = drive(&io, || {
        db.catalog().get(txns, db.versions(), &mut before, &mut get)
    })
    .unwrap();
    assert_eq!(
        seen,
        Some(entry(1, Layout::Index)),
        "an older snapshot's row"
    );
    txns.abort(before);
    assert_eq!(row(&db, &io, id), None, "a deleted row");
    assert!(db.draw_tree_id() > rolled, "an id drawn again while open");
    drop(db);
    // After a restart the next id is above every id a row holds. Replay removes a deleted row
    // outright, there being no snapshot left to read it, so the deleted row's id, like the
    // rolled-back one's, is named by nothing durable and is drawn again.
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    assert_eq!(db.draw_tree_id(), id);
}
