use super::*;
use crate::io::MemoryIO;
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::sql::AtollDb;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::{Commit, Isolation};

const BASE: &str = "/db/header";

fn begin(db: &AtollDb, io: &Arc<dyn IO>) -> Txn {
    let txns = db.atoll().transactions();
    let mut t = drive(io, || txns.begin(Isolation::Snapshot)).unwrap();
    crate::storage::atoll::testing::share(txns, &mut t);
    txns.statement(&mut t).unwrap();
    t
}

fn read(db: &AtollDb, io: &Arc<dyn IO>, txn: &mut Txn, field: Field) -> u32 {
    let mut get = Header::get_op(field);
    drive(io, || {
        db.header().get(
            db.atoll().transactions(),
            db.versions(),
            txn,
            field,
            &mut get,
        )
    })
    .unwrap()
}

fn set(db: &AtollDb, io: &Arc<dyn IO>, txn: &mut Txn, field: Field, value: u32) -> Write {
    let mut op = Header::put_op(field, value);
    drive(io, || {
        db.header()
            .write(db.atoll().transactions(), db.versions(), txn, &mut op)
    })
    .unwrap()
}

fn commit(db: &AtollDb, io: &Arc<dyn IO>, txn: Txn) -> Ts {
    let mut op = Commit::new(txn);
    drive(io, || db.atoll().transactions().commit(&mut op))
        .unwrap()
        .expect("a commit that wrote")
}

/// Fields read their defaults until written; a write is versioned like any row, two transactions
/// setting different fields at once never meet, and two setting one field conflict.
#[test]
fn fields_default_until_written_are_versioned_and_conflict_only_on_one_field() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    let mut t = begin(&db, &io);
    for f in Field::ALL {
        assert_eq!(read(&db, &io, &mut t, f), f.default(), "{f:?}");
    }
    assert_eq!(read(&db, &io, &mut t, Field::SchemaFormat), 4);
    assert_eq!(read(&db, &io, &mut t, Field::TextEncoding), 1);
    let mut before = begin(&db, &io);
    assert!(matches!(
        set(&db, &io, &mut t, Field::UserVersion, 7),
        Write::Done(_)
    ));
    assert_eq!(
        read(&db, &io, &mut t, Field::UserVersion),
        7,
        "its own write"
    );
    commit(&db, &io, t);
    assert_eq!(read(&db, &io, &mut before, Field::UserVersion), 0);
    let mut later = begin(&db, &io);
    assert_eq!(read(&db, &io, &mut later, Field::UserVersion), 7);
    // Different fields at once: neither waits.
    let (mut a, mut b) = (begin(&db, &io), begin(&db, &io));
    assert!(matches!(
        set(&db, &io, &mut a, Field::ApplicationId, 11),
        Write::Done(_)
    ));
    assert!(matches!(
        set(&db, &io, &mut b, Field::SchemaCookie, 3),
        Write::Done(_)
    ));
    commit(&db, &io, a);
    commit(&db, &io, b);
    // One field at once: the second waits on the first, then conflicts with its commit.
    let (mut c, mut d) = (begin(&db, &io), begin(&db, &io));
    assert!(matches!(
        set(&db, &io, &mut c, Field::UserVersion, 8),
        Write::Done(_)
    ));
    assert!(matches!(
        set(&db, &io, &mut d, Field::UserVersion, 9),
        Write::Wait(_)
    ));
    commit(&db, &io, c);
    assert_eq!(
        set(&db, &io, &mut d, Field::UserVersion, 9),
        Write::Conflict
    );
    let txns = db.atoll().transactions();
    let mut abort = crate::storage::atoll::txn::Abort::new(d);
    drive(&io, || txns.rollback(db.versions(), &mut abort)).unwrap();
    for t in [before, later] {
        txns.abort(t);
    }
}

/// The committed header takes, per field, the value of the newest commit that wrote it, whatever
/// order commits publish in, and a restart reads it back from the rows.
#[test]
fn the_committed_header_takes_each_fields_newest_commit_and_survives_a_restart() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    let h = db.header();
    // Above every timestamp the open published at, whatever transactions it ran.
    let base = db.atoll().transactions().clock().now();
    h.publish(Field::UserVersion, base + 5, 50);
    h.publish(Field::UserVersion, base + 3, 30);
    h.publish(Field::ApplicationId, base + 4, 40);
    assert_eq!(
        h.committed(Field::UserVersion),
        50,
        "an older commit published later"
    );
    assert_eq!(h.committed(Field::ApplicationId), 40);
    assert_eq!(h.committed(Field::SchemaFormat), 4);
    let mut t = begin(&db, &io);
    set(&db, &io, &mut t, Field::UserVersion, 77);
    set(&db, &io, &mut t, Field::SchemaCookie, 2);
    commit(&db, &io, t);
    drop(db);
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    assert_eq!(db.header().committed(Field::UserVersion), 77);
    assert_eq!(db.header().committed(Field::SchemaCookie), 2);
    assert_eq!(db.header().committed(Field::ApplicationId), 0);
}
