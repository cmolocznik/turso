//! AtollCursor's reads against a model ordered by Turso's own comparison: walks both ways, and
//! every seek op from keys present and absent, on a rowid table, an index and a UNIQUE index whose
//! keys leave the rowid out.

use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::Mode;
use crate::storage::atoll::page::SizeClass;
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::versioned::{Probe, Write, WriteOp};
use crate::storage::atoll::txn::{Commit, Isolation};
use crate::storage::btree::BTreeKey;
use crate::translate::collate::CollationSeq;
use crate::types::{cmp_in_column, AsValueRef, KeyInfo};
use crate::Value;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_parser::ast::SortOrder;

const BASE: &str = "/db/cursor";
use super::key::ENCODING;

fn run<T>(io: &Arc<dyn IO>, op: impl FnMut() -> Result<IOResult<T>>) -> T {
    drive(io, op).unwrap()
}

struct Db {
    io: Arc<dyn IO>,
    db: Arc<AtollDb>,
}

impl Db {
    fn open() -> Self {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Arc::new(AtollDb::open(io.clone(), BASE, config()).unwrap());
        Self { io, db }
    }

    fn begin(&self) -> TxnCell {
        let txns = self.db.atoll().transactions();
        let mut t = run(&self.io, || txns.begin(Isolation::Snapshot));
        crate::storage::atoll::testing::share(txns, &mut t);
        txns.statement(&mut t).unwrap();
        TxnSlot::new(Some(t))
    }

    fn commit(&self, cell: &TxnCell) {
        let mut op = Commit::new(cell.lock().take().unwrap());
        run(&self.io, || self.db.atoll().transactions().commit(&mut op));
    }

    /// A tree holding `rows`, key and record, written and committed; its catalog row.
    fn tree(&self, layout: Layout, rows: &[(Vec<u8>, Vec<Value>)]) -> Entry {
        let cell = self.begin();
        let root = {
            let mut guard = cell.lock();
            let txn = guard.as_mut().unwrap();
            let root = run(&self.io, || {
                self.db.create_tree(txn, SizeClass::new(0).unwrap())
            });
            let a = self.db.atoll();
            let tree =
                VersionedTree::open(self.io.clone(), a.pool().clone(), a.store().clone(), root);
            for (key, values) in rows {
                let record = ImmutableRecord::from_values(values, values.len()).unwrap();
                let mut op = WriteOp::put(key, record.get_payload().to_vec(), Mode::Insert);
                let w = run(&self.io, || {
                    tree.write(a.transactions(), self.db.versions(), txn, &mut op)
                });
                assert!(matches!(w, Write::Done(_)), "{w:?}");
            }
            root
        };
        self.commit(&cell);
        Entry {
            root,
            layout,
            encoding: ENCODING,
            created_by: 0,
        }
    }

    fn cursor(&self, entry: Entry, index: Option<IndexInfo>) -> AtollCursor {
        AtollCursor::new(
            self.db.clone(),
            self.begin(),
            7,
            entry,
            index.map(Arc::new),
            ENCODING,
        )
    }
}

fn values_of(c: &mut AtollCursor, _io: &Arc<dyn IO>) -> Vec<Value> {
    // A record is read from the row the cursor holds, with no IO.
    match c.record().unwrap() {
        IOResult::Done(record) => record.expect("a row").get_values_owned().unwrap(),
        IOResult::IO(_) => unreachable!("a record read with IO"),
    }
}

/// Every row, walked forward from the first or backward from the last.
fn walk(c: &mut AtollCursor, io: &Arc<dyn IO>, forward: bool) -> Vec<Vec<Value>> {
    let mut out = Vec::new();
    if forward {
        run(io, || c.rewind());
    } else {
        run(io, || c.last());
    }
    while !c.is_empty() {
        out.push(values_of(c, io));
        if forward {
            run(io, || c.next());
        } else {
            run(io, || c.prev());
        }
    }
    out
}

const OPS: [SeekOp; 6] = [
    SeekOp::GE { eq_only: true },
    SeekOp::GE { eq_only: false },
    SeekOp::GT,
    SeekOp::LE { eq_only: true },
    SeekOp::LE { eq_only: false },
    SeekOp::LT,
];

/// What `op` finds from a key among `rows`, in order, given each row's comparison with the key:
/// the row, or `None`, and whether it is found.
fn expected(cmps: &[Ordering], op: SeekOp) -> (Option<usize>, bool) {
    let pick = match op {
        SeekOp::GE { .. } => cmps.iter().position(|c| c.is_ge()),
        SeekOp::GT => cmps.iter().position(|c| c.is_gt()),
        SeekOp::LE { .. } => cmps.iter().rposition(|c| c.is_le()),
        SeekOp::LT => cmps.iter().rposition(|c| c.is_lt()),
    };
    let found = match op.eq_only() {
        true => pick.is_some_and(|i| cmps[i].is_eq()),
        false => pick.is_some(),
    };
    (pick, found)
}

/// Checks one seek against the model: found as it says, and on the row it names when found.
fn check_seek(
    c: &mut AtollCursor,
    io: &Arc<dyn IO>,
    key: SeekKey<'_>,
    op: SeekOp,
    rows: &[Vec<Value>],
    cmps: &[Ordering],
    label: &str,
) {
    let got = run(io, || c.seek(key.clone(), op));
    let (pick, found) = expected(cmps, op);
    assert_eq!(got == SeekResult::Found, found, "{label} {op:?}: {got:?}");
    if found {
        assert_eq!(&values_of(c, io), &rows[pick.unwrap()], "{label} {op:?}");
    }
}

/// A rowid table walks both ways in rowid order, and every seek op finds what the model finds,
/// from rowids present, absent, and at both ends.
#[test]
fn a_table_cursor_walks_and_seeks_rowids_as_the_model_does() {
    let db = Db::open();
    let rowids = [i64::MIN + 1, -5, 1, 2, 10, 100, i64::MAX];
    let rows: Vec<(Vec<u8>, Vec<Value>)> = rowids
        .iter()
        .map(|&r| {
            let values = vec![Value::from_i64(r), Value::build_text(format!("row {r}"))];
            (key::rowid(r).to_vec(), values)
        })
        .collect();
    let entry = db.tree(Layout::Table, &rows);
    let mut c = db.cursor(entry, None);
    let model: Vec<Vec<Value>> = rows.iter().map(|(_, v)| v.clone()).collect();
    assert_eq!(walk(&mut c, &db.io, true), model);
    let mut back = model.clone();
    back.reverse();
    assert_eq!(walk(&mut c, &db.io, false), back);
    run(&db.io, || c.rewind());
    assert_eq!(run(&db.io, || c.rowid()), Some(i64::MIN + 1));
    for probe in [
        i64::MIN,
        -6,
        -5,
        0,
        1,
        3,
        10,
        99,
        100,
        i64::MAX - 1,
        i64::MAX,
    ] {
        let cmps: Vec<Ordering> = rowids.iter().map(|r| r.cmp(&probe)).collect();
        for op in OPS {
            let label = format!("rowid {probe}");
            check_seek(
                &mut c,
                &db.io,
                SeekKey::TableRowId(probe),
                op,
                &model,
                &cmps,
                &label,
            );
            if run(&db.io, || c.seek(SeekKey::TableRowId(probe), op)) == SeekResult::Found {
                let r = run(&db.io, || c.rowid()).unwrap();
                assert_eq!(Value::from_i64(r), values_of(&mut c, &db.io)[0]);
            }
        }
    }
}

fn info(key_info: &[KeyInfo], unique: bool) -> IndexInfo {
    let mut info = IndexInfo::default();
    info.key_info.clear();
    info.key_info.extend(key_info.iter().cloned());
    info.num_cols = key_info.len();
    info.has_rowid = true;
    info.is_unique = unique;
    info
}

fn compare(a: &[Value], b: &[Value], key_info: &[KeyInfo]) -> Ordering {
    a.iter()
        .zip(b)
        .zip(key_info)
        .map(|((a, b), k)| cmp_in_column(&a.as_value_ref(), &b.as_value_ref(), k))
        .find(|o| o.is_ne())
        .unwrap_or(Ordering::Equal)
}

fn text_or_null(rng: &mut ChaCha8Rng) -> Value {
    match rng.random_range(0..8) {
        0 => Value::Null,
        n => Value::build_text(
            ["a", "A", "b", "B b", "c", "ab"][n % 6].to_string() + &"x".repeat(n / 6),
        ),
    }
}

/// An index on a NOCASE text column and a descending integer, with its rowid: it walks in the
/// order Turso compares its records, and seeks by one column, two, or the whole key find what the
/// model finds for every op.
#[test]
fn an_index_cursor_seeks_prefixes_in_turso_order() {
    let db = Db::open();
    let key_info = [
        KeyInfo {
            sort_order: SortOrder::Asc,
            collation: CollationSeq::NOCASE,
            nulls_order: None,
        },
        KeyInfo {
            sort_order: SortOrder::Desc,
            collation: CollationSeq::BINARY,
            nulls_order: None,
        },
        KeyInfo {
            sort_order: SortOrder::Asc,
            collation: CollationSeq::BINARY,
            nulls_order: None,
        },
    ];
    let mut rng = ChaCha8Rng::seed_from_u64(7);
    let mut model: Vec<Vec<Value>> = (0..300)
        .map(|rowid| {
            vec![
                text_or_null(&mut rng),
                Value::from_i64(rng.random_range(-3..4)),
                Value::from_i64(rowid),
            ]
        })
        .collect();
    model.sort_by(|a, b| compare(a, b, &key_info));
    let rows: Vec<(Vec<u8>, Vec<Value>)> = model
        .iter()
        .map(|v| {
            let mut k = Vec::new();
            key::encode(v.iter().map(|v| v.as_value_ref()), &key_info, &mut k).unwrap();
            (k, v.clone())
        })
        .collect();
    let entry = db.tree(Layout::Index, &rows);
    let mut c = db.cursor(entry, Some(info(&key_info, false)));
    assert_eq!(walk(&mut c, &db.io, true), model);
    for _ in 0..200 {
        let n = rng.random_range(1..=3);
        let mut probe = model[rng.random_range(0..model.len())][..n].to_vec();
        if rng.random_bool(0.3) {
            probe[0] = text_or_null(&mut rng);
        }
        let record = ImmutableRecord::from_values(&probe, n).unwrap();
        let cmps: Vec<Ordering> = model
            .iter()
            .map(|r| compare(&r[..n], &probe, &key_info))
            .collect();
        for op in OPS {
            let key = SeekKey::IndexKey(record.as_record_ref());
            let label = format!("{probe:?}");
            check_seek(&mut c, &db.io, key, op, &model, &cmps, &label);
        }
    }
}

/// A UNIQUE index whose keys leave the rowid out where no column is NULL, and keep it where one
/// is: seeks by the whole key, rowid included, find what the model finds for every op.
#[test]
fn a_unique_index_seeks_by_the_rowid_its_keys_leave_out() {
    let db = Db::open();
    let key_info = [
        KeyInfo {
            sort_order: SortOrder::Asc,
            collation: CollationSeq::BINARY,
            nulls_order: None,
        },
        KeyInfo {
            sort_order: SortOrder::Asc,
            collation: CollationSeq::BINARY,
            nulls_order: None,
        },
    ];
    let mut model: Vec<Vec<Value>> = (0..40)
        .map(|i| {
            let a = if i % 7 == 0 {
                Value::Null
            } else {
                Value::from_i64(i * 3)
            };
            vec![a, Value::from_i64(1000 - i)]
        })
        .collect();
    model.sort_by(|a, b| compare(a, b, &key_info));
    let rows: Vec<(Vec<u8>, Vec<Value>)> = model
        .iter()
        .map(|v| {
            let n = if v[0] == Value::Null { 2 } else { 1 };
            let mut k = Vec::new();
            key::encode(v[..n].iter().map(|v| v.as_value_ref()), &key_info, &mut k).unwrap();
            (k, v.clone())
        })
        .collect();
    let entry = db.tree(Layout::Index, &rows);
    let mut c = db.cursor(entry, Some(info(&key_info, true)));
    assert_eq!(walk(&mut c, &db.io, true), model);
    for a in [-1, 0, 3, 4, 21, 117, 200] {
        for rowid in [0, 990, 999, 1000 - a / 3, 2000] {
            let probe = vec![Value::from_i64(a), Value::from_i64(rowid)];
            let record = ImmutableRecord::from_values(&probe, 2).unwrap();
            let cmps: Vec<Ordering> = model
                .iter()
                .map(|r| compare(r, &probe, &key_info))
                .collect();
            for op in OPS {
                let key = SeekKey::IndexKey(record.as_record_ref());
                check_seek(
                    &mut c,
                    &db.io,
                    key,
                    op,
                    &model,
                    &cmps,
                    &format!("{probe:?}"),
                );
            }
        }
    }
}

/// A tree whose keys are of another encoding is scanned whole, and a seek into it is refused,
/// naming REINDEX.
#[test]
fn a_tree_of_another_encoding_is_scanned_and_never_sought() {
    let db = Db::open();
    let key_info = [KeyInfo {
        sort_order: SortOrder::Asc,
        collation: CollationSeq::BINARY,
        nulls_order: None,
    }];
    let rows: Vec<(Vec<u8>, Vec<Value>)> = (0..5)
        .map(|i| {
            let v = vec![Value::from_i64(i)];
            let mut k = Vec::new();
            key::encode(v.iter().map(|v| v.as_value_ref()), &key_info, &mut k).unwrap();
            (k, v)
        })
        .collect();
    let mut entry = db.tree(Layout::Index, &rows);
    entry.encoding = ENCODING + 1;
    let mut c = db.cursor(entry, Some(info(&key_info, false)));
    assert_eq!(walk(&mut c, &db.io, true).len(), 5);
    let probe = [Value::from_i64(2)];
    let record = ImmutableRecord::from_values(&probe, 1).unwrap();
    let err = drive(&db.io, || {
        c.seek(SeekKey::IndexKey(record.as_record_ref()), SeekOp::GT)
    })
    .expect_err("a seek refused");
    assert!(err.to_string().contains("REINDEX"), "{err}");
}

/// A cursor sees what its transaction sees: another open transaction's row is not there until it
/// commits and a later transaction reads, while a cursor in the writer's transaction sees it at
/// once.
#[test]
fn a_cursor_sees_its_own_transactions_rows_and_no_open_ones() {
    let db = Db::open();
    let entry = db.tree(
        Layout::Table,
        &[(key::rowid(1).to_vec(), vec![Value::from_i64(1)])],
    );
    let writer = db.begin();
    {
        let mut guard = writer.lock();
        let txn = guard.as_mut().unwrap();
        let a = db.db.atoll();
        let tree = VersionedTree::open(
            db.io.clone(),
            a.pool().clone(),
            a.store().clone(),
            entry.root,
        );
        let record = ImmutableRecord::from_values(&[Value::from_i64(2)], 1).unwrap();
        let mut op = WriteOp::put(&key::rowid(2), record.get_payload().to_vec(), Mode::Insert);
        run(&db.io, || {
            tree.write(a.transactions(), db.db.versions(), txn, &mut op)
        });
    }
    let mut other = db.cursor(entry, None);
    assert_eq!(walk(&mut other, &db.io, true).len(), 1);
    let mut own = AtollCursor::new(db.db.clone(), writer.clone(), 7, entry, None, ENCODING);
    assert_eq!(walk(&mut own, &db.io, true).len(), 2);
    db.commit(&writer);
    assert_eq!(
        walk(&mut other, &db.io, true).len(),
        1,
        "its snapshot is older"
    );
    assert_eq!(walk(&mut db.cursor(entry, None), &db.io, true).len(), 2);
}

fn table_key<'a>(rowid: i64, record: &'a ImmutableRecord) -> BTreeKey<'a> {
    BTreeKey::new_table_rowid(rowid, Some(record))
}

fn record_of(values: &[Value]) -> ImmutableRecord {
    ImmutableRecord::from_values(values, values.len()).unwrap()
}

impl Db {
    fn begin_rc(&self) -> TxnCell {
        let txns = self.db.atoll().transactions();
        let mut t = run(&self.io, || txns.begin(Isolation::ReadCommitted));
        crate::storage::atoll::testing::share(txns, &mut t);
        txns.statement(&mut t).unwrap();
        TxnSlot::new(Some(t))
    }

    fn empty(&self, layout: Layout) -> Entry {
        self.tree(layout, &[])
    }

    fn on(&self, cell: &TxnCell, entry: Entry, index: Option<IndexInfo>) -> AtollCursor {
        AtollCursor::new(
            self.db.clone(),
            cell.clone(),
            7,
            entry,
            index.map(Arc::new),
            ENCODING,
        )
    }

    fn entries(&self, root: crate::storage::atoll::page::PageId) -> usize {
        let a = self.db.atoll();
        let tree = VersionedTree::open(self.io.clone(), a.pool().clone(), a.store().clone(), root);
        let mut check = crate::storage::atoll::btree::Check::default();
        run(&self.io, || tree.tree().check(&mut check)).entries
    }
}

fn written(c: &mut AtollCursor, io: &Arc<dyn IO>, key: &BTreeKey<'_>) -> Write {
    run(io, || c.insert(key))
}

/// Rows a cursor inserts, overwrites and deletes are its transaction's own until it commits:
/// another transaction sees none of them, and one begun after the commit sees them all.
#[test]
fn writes_through_a_cursor_are_its_transactions_until_committed() {
    let db = Db::open();
    let entry = db.empty(Layout::Table);
    let cell = db.begin();
    let mut c = db.on(&cell, entry, None);
    for r in 1..=5 {
        let record = record_of(&[Value::from_i64(r)]);
        assert!(matches!(
            written(&mut c, &db.io, &table_key(r, &record)),
            Write::Done(_)
        ));
    }
    let record = record_of(&[Value::from_i64(30)]);
    written(&mut c, &db.io, &table_key(3, &record));
    assert_eq!(
        run(&db.io, || c
            .seek(SeekKey::TableRowId(2), SeekOp::GE { eq_only: true })),
        SeekResult::Found
    );
    assert_eq!(run(&db.io, || c.delete()), Write::Done(Outcome::Updated));
    let mine: Vec<Vec<Value>> = walk(&mut c, &db.io, true);
    let expected: Vec<Vec<Value>> = [1, 30, 4, 5]
        .iter()
        .map(|&v| vec![Value::from_i64(v)])
        .collect();
    assert_eq!(mine, expected, "its own writes");
    assert!(
        walk(&mut db.cursor(entry, None), &db.io, true).is_empty(),
        "another's view"
    );
    db.commit(&cell);
    assert_eq!(walk(&mut db.cursor(entry, None), &db.io, true), expected);
}

/// A UNIQUE index refuses a second row of a key with no NULL, whose keys leave the rowid out, but
/// takes any number of rows whose key holds a NULL, which keep it.
#[test]
fn a_unique_index_refuses_a_second_row_of_a_key_but_not_of_a_null() {
    let db = Db::open();
    let key_info = [
        KeyInfo {
            sort_order: SortOrder::Asc,
            collation: CollationSeq::BINARY,
            nulls_order: None,
        },
        KeyInfo {
            sort_order: SortOrder::Asc,
            collation: CollationSeq::BINARY,
            nulls_order: None,
        },
    ];
    let entry = db.empty(Layout::Index);
    let cell = db.begin();
    let mut c = db.on(&cell, entry, Some(info(&key_info, true)));
    let put = |c: &mut AtollCursor, a: Value, rowid: i64| {
        let record = record_of(&[a, Value::from_i64(rowid)]);
        let key = BTreeKey::IndexKey(record.as_record_ref());
        drive(&db.io, || c.insert(&key))
    };
    assert!(put(&mut c, Value::from_i64(1), 10).is_ok());
    let err = put(&mut c, Value::from_i64(1), 11).expect_err("a second row of key 1");
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    assert!(put(&mut c, Value::Null, 12).is_ok());
    assert!(
        put(&mut c, Value::Null, 13).is_ok(),
        "a NULL keeps its rowid"
    );
    assert_eq!(walk(&mut c, &db.io, true).len(), 3);
    db.commit(&cell);
}

/// A key longer than the tree holds fails its write, naming the limit.
#[test]
fn a_key_past_the_tree_is_refused() {
    let db = Db::open();
    let key_info = [KeyInfo {
        sort_order: SortOrder::Asc,
        collation: CollationSeq::BINARY,
        nulls_order: None,
    }];
    let entry = db.empty(Layout::Index);
    let cell = db.begin();
    let mut c = db.on(&cell, entry, Some(info(&key_info, false)));
    let record = record_of(&[Value::build_text("k".repeat(2000))]);
    let key = BTreeKey::IndexKey(record.as_record_ref());
    let err = drive(&db.io, || c.insert(&key)).expect_err("a key too long");
    assert!(err.to_string().contains("holds keys of at most"), "{err}");
    db.commit(&cell);
}

/// The statement that created a tree writes it plainly: a rollback undoes none of those writes,
/// the tree going on the abandoned list whole, while versioned writes into a tree created in an
/// earlier statement are undone one by one.
#[test]
fn the_creating_statement_writes_plainly_and_a_rollback_abandons_the_tree_whole() {
    let db = Db::open();
    for plain in [true, false] {
        let cell = db.begin();
        let root = {
            let mut guard = cell.lock();
            let txn = guard.as_mut().unwrap();
            let root = run(&db.io, || {
                db.db.create_tree(txn, SizeClass::new(0).unwrap())
            });
            if !plain {
                db.db.atoll().transactions().statement(txn).unwrap();
            }
            root
        };
        let entry = Entry {
            root,
            layout: Layout::Table,
            encoding: ENCODING,
            created_by: 0,
        };
        let mut c = db.on(&cell, entry, None);
        if plain {
            c.write_plainly();
        }
        for r in 0..100 {
            let record = record_of(&[Value::from_i64(r)]);
            written(&mut c, &db.io, &table_key(r, &record));
        }
        assert_eq!(
            walk(&mut c, &db.io, true).len(),
            100,
            "plain {plain}: its own rows"
        );
        let txn = cell.lock().take().unwrap();
        let mut abort = crate::storage::atoll::txn::Abort::new(txn);
        let txns = db.db.atoll().transactions();
        run(&db.io, || txns.rollback(db.db.versions(), &mut abort));
        assert_eq!(abort.abandoned(), &[root]);
        let left = db.entries(root);
        assert_eq!(
            left,
            if plain { 100 } else { 0 },
            "plain {plain}: entries undone"
        );
        db.db.abandoned(abort.abandoned());
        run(&db.io, || db.db.drain());
    }
}

/// A write behind another transaction's open version comes back as a wait, and stepped again
/// once that one commits, goes on, under read committed.
#[test]
fn a_write_behind_an_open_writer_waits_and_goes_on_once_it_commits() {
    let db = Db::open();
    let entry = db.empty(Layout::Table);
    let first = db.begin();
    let record = record_of(&[Value::from_i64(1)]);
    written(
        &mut db.on(&first, entry, None),
        &db.io,
        &table_key(1, &record),
    );
    let second = db.begin_rc();
    let mut c = db.on(&second, entry, None);
    let other = record_of(&[Value::from_i64(2)]);
    let key = table_key(1, &other);
    assert!(matches!(written(&mut c, &db.io, &key), Write::Wait(_)));
    db.commit(&first);
    assert!(matches!(written(&mut c, &db.io, &key), Write::Done(_)));
    db.commit(&second);
    assert_eq!(
        walk(&mut db.cursor(entry, None), &db.io, true),
        vec![vec![Value::from_i64(2)]]
    );
}

fn probe_of(c: &mut AtollCursor, io: &Arc<dyn IO>, rowid: i64) -> Probe {
    run(io, || c.probe(&key::rowid(rowid)))
}

fn rowid_read(c: &mut AtollCursor, io: &Arc<dyn IO>, rowid: i64) -> SeekResult {
    run(io, || {
        c.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
    })
}

fn leaf_reaches() -> u64 {
    crate::storage::atoll::btree::LEAF_REACHES.with(|n| n.get())
}

/// An insert after its rowid's probe reaches its leaf once, by the probe: the rowid read answers
/// absent from the probe's look, and the write goes in where that look found its place, whether
/// the key goes after every key, past a gap or before every key.
#[test]
fn an_insert_after_its_probe_reaches_its_leaf_once() {
    let db = Db::open();
    let rows: Vec<_> = (1..=50)
        .map(|r| (key::rowid(r).to_vec(), vec![Value::from_i64(r)]))
        .collect();
    let entry = db.tree(Layout::Table, &rows);
    let cell = db.begin();
    let mut c = db.on(&cell, entry, None);
    for r in [51, 60, 0] {
        let before = leaf_reaches();
        assert_eq!(probe_of(&mut c, &db.io, r), Probe::Absent);
        assert_eq!(leaf_reaches() - before, 1, "the probe of {r}");
        assert_eq!(rowid_read(&mut c, &db.io, r), SeekResult::NotFound);
        assert!(c.is_empty(), "{r}: at no row");
        let record = record_of(&[Value::from_i64(r * 10)]);
        assert_eq!(
            written(&mut c, &db.io, &table_key(r, &record)),
            Write::Done(Outcome::Inserted)
        );
        assert_eq!(
            leaf_reaches() - before,
            1,
            "{r}: the read and the write reach no leaf again"
        );
    }
    let mut expected: Vec<Vec<Value>> = (1..=50).map(|r| vec![Value::from_i64(r)]).collect();
    expected.insert(0, vec![Value::from_i64(0)]);
    expected.extend([vec![Value::from_i64(510)], vec![Value::from_i64(600)]]);
    assert_eq!(walk(&mut c, &db.io, true), expected);
}

/// An insert of a value too long for its leaf leaves the cursor at the row it wrote, whole: shown
/// from the entry the write built, which keeps the value the leaf holds out of line.
#[test]
fn an_insert_of_a_value_out_of_line_shows_the_row_it_wrote() {
    let db = Db::open();
    let entry = db.empty(Layout::Table);
    let cell = db.begin();
    let mut c = db.on(&cell, entry, None);
    let long = Value::build_text("y".repeat(5000));
    let record = record_of(&[Value::from_i64(1), long.clone()]);
    assert_eq!(
        written(&mut c, &db.io, &table_key(1, &record)),
        Write::Done(Outcome::Inserted)
    );
    assert_eq!(values_of(&mut c, &db.io), vec![Value::from_i64(1), long]);
}

/// A probe's look answers only its own key, and only while its leaf is unchanged: a read of
/// another key, a read after another cursor of the transaction wrote the leaf, and a write of
/// another key each reach the leaf again and answer as they would with no probe.
#[test]
fn a_probes_look_answers_only_its_key_while_its_leaf_is_unchanged() {
    let db = Db::open();
    let rows: Vec<_> = (1..=3)
        .map(|r| (key::rowid(r).to_vec(), vec![Value::from_i64(r)]))
        .collect();
    let entry = db.tree(Layout::Table, &rows);
    let cell = db.begin();
    let mut c = db.on(&cell, entry, None);
    let mut other = db.on(&cell, entry, None);
    assert_eq!(probe_of(&mut c, &db.io, 4), Probe::Absent);
    assert_eq!(
        rowid_read(&mut c, &db.io, 1),
        SeekResult::Found,
        "another key"
    );
    assert_eq!(probe_of(&mut c, &db.io, 2), Probe::Present);
    assert_eq!(
        rowid_read(&mut c, &db.io, 2),
        SeekResult::Found,
        "a key present"
    );
    assert_eq!(probe_of(&mut c, &db.io, 4), Probe::Absent);
    let four = record_of(&[Value::from_i64(4)]);
    assert_eq!(
        written(&mut other, &db.io, &table_key(4, &four)),
        Write::Done(Outcome::Inserted)
    );
    assert_eq!(
        rowid_read(&mut c, &db.io, 4),
        SeekResult::Found,
        "the row the other cursor put"
    );
    assert_eq!(probe_of(&mut c, &db.io, 10), Probe::Absent);
    let zero = record_of(&[Value::from_i64(0)]);
    assert_eq!(
        written(&mut c, &db.io, &table_key(0, &zero)),
        Write::Done(Outcome::Inserted),
        "a key other than the probe's"
    );
    let expected: Vec<Vec<Value>> = (0..=4).map(|r| vec![Value::from_i64(r)]).collect();
    assert_eq!(walk(&mut c, &db.io, true), expected);
    assert_eq!(db.entries(entry.root), 5);
}

/// A tombstone the probe finds answers the rowid read absent only where the statement sees it:
/// under read committed, a delete committed after the statement began is a tombstone the probe
/// passes and the read reads past, to the row the statement sees.
#[test]
fn a_probes_tombstone_answers_absent_only_where_the_statement_sees_it() {
    let db = Db::open();
    let entry = db.tree(
        Layout::Table,
        &[(key::rowid(1).to_vec(), vec![Value::from_i64(1)])],
    );
    let reader = db.begin_rc();
    let deleter = db.begin();
    let mut d = db.on(&deleter, entry, None);
    assert_eq!(rowid_read(&mut d, &db.io, 1), SeekResult::Found);
    assert_eq!(run(&db.io, || d.delete()), Write::Done(Outcome::Updated));
    assert_eq!(probe_of(&mut d, &db.io, 1), Probe::Absent);
    assert_eq!(
        rowid_read(&mut d, &db.io, 1),
        SeekResult::NotFound,
        "its own tombstone"
    );
    db.commit(&deleter);
    let mut c = db.on(&reader, entry, None);
    assert_eq!(probe_of(&mut c, &db.io, 1), Probe::Absent);
    assert_eq!(
        rowid_read(&mut c, &db.io, 1),
        SeekResult::Found,
        "the statement began before the delete"
    );
}

/// Inserts through a cursor build each row in buffers the cursor hands round, from its write to
/// the row it shows and back: past its first rows, an insert that splits no node allocates only
/// when its transaction's own log takes a chunk or spills one, its write's submission allocating
/// once a chunk (DESIGN.md §11.4.1).
#[cfg(not(any(shuttle, atoll_loom, miri)))]
#[test]
fn an_insert_that_splits_nothing_allocates_nothing() {
    use crate::storage::atoll::btree::SPLITS;
    use crate::storage::atoll::testing::allocations;
    let db = Db::open();
    let entry = db.empty(Layout::Table);
    let cell = db.begin();
    let mut c = db.on(&cell, entry, None);
    let rows = 1000;
    let records: Vec<_> = (0..rows)
        .map(|r| record_of(&[Value::from_i64(r), Value::build_text("x".repeat(100))]))
        .collect();
    for r in 0..10 {
        written(&mut c, &db.io, &table_key(r, &records[r as usize]));
    }
    let room = || {
        let guard = cell.lock();
        let changes = guard.as_ref().unwrap().changes();
        (changes.memory(), changes.parts().len())
    };
    let (mut plain, mut allocating, mut grew) = (0, Vec::new(), 0);
    for r in 10..rows {
        let (splits, before, had) = (SPLITS.with(|n| n.get()), allocations(), room());
        written(&mut c, &db.io, &table_key(r, &records[r as usize]));
        let made = allocations() - before;
        let now = room();
        let growing = now.0 > had.0 || now.1 > had.1;
        grew += growing as usize;
        if SPLITS.with(|n| n.get()) == splits && !growing {
            plain += 1;
            if made > 0 {
                allocating.push((r, made));
            }
        }
    }
    assert!(plain > rows / 2, "{plain} inserts split nothing");
    assert!(
        grew > 0,
        "the transaction's log never took or spilled a chunk"
    );
    assert!(
        allocating.is_empty(),
        "inserts that split nothing and allocated: {allocating:?}"
    );
}

/// A cursor is its operation's top (DESIGN.md 9.1.3, *The top and the owner*): a write that comes
/// short of memory reserves its frame at the cursor, holding nothing, keeps its tenure there
/// across the calls it takes, and ends it once done; its statement's reset ends one unfinished.
#[test]
fn a_cursors_write_that_comes_short_reserves_at_the_cursor_and_ends_there() {
    // Checked with every Atoll value let go: a failed check unwinding through one ends the
    // process rather than failing the test.
    if let Err(e) = cursor_write_comes_short() {
        panic!("{e}");
    }
}

fn cursor_write_comes_short() -> std::result::Result<(), String> {
    use crate::io::Completion;
    use crate::storage::atoll::buffer::scarce;
    use crate::types::IOCompletions;
    let db = Db::open();
    let entry = db.empty(Layout::Table);
    let cell = db.begin();
    let mut c = db.on(&cell, entry, None);
    let pool = db.db.atoll().pool().clone();
    let record = record_of(&[Value::build_text("v".repeat(600))]);
    let step =
        |c: &mut AtollCursor, rowid: i64| -> std::result::Result<Option<Completion>, String> {
            match scarce(|| c.insert(&table_key(rowid, &record))).map_err(|e| e.to_string())? {
                IOResult::Done(Write::Done(_)) => Ok(None),
                IOResult::Done(w) => Err(format!("{w:?}")),
                IOResult::IO(IOCompletions(w)) => Ok(Some(w)),
            }
        };
    let wait = |w: &Completion| {
        if !w.finished() {
            crate::storage::atoll::park::wait(db.io.as_ref(), w);
        }
    };
    let mut short = 0;
    for rowid in 0..64 {
        while let Some(w) = step(&mut c, rowid)? {
            if c.shortage_queued() {
                short += 1;
            }
            wait(&w);
        }
        if c.shortage_queued() || pool.shortage_len() != 0 {
            return Err(format!("write {rowid} done kept its tenure"));
        }
    }
    if short == 0 {
        return Err("no write came short: the test reached no shortage".into());
    }
    // A write that came short, then its statement's reset.
    let mut rowid = 64;
    loop {
        if rowid == 1024 {
            return Err("no write came short again".into());
        }
        match step(&mut c, rowid)? {
            None => rowid += 1,
            Some(_) if c.shortage_queued() => break,
            Some(w) => wait(&w),
        }
    }
    if pool.shortage_len() != 1 {
        return Err(format!("{} places queued", pool.shortage_len()));
    }
    c.end_operation();
    if c.shortage_queued() || pool.shortage_len() != 0 {
        return Err("the reset left the write's tenure behind".into());
    }
    // The write goes on with memory to spare.
    match run(&db.io, || c.insert(&table_key(rowid, &record))) {
        Write::Done(_) => Ok(()),
        w => Err(format!("{w:?}")),
    }
}
