use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET, WRITEBACK_BYTES};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::{Commit, Isolation, Txn, TxnConfig};

fn open(budget: usize) -> (Arc<dyn IO>, Atoll) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let config = AtollConfig {
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
            budget,
            writeback_bytes: WRITEBACK_BYTES,
            kept_bytes: None,
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        txn: TxnConfig { slots: 4 },
    };
    let db = Atoll::open(io.clone(), "/db/arena", config).unwrap();
    (io, db)
}

/// Ends `txn`, which logged nothing: its arena entries are a test's own.
fn end(io: &Arc<dyn IO>, db: &Atoll, txn: Txn) {
    let mut op = Commit::new(txn);
    drive(io, || db.transactions().commit(&mut op)).unwrap();
}

fn begin(io: &Arc<dyn IO>, db: &Atoll) -> Txn {
    let mut txn = drive(io, || db.transactions().begin(Isolation::Snapshot)).unwrap();
    db.transactions().statement(&mut txn).unwrap();
    txn
}

/// The `n`th entry's key and replaced bytes, of lengths that vary, so pages fill unevenly.
fn entry(n: u64) -> (Vec<u8>, Vec<u8>) {
    let key = vec![(n % 251) as u8; 1 + (n * 37 % 200) as usize];
    let replaced = vec![(n % 241) as u8; (n * 7_919 % 3_000) as usize];
    (key, replaced)
}

fn append(io: &Arc<dyn IO>, arenas: &Arenas, txn: &mut Txn, n: u64) -> Ptr {
    let (key, replaced) = entry(n);
    drive(io, || {
        arenas.append(txn, PageId::from_raw(n), &key, &replaced)
    })
    .unwrap()
}

fn expect(io: &Arc<dyn IO>, arenas: &Arenas, ptr: Ptr, n: u64, statement: Statement) {
    let (key, replaced) = entry(n);
    let read = drive(io, || arenas.read(ptr)).unwrap();
    assert_eq!(
        read,
        Entry {
            statement,
            root: PageId::from_raw(n),
            key,
            replaced
        },
        "entry {n}"
    );
}

/// Entries appended over many pages read back as written, and walk back from the last to the
/// first in order, the first having none before it; the transaction knows its first and last.
#[test]
fn entries_read_back_and_walk_back_across_pages() {
    let (io, db) = open(MIN_BUDGET);
    let arenas = Arenas::new(db.pool().clone(), 4);
    let mut txn = begin(&io, &db);
    let mut ptrs = Vec::new();
    for n in 0..2_000u64 {
        if n % 100 == 99 {
            db.transactions().statement(&mut txn).unwrap();
        }
        ptrs.push((append(&io, &arenas, &mut txn, n), txn.command()));
    }
    let pages: std::collections::BTreeSet<PageId> = ptrs.iter().map(|(p, _)| p.page()).collect();
    assert!(pages.len() >= 20, "only {} pages", pages.len());
    assert_eq!(txn.replaced, Some((ptrs[0].0, ptrs.last().unwrap().0)));
    for (n, &(ptr, statement)) in ptrs.iter().enumerate() {
        expect(&io, &arenas, ptr, n as u64, statement);
    }
    let mut at = Some(ptrs.last().unwrap().0);
    let mut walked = Vec::new();
    while let Some(ptr) = at {
        walked.push(ptr);
        at = drive(&io, || arenas.before(ptr)).unwrap();
    }
    walked.reverse();
    assert_eq!(walked, ptrs.iter().map(|(p, _)| *p).collect::<Vec<_>>());
    end(&io, &db, txn);
}

/// Two slots appending in turn keep arenas of their own: each walks back through its entries
/// alone.
#[test]
fn slots_keep_arenas_of_their_own() {
    let (io, db) = open(MIN_BUDGET);
    let arenas = Arenas::new(db.pool().clone(), 4);
    let mut a = begin(&io, &db);
    let mut b = begin(&io, &db);
    assert_ne!(a.slot(), b.slot());
    let (mut of_a, mut of_b) = (Vec::new(), Vec::new());
    for n in 0..600u64 {
        match n % 2 {
            0 => of_a.push(append(&io, &arenas, &mut a, n)),
            _ => of_b.push(append(&io, &arenas, &mut b, n)),
        }
    }
    for own in [of_a, of_b] {
        let mut at = own.last().copied();
        let mut walked = Vec::new();
        while let Some(ptr) = at {
            walked.push(ptr);
            at = drive(&io, || arenas.before(ptr)).unwrap();
        }
        walked.reverse();
        assert_eq!(walked, own);
    }
    end(&io, &db, a);
    end(&io, &db, b);
}

/// A reader on another thread reads every entry as soon as it is published while the writer goes
/// on appending to the same page, and reads each as written: an append changes no entry before it.
/// The writer goes no more than `AHEAD` entries past the newest the reader has read, so the reads
/// overlap the appends however the threads are scheduled.
#[test]
fn entries_read_whole_while_their_page_is_appended_to() {
    const ENTRIES: u64 = 3_000;
    const AHEAD: u64 = 8;
    let (io, db) = open(MIN_BUDGET);
    let arenas = Arc::new(Arenas::new(db.pool().clone(), 4));
    let mut txn = begin(&io, &db);
    let statement = txn.command();
    let published = Arc::new(std::sync::Mutex::new(Vec::<(Ptr, u64)>::new()));
    // The newest entry the reader has read, plus one.
    let read_to = Arc::new(crate::sync::atomic::AtomicU64::new(0));
    let reader = {
        let (io, arenas, published, read_to) = (
            io.clone(),
            arenas.clone(),
            published.clone(),
            read_to.clone(),
        );
        std::thread::spawn(move || {
            let mut reads = 0u64;
            while read_to.load(Ordering::Acquire) < ENTRIES {
                let seen: Vec<(Ptr, u64)> = published.lock().unwrap().clone();
                for &(ptr, n) in seen.iter().rev().take(AHEAD as usize) {
                    expect(&io, &arenas, ptr, n, statement);
                    reads += 1;
                }
                read_to.store(seen.len() as u64, Ordering::Release);
            }
            reads
        })
    };
    for n in 0..ENTRIES {
        while read_to.load(Ordering::Acquire) + AHEAD < n {
            std::thread::yield_now();
        }
        let ptr = append(&io, &arenas, &mut txn, n);
        published.lock().unwrap().push((ptr, n));
    }
    let reads = reader.join().unwrap();
    // To read up to the last entry with the writer at most `AHEAD` past it, the reader read at
    // least once every `AHEAD + 1` entries.
    assert!(reads >= ENTRIES / (AHEAD + 1), "only {reads} reads");
    end(&io, &db, txn);
}

/// More entries than the pool holds: their pages are evicted to the temporary file and read back
/// whole, and the walk back crosses them.
#[test]
fn evicted_pages_read_back_whole() {
    let (io, db) = open(MIN_BUDGET);
    let arenas = Arenas::new(db.pool().clone(), 4);
    let mut txn = begin(&io, &db);
    let statement = txn.command();
    let mut ptrs = Vec::new();
    let mut bytes = 0;
    let mut n = 0u64;
    // Twice the pool's memory of entries.
    while bytes < 2 * MIN_BUDGET {
        let (key, replaced) = entry(n);
        bytes += key.len() + replaced.len();
        ptrs.push(append(&io, &arenas, &mut txn, n));
        n += 1;
    }
    for (n, &ptr) in ptrs.iter().enumerate() {
        expect(&io, &arenas, ptr, n as u64, statement);
    }
    let mut at = ptrs.last().copied();
    let mut count = 0;
    while let Some(ptr) = at {
        count += 1;
        at = drive(&io, || arenas.before(ptr)).unwrap();
    }
    assert_eq!(count, ptrs.len());
    end(&io, &db, txn);
}
