//! The writer registry (DESIGN.md §10): per transaction slot, the transaction that has logged a
//! group and not yet its Commit or End, with where its first and last groups start. A checkpoint
//! takes it once its redo point R is fixed, as the writers that may still need undo by the log
//! before R.
//!
//! A transaction enters its slot's cell as each of its groups is reserved, before the group's copy
//! completes ([`super::Log::append_with`]), and leaves it as its Commit or End is reserved. The
//! log is durable past a group only once the group is copied in, and R is never past what is
//! durable, so a table taken once R is fixed holds every writer with a group before R, and no
//! writer whose Commit or End lies before it.
//!
//! Each cell is written by its slot's transaction alone, under a versioned latch nothing else
//! takes; a checkpoint reads it optimistically and validates, as a page is read.

use self::sync::{yield_now, AtomicU64, Ordering};
use super::record::{GroupStart, Header, Kind};
use crate::storage::atoll::latch::{Held, VersionedLatch};
use crate::sync::Arc;
use crate::{LimboError, Result};

/// A checkpoint's writer table, a record of its own:
///
/// ```text
/// count: u32 | (txn: u64 | first: u64 | last: u64) * count
/// ```
pub const WRITERS: Kind = Kind::new(20).unwrap();

/// Bytes of a table of `count` writers.
pub fn table_len(count: usize) -> usize {
    4 + 24 * count
}

pub fn encode_table(writers: &[Writer]) -> Vec<u8> {
    let mut out = Vec::with_capacity(table_len(writers.len()));
    out.extend_from_slice(&(writers.len() as u32).to_le_bytes());
    for w in writers {
        out.extend_from_slice(&w.txn.to_le_bytes());
        out.extend_from_slice(&w.first.get().to_le_bytes());
        out.extend_from_slice(&w.last.get().to_le_bytes());
    }
    out
}

/// The writers a table record's payload lists.
pub fn decode_table(payload: &[u8]) -> Result<Vec<Writer>> {
    let corrupt = || LimboError::Corrupt("a writer table does not decode".into());
    let count = u32::from_le_bytes(payload.get(..4).ok_or_else(corrupt)?.try_into().unwrap());
    if payload.len() != table_len(count as usize) {
        return Err(corrupt());
    }
    payload[4..]
        .chunks_exact(24)
        .map(|w| {
            let word = |at: usize| u64::from_le_bytes(w[at..at + 8].try_into().unwrap());
            let group = |raw: u64| match GroupStart::decode(raw) {
                Some(Some(g)) => Ok(g),
                _ => Err(corrupt()),
            };
            let (first, last) = (group(word(8))?, group(word(16))?);
            if word(0) == 0 || first > last {
                return Err(corrupt());
            }
            Ok(Writer {
                txn: word(0),
                first,
                last,
            })
        })
        .collect()
}

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU64, Ordering};
    pub(super) use crate::thread::yield_now;
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
    pub(super) use loom::thread::yield_now;
}

/// A writer as the registry holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Writer {
    pub txn: u64,
    pub first: GroupStart,
    pub last: GroupStart,
}

#[derive(Default)]
struct Cell {
    latch: VersionedLatch,
    /// The slot's writer; 0 for none. The words below are its.
    txn: AtomicU64,
    first: AtomicU64,
    last: AtomicU64,
}

pub struct Writers {
    cells: Box<[Cell]>,
}

impl Writers {
    pub fn new(slots: usize) -> Self {
        Self {
            cells: (0..slots).map(|_| Cell::default()).collect(),
        }
    }

    /// The cell of `slot`, held: only its transaction changes it, so the latch is always free.
    fn hold(&self, slot: u16) -> (&Cell, Held<&VersionedLatch>) {
        let cell = &self.cells[slot as usize];
        let held = Held::try_take(&cell.latch)
            .expect("a writer cell taken by two holders: only its slot's transaction writes it");
        (cell, held)
    }

    /// Enters `txn`'s group reserved at `start`, in `slot`: its first group makes it a writer.
    pub fn reserved(&self, slot: u16, txn: u64, start: GroupStart) {
        let (cell, _held) = self.hold(slot);
        // Relaxed, here and below: the cell's own words, under its latch.
        if cell.txn.load(Ordering::Relaxed) != txn {
            assert_eq!(
                cell.txn.load(Ordering::Relaxed),
                0,
                "a slot's transaction logged before the one before it finished"
            );
            cell.txn.store(txn, Ordering::Relaxed);
            cell.first.store(start.get(), Ordering::Relaxed);
        }
        cell.last.store(start.get(), Ordering::Relaxed);
    }

    /// Removes `txn`, whose Commit or End is reserved: no log after that needs it undone.
    pub fn finished(&self, slot: u16, txn: u64) {
        let (cell, _held) = self.hold(slot);
        if cell.txn.load(Ordering::Relaxed) == txn {
            cell.txn.store(0, Ordering::Relaxed);
        }
    }

    /// Every writer now. A cell held is one its transaction is entering a group in, which takes
    /// no IO and no yield, so the read waits on it a moment and reads again.
    pub fn snapshot(&self) -> Vec<Writer> {
        let mut out = Vec::new();
        for cell in self.cells.iter() {
            loop {
                let Some(seen) = cell.latch.optimistic() else {
                    yield_now();
                    continue;
                };
                // Relaxed: validated against the latch's version below.
                let (txn, first, last) = (
                    cell.txn.load(Ordering::Relaxed),
                    cell.first.load(Ordering::Relaxed),
                    cell.last.load(Ordering::Relaxed),
                );
                if !cell.latch.validate(seen) {
                    continue;
                }
                if txn != 0 {
                    out.push(Writer {
                        txn,
                        first: GroupStart::at(first),
                        last: GroupStart::at(last),
                    });
                }
                break;
            }
        }
        out
    }
}

/// What a transaction's change is logged as: the transaction, where its previous group starts,
/// and its slot in the registry, which its group enters as it is reserved.
#[derive(Clone)]
pub struct TxnTail {
    txn: u64,
    prev: Option<GroupStart>,
    writers: Arc<Writers>,
    slot: u16,
}

impl TxnTail {
    pub fn new(txn: u64, prev: Option<GroupStart>, writers: Arc<Writers>, slot: u16) -> Self {
        assert_ne!(txn, 0, "transaction 0 names none");
        Self {
            txn,
            prev,
            writers,
            slot,
        }
    }

    pub fn txn(&self) -> u64 {
        self.txn
    }

    /// The header of the transaction's next record of `kind`.
    pub fn header(&self, kind: Kind) -> Header {
        Header {
            kind,
            flags: 0,
            txn: self.txn,
            prev: self.prev,
        }
    }

    /// Enters the group reserved at `start` in the registry.
    pub fn reserved(&self, start: GroupStart) {
        self.writers.reserved(self.slot, self.txn, start);
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests {
    use super::*;

    fn at(n: u64) -> GroupStart {
        GroupStart::at(n * 16)
    }

    #[test]
    fn a_writer_is_listed_from_its_first_group_to_its_commit() {
        let writers = Writers::new(3);
        assert_eq!(writers.snapshot(), vec![]);
        writers.reserved(1, 7, at(10));
        writers.reserved(1, 7, at(12));
        writers.reserved(2, 9, at(11));
        assert_eq!(
            writers.snapshot(),
            vec![
                Writer {
                    txn: 7,
                    first: at(10),
                    last: at(12)
                },
                Writer {
                    txn: 9,
                    first: at(11),
                    last: at(11)
                },
            ]
        );
        writers.finished(1, 7);
        // The slot's next transaction starts afresh.
        writers.reserved(1, 8, at(20));
        assert_eq!(
            writers.snapshot(),
            vec![
                Writer {
                    txn: 8,
                    first: at(20),
                    last: at(20)
                },
                Writer {
                    txn: 9,
                    first: at(11),
                    last: at(11)
                },
            ]
        );
    }

    #[test]
    fn a_writer_table_reads_back_as_written_and_refuses_a_bad_one() {
        let writers = vec![
            Writer {
                txn: 3,
                first: at(0),
                last: at(4),
            },
            Writer {
                txn: u64::MAX - 1,
                first: at(9),
                last: at(9),
            },
        ];
        let bytes = encode_table(&writers);
        assert_eq!(bytes.len(), table_len(2));
        assert_eq!(decode_table(&bytes).unwrap(), writers);
        assert_eq!(decode_table(&encode_table(&[])).unwrap(), vec![]);
        for bad in [
            &bytes[..bytes.len() - 1],
            &bytes[..3],
            // A count past what follows.
            &[[3u8, 0, 0, 0].as_slice(), &bytes[4..]].concat()[..],
        ] {
            assert!(decode_table(bad).is_err(), "{} bytes decoded", bad.len());
        }
        let mut off_grid = bytes.clone();
        off_grid[4 + 8] = 1;
        assert!(decode_table(&off_grid).is_err(), "a first group off the grid");
        let mut backwards = bytes.clone();
        backwards[4 + 16..4 + 24].copy_from_slice(&0u64.to_le_bytes());
        backwards[4 + 8..4 + 16].copy_from_slice(&64u64.to_le_bytes());
        assert!(decode_table(&backwards).is_err(), "a first group past the last");
        let mut nobody = bytes.clone();
        nobody[4..12].copy_from_slice(&0u64.to_le_bytes());
        assert!(decode_table(&nobody).is_err(), "transaction 0");
    }

    #[test]
    #[should_panic(expected = "before the one before it finished")]
    fn a_slots_next_writer_cannot_enter_before_the_last_finished() {
        let writers = Writers::new(1);
        writers.reserved(0, 7, at(1));
        writers.reserved(0, 8, at(2));
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    /// A checkpoint snapshots a cell while its slot's writer moves on, through its Commit, to the
    /// slot's next transaction: what it reads is one transaction's words, never a mix.
    #[test]
    fn a_snapshot_never_mixes_two_writers_words() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let writers = Arc::new(Writers::new(1));
            let writer = {
                let writers = writers.clone();
                thread::spawn(move || {
                    writers.reserved(0, 1, GroupStart::at(16));
                    writers.reserved(0, 1, GroupStart::at(32));
                    writers.finished(0, 1);
                    writers.reserved(0, 2, GroupStart::at(48));
                })
            };
            for w in writers.snapshot() {
                let words = (w.txn, w.first.get(), w.last.get());
                assert!(
                    [(1, 16, 16), (1, 16, 32), (2, 48, 48)].contains(&words),
                    "a snapshot read {words:?}"
                );
            }
            writer.join().unwrap();
        });
    }
}
