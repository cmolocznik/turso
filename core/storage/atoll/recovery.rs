//! Recovery (audit 18): the checkpoint in charge, then the log from its redo point.
//!
//! 1. The metadata in charge is the copies numbered at or below the superblock's checkpoint
//!    ([`PageStore::open`]). A metadata page that a later checkpoint, cut short, named in its
//!    intent record may hold a torn copy in its other place: it is owed a write again, so the next
//!    checkpoint, which takes the same number, writes over it (review 2).
//! 2. The log is read from the redo point to its end twice. The first pass keeps the roots that
//!    replayed creations name from every other allocation, notes which drained roots the mapping
//!    holds, and finds the clock and where the log ends: a superblock with no room for the gap
//!    reopening there leaves stops recovery before anything is applied. The second applies, in log order, every committed body whose
//!    commit timestamp is past the checkpoint's snapshot, and every run ([`replay`]): a body at
//!    or below the snapshot is in the checkpoint's pages already.
//! 3. The log reopens past its end, the gap recorded in a superblock before anything is appended
//!    (review 7), the clock restarts above every timestamp read, and a full checkpoint makes what
//!    was recovered the state in charge.
//!
//! No analysis pass, no undo, no tombstone sweep, no page LSN.

use super::buffer::{BufferPool, LocalSource};
use super::checkpoint::{Checkpointer, META_INTENT};
use super::control::{Control, Superblock, MAX_CONTINUATIONS};
use super::db::AtollConfig;
use super::drive;
use super::log::{Log, LogConfig, Lsn};
use super::meta::MetaId;
use super::page::PageId;
use super::radix::FREE;
use super::replay::{Apply, Gather};
use super::store::PageStore;
use super::txn::changes::Change;
use super::txn::{self, Clock, Transactions};
use crate::io::IO;
use crate::sync::Arc;
use crate::{LimboError, Result};
use std::collections::HashSet;

/// What a recovery leaves: the database open on its recovered state, that state in charge.
pub struct Recovered {
    pub log: Arc<Log>,
    pub pool: Arc<BufferPool>,
    pub checkpointer: Checkpointer,
    pub txns: Transactions,
}

/// What the first pass over the log found.
#[derive(Default)]
struct Scan {
    /// The roots of the creations the second pass replays.
    created: Vec<PageId>,
    /// The roots of the drains it replays.
    drained: HashSet<PageId>,
    /// The metadata pages a checkpoint cut short meant to write.
    intents: Vec<MetaId>,
    clock: u64,
    /// Where the log's valid records end.
    end: Lsn,
}

/// Recovers the database `superblock` is in charge of, its store opened on that superblock.
pub fn recover(
    io: Arc<dyn IO>,
    base: &str,
    config: &AtollConfig,
    store: Arc<PageStore>,
    control: Control,
    superblock: Superblock,
) -> Result<Recovered> {
    let scan = scan(&io, base, &config.log, &superblock)?;
    let restart = Log::resumes_at(scan.end);
    if restart > scan.end && superblock.continuations.len() >= MAX_CONTINUATIONS {
        return Err(LimboError::InternalError(format!(
            "recovery would reopen the log at {restart}, past its end at {}, and the superblock \
             already records the {MAX_CONTINUATIONS} gaps it holds: recovering on would leave a gap \
             no reader goes past",
            scan.end
        )));
    }
    let pool = BufferPool::new(
        config.buffer.clone(),
        Arc::new(LocalSource::new(store.clone())),
        store.clone(),
    )?;
    // Owed a write: the next checkpoint takes the number that wrote them.
    if !scan.intents.is_empty() {
        store.meta().set_torn_ok(true);
        let frames = drive(&io, || store.meta().get_all(&scan.intents));
        store.meta().set_torn_ok(false);
        for frame in frames? {
            while store.meta().owe(&frame).is_none() {
                let wait = frame.wait_while_held();
                crate::storage::atoll::park::wait(io.as_ref(), &wait);
            }
        }
    }
    // Roots the mapping holds are judged before anything is replayed.
    let mut opened = Vec::new();
    for &root in &scan.drained {
        if drive(&io, || store.entry(root))? != FREE {
            opened.push(root);
        }
    }
    store.reserve(scan.created.iter().copied());
    let mut apply = Apply::new(io.clone(), pool.clone(), store.clone()).opened_with(opened);
    let mut reader = Log::reader(io.clone(), base, &config.log, superblock.redo)
        .following(&superblock.continuations);
    let mut gather = Gather::new(superblock.snapshot);
    let mut parts = Log::rereader(io.clone(), base, &config.log);
    while let Some(r) = reader.next_record()? {
        let Some((ts, body)) = gather.record(r.header.kind, r.header.txn, r.start, &r.payload)?
        else {
            continue;
        };
        if ts.is_none_or(|ts| ts > superblock.snapshot) {
            apply.apply(&body, &mut parts)?;
        }
    }
    store.release_reserved();
    let end = reader.position();
    if end != scan.end {
        return Err(LimboError::Corrupt(format!(
            "the log read to {} once and to {end} the second time",
            scan.end
        )));
    }
    // Reopening zeroes everything a crash left from `restart` on, durably. The gap is recorded
    // only then, and before anything is appended: a reader that follows it finds nothing past it
    // but what this recovery appends, never records the crash left there.
    let log = Log::open_in(io.clone(), base, config.log.clone(), end, &pool)?;
    assert_eq!(log.end(), restart, "the log reopened elsewhere");
    let mut in_charge = superblock;
    if restart > end {
        in_charge.continuations.push((end, restart));
        in_charge.seq += 1;
        control.write_and_wait(&in_charge)?;
    }
    let clock = Arc::new(Clock::after(scan.clock.max(in_charge.clock)));
    let txns = Transactions::new(&config.txn, clock, log.clone());
    let checkpointer = Checkpointer::new(
        pool.clone(),
        store.clone(),
        log.clone(),
        control,
        in_charge,
        config.log_budget(),
        config.checkpoint_write_bytes(),
    );
    // What was recovered is in memory alone until a checkpoint writes it.
    let mut ticket = checkpointer.request_full();
    drive(&io, || checkpointer.step_full(&mut ticket, &txns))?;
    Ok(Recovered {
        log,
        pool,
        checkpointer,
        txns,
    })
}

/// The first pass: what the second needs before it applies anything.
fn scan(io: &Arc<dyn IO>, base: &str, config: &LogConfig, superblock: &Superblock) -> Result<Scan> {
    let mut out = Scan {
        clock: superblock.clock,
        ..Scan::default()
    };
    let mut reader =
        Log::reader(io.clone(), base, config, superblock.redo).following(&superblock.continuations);
    let mut gather = Gather::new(superblock.snapshot);
    let mut parts = Log::rereader(io.clone(), base, config);
    while let Some(r) = reader.next_record()? {
        // A record's transaction is named by its start timestamp, and a spilled part's too.
        out.clock = out.clock.max(r.header.txn);
        if r.header.kind == txn::SPILL {
            if let Some(start) = r.payload.get(..8) {
                out.clock = out.clock.max(u64::from_le_bytes(start.try_into().unwrap()));
            }
        }
        if r.header.kind == META_INTENT {
            out.intents.extend(decode_intent(&r.payload)?);
            continue;
        }
        let Some((ts, body)) = gather.record(r.header.kind, r.header.txn, r.start, &r.payload)?
        else {
            continue;
        };
        if let Some(ts) = ts {
            out.clock = out.clock.max(ts);
            if ts <= superblock.snapshot {
                continue;
            }
        }
        body.changes(&mut parts, &mut |change| {
            match change {
                Change::Create { root, .. } => out.created.push(root),
                Change::Drain { root, .. } => {
                    out.drained.insert(root);
                }
                _ => {}
            }
            Ok(())
        })?;
    }
    out.end = reader.position();
    Ok(out)
}

/// The metadata pages an intent record names.
fn decode_intent(payload: &[u8]) -> Result<Vec<MetaId>> {
    let corrupt = || LimboError::Corrupt("a metadata intent record does not decode".into());
    let ids = payload.get(8..).ok_or_else(corrupt)?;
    if ids.len() % 8 != 0 {
        return Err(corrupt());
    }
    ids.chunks_exact(8)
        .map(|c| {
            MetaId::of_pid(PageId::from_raw(u64::from_le_bytes(c.try_into().unwrap())))
                .ok_or_else(corrupt)
        })
        .collect()
}
