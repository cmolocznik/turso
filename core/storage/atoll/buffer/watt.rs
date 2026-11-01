//! Write-Aware Timestamp Tracking (Vöhringer and Leis, PVLDB 16(11), 2023), the replacement
//! policy of LeanStore 2024.
//!
//! Each frame keeps the epochs of its last [`READS`] accesses and its last [`WRITES`] changes. A
//! page's value estimates how often it is used: for each `i`, `i` accesses over the time since the
//! `i`th most recent one, taking the largest (equation 1 and the max aggregation), with the most
//! recent access alone damped by [`DAMPING`]. Changes count the same way, weighted by
//! [`WRITE_WEIGHT`] because evicting a dirty page costs a write (equation 3). Every default is the
//! paper's tuned value (its table of WATT parameters, figures 3 to 8).
//!
//! Time is an epoch, advanced each time a quarter of the pool has been evicted (the paper's 4
//! epochs per full replacement). An access in the epoch already recorded is not recorded again,
//! so a hot page's tracker is read, not written, and never bounces between cores. Races between
//! two recordings can leave one stale entry, which the paper shows harmless (§5.1).
//!
//! The paper divides by `now - t_i`, which is zero for an access in the current epoch. Here
//! elapsed time counts the current epoch as one, `now - t_i + 1`, so pages used this epoch still
//! rank by how often.

use crate::sync::atomic::{AtomicU32, AtomicU8, Ordering};

/// Accesses remembered per frame (the paper's access log length, figure 4).
pub const READS: usize = 8;
/// Changes remembered per frame (its write log length, figure 7).
pub const WRITES: usize = 4;
/// How much a change costs against a read when evicting (its write weight, figure 8).
pub const WRITE_WEIGHT: f32 = 4.0;
/// Weight of the subfrequency of the most recent access alone (its dampening factor, figure 6).
pub const DAMPING: f32 = 0.1;
/// Frames whose least value sets the eviction threshold (its sampling size, figure 3).
pub const THRESHOLD_SAMPLE: usize = 8;
/// Frames looked at per eviction round, evicted if at or below the threshold (its listing 2).
pub const CANDIDATES: usize = 64;
/// Epochs per eviction of the whole pool (its replacements per epoch, figure 5).
pub const EPOCHS_PER_TURNOVER: usize = 4;

/// The first epoch; 0 marks an empty entry.
pub const FIRST_EPOCH: u32 = 1;

#[derive(Debug, Default)]
pub struct Tracker {
    reads: [AtomicU32; READS],
    writes: [AtomicU32; WRITES],
    read_head: AtomicU8,
    write_head: AtomicU8,
}

fn record<const N: usize>(log: &[AtomicU32; N], head: &AtomicU8, now: u32) {
    let at = head.load(Ordering::Relaxed) as usize % N;
    if log[at].load(Ordering::Relaxed) != now {
        let next = (at + 1) % N;
        log[next].store(now, Ordering::Relaxed);
        head.store(next as u8, Ordering::Relaxed);
    }
}

fn frequency<const N: usize>(log: &[AtomicU32; N], head: &AtomicU8, now: u32) -> f32 {
    let head = head.load(Ordering::Relaxed) as usize % N;
    let mut best = 0f32;
    for i in 1..=N {
        let t = log[(head + N + 1 - i) % N].load(Ordering::Relaxed);
        if t == 0 {
            break;
        }
        let elapsed = now.saturating_sub(t) as f32 + 1.0;
        let mut sf = i as f32 / elapsed;
        if i == 1 {
            sf *= DAMPING;
        }
        best = best.max(sf);
    }
    best
}

impl Tracker {
    /// Records an access in epoch `now`.
    pub fn read(&self, now: u32) {
        record(&self.reads, &self.read_head, now);
    }

    /// Records a change in epoch `now`, which is an access too.
    pub fn write(&self, now: u32) {
        self.read(now);
        record(&self.writes, &self.write_head, now);
    }

    /// The epoch of the latest access, 0 for none.
    pub fn last(&self) -> u32 {
        self.reads[self.read_head.load(Ordering::Relaxed) as usize % READS].load(Ordering::Relaxed)
    }

    /// The page's value in epoch `now`: higher is worth keeping more.
    pub fn value(&self, now: u32) -> f32 {
        frequency(&self.reads, &self.read_head, now)
            + WRITE_WEIGHT * frequency(&self.writes, &self.write_head, now)
    }

    /// Forgets every access, for a frame taking a new page.
    pub fn clear(&self) {
        for t in self.reads.iter().chain(&self.writes) {
            t.store(0, Ordering::Relaxed);
        }
        self.read_head.store(0, Ordering::Relaxed);
        self.write_head.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_is_the_best_subfrequency_with_the_latest_access_damped_and_writes_weighted() {
        // The paper's running example: accesses at 0, 8, 15 and 42, read at 50. Epoch 0 marks an
        // empty entry here, so the example is shifted by one.
        let t = Tracker::default();
        for at in [1, 9, 16, 43] {
            t.read(at);
        }
        t.read(43);
        let now = 51;
        let sf = |i: f32, at: u32| i / (now - at + 1) as f32;
        let expected = [DAMPING * sf(1.0, 43), sf(2.0, 16), sf(3.0, 9), sf(4.0, 1)]
            .into_iter()
            .fold(0f32, f32::max);
        assert_eq!(t.value(now), expected);
        assert_eq!(
            expected,
            sf(4.0, 1),
            "the long-run frequency wins, as in the paper"
        );
        let w = Tracker::default();
        w.write(43);
        assert_eq!(
            w.value(now),
            (1.0 + WRITE_WEIGHT) * DAMPING * sf(1.0, 43),
            "a change counts as an access and as a write"
        );
    }

    #[test]
    fn a_page_used_steadily_outranks_one_used_once_lately() {
        let steady = Tracker::default();
        for at in (1..=40).step_by(5) {
            steady.read(at);
        }
        let once = Tracker::default();
        once.read(40);
        assert!(steady.value(41) > once.value(41));
    }
}
