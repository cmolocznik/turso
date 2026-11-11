//! The lend road for kept copies (DESIGN.md 9.1.1): frames lent from lend batches, which hold no
//! page and which no cache claim takes, so a checkpoint's reads never wait on the copies it holds.
//!
//! A request of one class takes the fast path: a bounded sequence with no retry (`slow` read, one
//! add to a copy batch's count and one clear of its bits). Anything else takes the rare path, one
//! owner at a time by the gate, which plans the whole placement before it takes anything. Neither
//! loops on a word, and nothing blocks: a request that cannot have its frames is turned down, its
//! writer lets go of its latches and waits on the pool's supply ([`BufferPool::wait_for_room`],
//! DESIGN.md 9.1.3). Everything either path gives back is a change of the supply.

use super::frame::{LENT, NO_PAGE};
use super::memory::{self, LendKind, CORE, GRAINS, LEND_TOKEN};
use super::BufferPool;
use crate::io::Completion;
use crate::storage::atoll::aligned::AlignedBlock;
use crate::storage::atoll::once::SetOnce;
use crate::storage::atoll::page::SizeClass;
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::LimboError;
use crate::Result;

/// The most pages a change keeps copies of at once.
pub const MOST_PAGES: usize = 3;

/// The class of the image bank's frames: the largest node class (DESIGN.md 9.1.1). An image of a
/// smaller page takes a whole frame and writes its own length.
pub fn bank_class() -> SizeClass {
    SizeClass::new(crate::storage::atoll::btree::node::MAX_CLASS).expect("the largest node class")
}

/// The image bank's frames for `write_bytes` of image writes in flight (normalized to at least
/// the largest page): `S = min(WRITES, W / 4 KiB) + 1` (DESIGN.md 9.1.1), so the checkpoint
/// building one image while `WRITES` are in flight never waits on its own.
pub fn bank_frames(write_bytes: usize) -> u32 {
    let w = write_bytes.max(crate::storage::atoll::page::MAX_PAGE_SIZE);
    (crate::storage::atoll::checkpoint::IMAGE_WRITES
        .min(w / crate::storage::atoll::page::MIN_PAGE_SIZE)
        + 1) as u32
}

/// What a change wants for its copies: a frame of each page's class, at most [`MOST_PAGES`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Want {
    classes: [Option<SizeClass>; MOST_PAGES],
}

impl Want {
    /// One frame of `class` more.
    pub fn add(&mut self, class: SizeClass) {
        let free = self
            .classes
            .iter_mut()
            .find(|c| c.is_none())
            .expect("at most three copies a change");
        *free = Some(class);
    }

    fn iter(&self) -> impl Iterator<Item = SizeClass> + '_ {
        self.classes.iter().flatten().copied()
    }

    fn len(&self) -> usize {
        self.iter().count()
    }

    /// The one class every frame is of, if so: a request the fast path may take.
    fn one_class(&self) -> Option<SizeClass> {
        let first = self.classes[0]?;
        self.iter().all(|c| c == first).then_some(first)
    }

    /// The classes wanted, each once.
    fn distinct(&self) -> impl Iterator<Item = SizeClass> + '_ {
        self.iter()
            .enumerate()
            .filter(|&(i, c)| self.iter().take(i).all(|d| d != c))
            .map(|(_, c)| c)
    }
}

/// The frames a request was lent, in the order of its [`Want`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Lent {
    frames: [Option<(SizeClass, u32)>; MOST_PAGES],
}

impl Lent {
    fn push(&mut self, class: SizeClass, frame: u32) {
        let free = self
            .frames
            .iter_mut()
            .find(|f| f.is_none())
            .expect("at most three frames");
        *free = Some((class, frame));
    }

    pub(super) fn count_of(&self, class: SizeClass) -> usize {
        self.frames
            .iter()
            .flatten()
            .filter(|(c, _)| *c == class)
            .count()
    }

    /// Takes a frame of `class` out.
    pub fn take(&mut self, class: SizeClass) -> Option<u32> {
        let at = self
            .frames
            .iter()
            .position(|f| matches!(f, Some((c, _)) if *c == class))?;
        self.frames[at].take().map(|(_, f)| f)
    }

    fn drain(&mut self) -> impl Iterator<Item = (SizeClass, u32)> + '_ {
        self.frames.iter_mut().filter_map(Option::take)
    }
}

/// Why a request of the lend road went to the rare path, counted (DESIGN.md 9.1.1, Measured).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rare {
    /// More than one class, or none: never fast.
    Mixed,
    /// `slow` was set.
    Slow,
    /// No copy batch of the class showed `n` frames unpromised and one bit word with `n` free.
    NoWord,
    /// The add found the batch retired, of another class, or too full: an overshoot.
    Overshoot,
    /// The clear found a bit taken.
    LostClear,
}

/// Counts of the lend road, for the sustained benchmark (DESIGN.md 9.1.1, Measured).
#[derive(Default)]
struct Counts {
    fast: AtomicU64,
    rare: AtomicU64,
    why: [AtomicU64; 5],
    owner_lost: AtomicU64,
    planned_again: AtomicU64,
    turned_down: AtomicU64,
    provided: AtomicU64,
}

/// The lend road's state beside the pool's memory (DESIGN.md 9.1.1).
pub(super) struct LendRoad {
    /// Set while the rare path's owner plans and takes: every fast request then goes rare.
    pub(super) slow: AtomicBool,
    /// One bit per batch: the batches the rare path's owner lost during its ownership, which it
    /// never plans again in it. Cleared as an ownership begins; touched only by the token's holder.
    lost: Box<[AtomicU64]>,
    counts: Counts,
    /// The image bank's batches, taken as the pool opens, and its frames `S`.
    bank: SetOnce<Box<[u32]>>,
    bank_frames: AtomicU64,
    /// Images leased from the bank and not dropped: at most `S`. A lease dropped is a change of
    /// the supply.
    leases: AtomicU64,
}

impl LendRoad {
    pub(super) fn new(batches: u32) -> Self {
        Self {
            slow: AtomicBool::new(false),
            lost: (0..(batches as usize).div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect(),
            counts: Counts::default(),
            bank: SetOnce::default(),
            bank_frames: AtomicU64::new(0),
            leases: AtomicU64::new(0),
        }
    }

    fn lost(&self, batch: u32) -> bool {
        self.lost[batch as usize / 64].load(Ordering::Relaxed) & 1 << (batch % 64) != 0
    }

    fn lose(&self, batch: u32) {
        self.lost[batch as usize / 64].fetch_or(1 << (batch % 64), Ordering::Relaxed);
    }

    fn forget_lost(&self) {
        for word in &self.lost[..] {
            word.store(0, Ordering::Relaxed);
        }
    }
}

/// A request the lend road turned down, with nothing held: its writer lets go of its latches and
/// the change gate, then hands it to [`BufferPool::wait_for_room`], which takes its ticket on the
/// supply there, after every undo it made (DESIGN.md 9.1.3, rule 5).
pub struct Refused {
    pub(super) why: Why,
}

pub(super) enum Why {
    /// The room's bytes: as many as the change asked for.
    Room { bytes: usize },
    /// Frames.
    Frames {
        want: Want,
        /// The rare path's gate lost: its wait, taken before the try, all the request waits on
        /// (rule 4).
        gate: Option<Completion>,
        /// Short only of a free batch not yet quiet: the epoch it is quiet at.
        quiet_at: Option<u64>,
    },
}

/// What a refused writer waits for, which each exit of the relief it runs tests (DESIGN.md 9.1.1,
/// Turned down; 9.1.3, rule 5): its ticket on the supply, taken at `wait_for_room`'s boundary, and
/// what it lacks. An exit starts the change again only once the ticket moved and a read, taking
/// nothing, finds what it lacks there: the relief's own work (its writes landing, its images
/// freeing room) moves the ticket too, and must not end the relief before it gave the writer
/// anything.
#[derive(Clone, Copy)]
pub enum Retry<'a> {
    Room(&'a super::supply::Ticket),
    Frames {
        want: &'a Want,
        ticket: &'a super::supply::Ticket,
    },
    /// A change refused by the change gate a cut closed: it may start again once the gate
    /// reopens.
    Gate,
}

impl Retry<'_> {
    /// Whether a change of the supply came since the ticket.
    pub fn moved(&self) -> bool {
        match self {
            Retry::Room(ticket) | Retry::Frames { ticket, .. } => ticket.moved(),
            Retry::Gate => true,
        }
    }
}

impl std::fmt::Debug for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.why {
            Why::Room { bytes } => write!(f, "Refused(room {bytes})"),
            Why::Frames { want, .. } => write!(f, "Refused(frames {want:?})"),
        }
    }
}

/// A piece of a rare plan: a whole batch to take, or frames of a batch to take.
#[derive(Clone, Copy, Debug)]
enum Piece {
    /// A free batch outside the core, taken by its bit in the unowned set.
    Free(u32),
    /// A batch a provide left `LENDING`, claimed into `LENT`.
    Lending(u32),
    /// An empty copy batch of another class, retired and retyped.
    Retype(u32, SizeClass),
    /// `count` frames of `class` from copy batch `batch`, by the bits `mask` of its word `w`.
    Frames {
        batch: u32,
        class: SizeClass,
        w: usize,
        mask: u64,
    },
}

/// A complete or partial plan, in fixed storage: at most one whole batch and one frame piece for
/// each frame wanted.
#[derive(Default)]
struct Plan {
    pieces: [Option<Piece>; 2 * MOST_PAGES],
}

impl Plan {
    fn push(&mut self, piece: Piece) {
        let free = self
            .pieces
            .iter_mut()
            .find(|p| p.is_none())
            .expect("at most six pieces");
        *free = Some(piece);
    }

    fn pieces(&self) -> impl Iterator<Item = Piece> + '_ {
        self.pieces.iter().flatten().copied()
    }

    /// Frames already planned from `batch`, and the bits of word `w` they take.
    fn planned(&self, batch: u32) -> (u32, [u64; memory::WORDS]) {
        let mut n = 0;
        let mut bits = [0u64; memory::WORDS];
        for p in self.pieces() {
            if let Piece::Frames {
                batch: b, w, mask, ..
            } = p
            {
                if b == batch {
                    n += mask.count_ones();
                    bits[w] |= mask;
                }
            }
        }
        (n, bits)
    }

    /// Whether `batch` is taken whole by this plan.
    fn takes_whole(&self, batch: u32) -> bool {
        self.pieces().any(|p| match p {
            Piece::Free(b) | Piece::Lending(b) | Piece::Retype(b, _) => b == batch,
            Piece::Frames { .. } => false,
        })
    }
}

/// The batches a rare owner published in its ownership, at most one for each frame wanted and
/// each plan made again after a lost piece: fixed storage, each batch once.
#[derive(Default)]
struct Published {
    batches: [Option<(u32, SizeClass)>; 2 * MOST_PAGES],
}

impl Published {
    fn push(&mut self, batch: u32, class: SizeClass) {
        if self.batches.iter().flatten().any(|&(b, _)| b == batch) {
            return;
        }
        if let Some(free) = self.batches.iter_mut().find(|b| b.is_none()) {
            *free = Some((batch, class));
        }
    }

    fn drain(&mut self) -> impl Iterator<Item = (u32, SizeClass)> + '_ {
        self.batches.iter_mut().filter_map(Option::take)
    }
}

/// The lowest `n` set bits of `bits`, if it has them.
fn lowest(bits: u64, n: u32) -> Option<u64> {
    let mut mask = 0;
    let mut rest = bits;
    for _ in 0..n {
        if rest == 0 {
            return None;
        }
        let b = rest & rest.wrapping_neg();
        mask |= b;
        rest &= !b;
    }
    Some(mask)
}

/// Where a plan stopped short.
enum Short {
    /// No supply: the supply's next change is its wake.
    Supply,
    /// Short only of a free batch not yet quiet: the epoch it is quiet at.
    Quiet(u64),
}

impl BufferPool {
    /// Frames for the copies `want` names, from the lend road (DESIGN.md 9.1.1): the fast path
    /// for one class, else the rare path. Turned down, nothing is held and the request says what
    /// it registered on.
    pub(super) fn lend_copies(self: &Arc<Self>, want: &Want) -> Result<Lent, Refused> {
        let why = match want.one_class() {
            Some(class) => match self.lend_fast(class, want.len() as u32) {
                Ok(lent) => {
                    self.lend.counts.fast.fetch_add(1, Ordering::Relaxed);
                    return Ok(lent);
                }
                Err(why) => why,
            },
            None => Rare::Mixed,
        };
        self.lend.counts.rare.fetch_add(1, Ordering::Relaxed);
        self.lend.counts.why[why as usize].fetch_add(1, Ordering::Relaxed);
        self.lend_rare(want)
    }

    /// The fast path: a bounded sequence with no retry. Any loss sends the request to the rare
    /// path, never round again. What it gives back of a lost attempt (the count an overshoot or a
    /// lost clear added) is a change of the supply: it may have hidden frames from a look made
    /// meanwhile (DESIGN.md 9.1.3, rule 5).
    pub(super) fn lend_fast(&self, class: SizeClass, n: u32) -> Result<Lent, Rare> {
        if self.lend.slow.load(Ordering::SeqCst) {
            return Err(Rare::Slow);
        }
        // The fullest copy batch of the class with `n` frames unpromised and one bit word with `n`
        // free: one lend word and at most eight bit words read per batch.
        let fits = frames_per_batch(class);
        let mut best: Option<(u64, u32, usize, u64)> = None;
        for batch in self.memory.copy_batches(class) {
            let word = self.memory.lend_word(batch);
            if !word.is_copy_of(class) || word.count() + n as u64 > fits as u64 {
                continue;
            }
            if best.is_some_and(|(count, ..)| count >= word.count()) {
                continue;
            }
            let found = (0..memory::WORDS)
                .find_map(|w| lowest(self.memory.lent_bits(batch, w), n).map(|mask| (w, mask)));
            if let Some((w, mask)) = found {
                best = Some((word.count(), batch, w, mask));
            }
        }
        let Some((_, batch, w, mask)) = best else {
            return Err(Rare::NoWord);
        };
        crate::race::point("lend.fast_chosen");
        if self.memory.promise_lent(batch, class, n).is_err() {
            // The add stood for a moment before it was taken back.
            self.signal_released();
            return Err(Rare::Overshoot);
        }
        if !self.memory.take_lent_bits(batch, w, mask) {
            self.frames.supply.change(|event| {
                let after = self.memory.unpromise_lent(batch, n);
                event.arm();
                if after.count() == 0 {
                    self.retire_if_no_cut(batch, class);
                }
            });
            return Err(Rare::LostClear);
        }
        crate::race::point("lend.fast_taken");
        let mut lent = Lent::default();
        for_bits(mask, |b| {
            lent.push(class, memory::frame_at(class, batch, w as u32 * 64 + b))
        });
        Ok(lent)
    }

    /// The rare path: one owner at a time by the gate; a complete placement planned before any
    /// piece is taken; a piece lost keeps what was taken and plans the rest again, never a batch
    /// lost in this ownership; short, everything taken is given back (each a change of the supply)
    /// and the request is turned down. The gate lost, the request waits on the gate's wait alone,
    /// taken before the try (DESIGN.md 9.1.3, rule 4).
    pub(super) fn lend_rare(self: &Arc<Self>, want: &Want) -> Result<Lent, Refused> {
        let _step = self.store.epochs().enter();
        let gate = self.frames.supply.gate_wait();
        if !self.memory.take_token(LEND_TOKEN) {
            self.lend.counts.owner_lost.fetch_add(1, Ordering::Relaxed);
            return Err(self.turned_down(want, Some(gate), None));
        }
        drop(gate);
        self.frames.supply.change(|_| self.lend_owned(want))
    }

    /// The rare path's work, holding the gate, inside one supply scope: every frame given back
    /// and every batch published or retired wakes its waiters once the scope closes, the gate let go
    /// before.
    fn lend_owned(self: &Arc<Self>, want: &Want) -> Result<Lent, Refused> {
        self.lend.slow.store(true, Ordering::SeqCst);
        self.lend.forget_lost();
        let mut lent = Lent::default();
        let mut published = Published::default();
        let outcome = loop {
            match self.lend_plan(want, &lent, true) {
                Ok(plan) => {
                    if self.lend_take(&plan, &mut lent, &mut published) {
                        break Ok(());
                    }
                    self.lend
                        .counts
                        .planned_again
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(short) => break Err(short),
            }
        };
        let result = match outcome {
            Ok(()) => Ok(lent),
            Err(short) => {
                for (class, frame) in lent.drain() {
                    self.give_back_lent(class, frame);
                }
                Err(match short {
                    Short::Quiet(at) => Some(at),
                    Short::Supply => None,
                })
            }
        };
        // A batch this owner published and left empty, a frame piece of it lost or every frame of
        // it given back, with no cut in effect, is retired: the owner's publication, a return and
        // `end_cut`'s sweep cannot all miss one another.
        for (batch, class) in published.drain() {
            if self.memory.lend_word(batch).count() == 0 {
                self.retire_if_no_cut(batch, class);
            }
        }
        self.lend.slow.store(false, Ordering::SeqCst);
        self.let_token_go(LEND_TOKEN);
        result.map_err(|quiet_at| self.turned_down(want, None, quiet_at))
    }

    fn turned_down(&self, want: &Want, gate: Option<Completion>, quiet_at: Option<u64>) -> Refused {
        self.lend.counts.turned_down.fetch_add(1, Ordering::Relaxed);
        Refused {
            why: Why::Frames {
                want: *want,
                gate,
                quiet_at,
            },
        }
    }

    /// A complete placement of what `want` still lacks beside `lent`, in the order of supply:
    /// non-empty copy batches of the class, fullest first, a group split across them; then an
    /// empty copy batch of another class, retyped; then a batch a provide left `LENDING`; then a
    /// free batch outside the core, quiet. Batches lost in this ownership are never planned.
    fn lend_plan(&self, want: &Want, lent: &Lent, owner: bool) -> Result<Plan, Short> {
        let mut plan = Plan::default();
        let mut not_quiet: Option<u64> = None;
        for class in want.distinct() {
            let mut need =
                (want.iter().filter(|&c| c == class).count() - lent.count_of(class)) as u32;
            let fits = frames_per_batch(class);
            // Copy batches of the class, fullest first: the fullest with room each time, one
            // frame piece per batch chosen, at most `need` choices.
            while need > 0 {
                let mut best: Option<(u64, u32, u32)> = None;
                for batch in self.memory.copy_batches(class) {
                    if owner && self.lend.lost(batch) {
                        continue;
                    }
                    let word = self.memory.lend_word(batch);
                    if !word.is_copy_of(class) {
                        continue;
                    }
                    let (planned, _) = plan.planned(batch);
                    let room = (fits as u64)
                        .saturating_sub(word.count() + planned as u64)
                        .min(self.free_bits(batch, &plan) as u64)
                        as u32;
                    if room == 0 || best.is_some_and(|(count, ..)| count >= word.count()) {
                        continue;
                    }
                    best = Some((word.count(), batch, room));
                }
                let Some((_, batch, room)) = best else {
                    break;
                };
                if owner {
                    crate::race::point("lend.owner_plans_frames");
                }
                let got = self.plan_frames(&mut plan, batch, class, need.min(room));
                if got == 0 {
                    // Bits read free were taken between the plan's reads of them, by a fast
                    // request: one that read `slow` clear before the owner set it can still be
                    // taking (`slow` keeps new requests out, never waits for those already past
                    // it). Another's success either way, and never read again: the owner counts
                    // the batch lost for its ownership and plans on, one batch at most once; the
                    // hint, which keeps no lost set, says yes, one attempt after another's
                    // success being bounded by it (DESIGN.md 9.1.3, rule 5).
                    if !owner {
                        return Ok(plan);
                    }
                    self.lend.lose(batch);
                    continue;
                }
                need -= got;
            }
            if need == 0 {
                continue;
            }
            // A whole batch for the rest: an empty copy batch of another class, retyped, then a
            // provided batch, then a free one.
            let whole = self
                .empty_copy_batch(class, &plan, owner)
                .map(|(b, from)| Piece::Retype(b, from))
                .or_else(|| match self.lending_batch(&plan, owner) {
                    Ok(b) => Some(Piece::Lending(b)),
                    Err(Some(at)) => {
                        not_quiet = Some(not_quiet.map_or(at, |n| n.min(at)));
                        None
                    }
                    Err(None) => None,
                })
                .or_else(|| match self.free_batch(&plan, owner) {
                    Ok(b) => Some(Piece::Free(b)),
                    Err(Some(at)) => {
                        not_quiet = Some(not_quiet.map_or(at, |n| n.min(at)));
                        None
                    }
                    Err(None) => None,
                });
            let Some(piece) = whole else {
                return Err(match not_quiet {
                    Some(at) => Short::Quiet(at),
                    None => Short::Supply,
                });
            };
            plan.push(piece);
            let batch = match piece {
                Piece::Free(b) | Piece::Lending(b) | Piece::Retype(b, _) => b,
                Piece::Frames { .. } => unreachable!(),
            };
            // A whole batch holds every frame a change wants: three of 64 KiB at most, and a
            // batch at least 32 of any node class.
            let got = self.plan_frames_whole(&mut plan, batch, class, need);
            debug_assert_eq!(got, need);
        }
        Ok(plan)
    }

    /// Free frames in `batch`'s bits less those `plan` takes.
    fn free_bits(&self, batch: u32, plan: &Plan) -> u32 {
        let (_, taken) = plan.planned(batch);
        (0..memory::WORDS)
            .map(|w| (self.memory.lent_bits(batch, w) & !taken[w]).count_ones())
            .sum()
    }

    /// Plans up to `n` frames of copy batch `batch`, from the bits as read: how many.
    fn plan_frames(&self, plan: &mut Plan, batch: u32, class: SizeClass, n: u32) -> u32 {
        let (_, taken) = plan.planned(batch);
        let mut left = n;
        for (w, held) in taken.into_iter().enumerate() {
            if left == 0 {
                break;
            }
            let free = self.memory.lent_bits(batch, w) & !held;
            let k = left.min(free.count_ones());
            if k == 0 {
                continue;
            }
            let mask = lowest(free, k).expect("k bits");
            plan.push(Piece::Frames {
                batch,
                class,
                w,
                mask,
            });
            left -= k;
        }
        n - left
    }

    /// Plans `n` frames of a batch the plan takes whole for `class`: its first frames.
    fn plan_frames_whole(&self, plan: &mut Plan, batch: u32, class: SizeClass, n: u32) -> u32 {
        let k = n.min(frames_per_batch(class)).min(64);
        plan.push(Piece::Frames {
            batch,
            class,
            w: 0,
            mask: lowest(u64::MAX, k).expect("k bits"),
        });
        k
    }

    /// An empty copy batch of a class other than `class`, not lost and not planned.
    fn empty_copy_batch(
        &self,
        class: SizeClass,
        plan: &Plan,
        owner: bool,
    ) -> Option<(u32, SizeClass)> {
        SizeClass::all()
            .filter(|&c| c != class)
            .flat_map(|c| self.memory.copy_batches(c).map(move |b| (b, c)))
            .find(|&(b, c)| {
                let word = self.memory.lend_word(b);
                (!owner || !self.lend.lost(b))
                    && !plan.takes_whole(b)
                    && word.is_copy_of(c)
                    && word.count() == 0
            })
    }

    /// A batch a provide left `LENDING`, quiet, outside the reserved run, not lost; else the epoch
    /// the earliest such batch not yet quiet is quiet at, if any.
    fn lending_batch(&self, plan: &Plan, owner: bool) -> Result<u32, Option<u64>> {
        let horizon = self.store.epochs().horizon();
        let mut earliest = None;
        for b in (CORE..self.memory.batches).rev() {
            if (owner && self.lend.lost(b))
                || plan.takes_whole(b)
                || !self.memory.is_lending(b)
                || self.memory.reserved_covers(b)
            {
                continue;
            }
            let at = self.batch_quiet_at(b);
            if at <= horizon {
                return Ok(b);
            }
            earliest = Some(earliest.map_or(at, |e: u64| e.min(at)));
        }
        Err(earliest)
    }

    /// Whether what `want` lacks reads as there now (DESIGN.md 9.1.3, rule 5): the rare plan's own
    /// placement, made from shared availability alone, outside the gate and taking nothing, the
    /// owner's set of batches lost in its ownership not read (it is cleared only by the next
    /// owner, under the gate). It is the plan itself, not a second reckoning, because with nothing
    /// moving the two must agree: a yes the plan refuses restarts the change for ever, and a no the
    /// plan would grant strands the writer. Bounded as the plan is: at most `MOST_PAGES` frame
    /// choices a class, each a pass over the class's copy batches, and one whole batch a class; a
    /// frame piece lost to a fast request between its reads ends it with a yes (another's success
    /// moved the supply). No allocation.
    pub(super) fn lend_hint(&self, want: &Want) -> bool {
        self.lend_plan(want, &Lent::default(), false).is_ok()
    }

    /// The provide (DESIGN.md 9.1.1): a turned-down writer whose generations have not moved drains
    /// the cache batch of the highest number outside the core for the lend road, or drives the one
    /// already draining for it, and waits on its writes and loads; the batch's release publishes
    /// it `LENDING`, which signals every class. None if nothing outside the core is a cache batch:
    /// the writer then runs checkpoint units or parks.
    pub(super) fn provide(self: &Arc<Self>) -> Result<Option<IOResult<()>>> {
        let (class, batch) = match self.memory.draining_for_lend() {
            Some(draining) => draining,
            None => {
                let Some((class, batch)) = self.memory.highest_cache_batch_outside_core() else {
                    return Ok(None);
                };
                if !self.memory.drain_for_lend(class, batch) {
                    // Changed hands, or draining already for another: look again on that.
                    return Ok(Some(IOResult::IO(IOCompletions(Completion::new_yield()))));
                }
                self.lend.counts.provided.fetch_add(1, Ordering::Relaxed);
                if !self.memory.is_draining(batch) {
                    // Released at once, `LENDING` or given back free: a change of the supply, made
                    // by the drain's release itself.
                    return Ok(Some(IOResult::Done(())));
                }
                (class, batch)
            }
        };
        self.drain(class, batch)?;
        if let Some(c) = self.drive_writebacks()? {
            return Ok(Some(IOResult::IO(IOCompletions(c))));
        }
        Ok(None)
    }

    /// A free batch outside the core, quiet, not lost; else the epoch the earliest free one is
    /// quiet at, if any is free.
    fn free_batch(&self, plan: &Plan, owner: bool) -> Result<u32, Option<u64>> {
        let horizon = self.store.epochs().horizon();
        let mut earliest = None;
        for b in self.memory.free_batches_outside_core() {
            if (owner && self.lend.lost(b)) || plan.takes_whole(b) {
                continue;
            }
            let at = self.batch_quiet_at(b);
            if at <= horizon {
                return Ok(b);
            }
            earliest = Some(earliest.map_or(at, |e: u64| e.min(at)));
        }
        Err(earliest)
    }

    /// The epoch the horizon must reach for every grain of `batch` to be quiet: no step can
    /// still read a page that was in it.
    fn batch_quiet_at(&self, batch: u32) -> u64 {
        let start = (batch * GRAINS) as usize;
        self.frames.given_up[start..start + GRAINS as usize]
            .iter()
            .map(|g| g.load(Ordering::Acquire))
            .max()
            .unwrap_or(0)
    }

    /// Takes `plan`'s pieces in order, whole batches first, then the frames; the frames taken go
    /// to `lent`. False if a piece was lost: what was taken stays the owner's, the batch is lost
    /// for the rest of the ownership, and the owner plans the rest again.
    fn lend_take(&self, plan: &Plan, lent: &mut Lent, published: &mut Published) -> bool {
        for piece in plan.pieces() {
            let batch = match piece {
                Piece::Free(b) => {
                    if !self.memory.take_free_batch(b) {
                        self.lend.lose(b);
                        return false;
                    }
                    b
                }
                Piece::Lending(b) => {
                    if !self.memory.claim_lending_lent(b) {
                        self.lend.lose(b);
                        return false;
                    }
                    b
                }
                Piece::Retype(b, from) => {
                    if !self.memory.retire(b, from) {
                        self.lend.lose(b);
                        return false;
                    }
                    b
                }
                Piece::Frames { .. } => continue,
            };
            // Whose class is the frame piece that follows the whole batch's.
            let for_class = plan
                .pieces()
                .find_map(|p| match p {
                    Piece::Frames {
                        batch: b, class, ..
                    } if b == batch => Some(class),
                    _ => None,
                })
                .expect("a whole batch is taken for frames");
            self.memory.publish_lent(batch, LendKind::Copy, for_class);
            published.push(batch, for_class);
            self.signal_released();
        }
        for piece in plan.pieces() {
            let Piece::Frames {
                batch,
                class,
                w,
                mask,
            } = piece
            else {
                continue;
            };
            let n = mask.count_ones();
            crate::race::point("lend.take_frames");
            if self.memory.promise_lent(batch, class, n).is_err() {
                self.lend.lose(batch);
                return false;
            }
            if !self.memory.take_lent_bits(batch, w, mask) {
                // An undo, inside the owner's scope: a change of the supply (rule 5).
                self.frames.supply.change(|event| {
                    self.memory.unpromise_lent(batch, n);
                    event.arm();
                });
                self.lend.lose(batch);
                return false;
            }
            for_bits(mask, |b| {
                lent.push(class, memory::frame_at(class, batch, w as u32 * 64 + b))
            });
        }
        true
    }

    /// A frame of a copy batch handed out as a block for a kept copy: its header reads `LENT`, so
    /// a hint to the page it last held fails before any byte is read.
    pub(super) fn lent_block(self: &Arc<Self>, class: SizeClass, frame: u32) -> AlignedBlock {
        self.lent_frame_block(frame, class, class.size(), super::LentUse::Kept)
    }

    /// A lent frame of a lend batch given back: its bit, then its count, in a change scope of the
    /// supply: a return, a lease of the image bank dropped and a frame a short plan gives back are
    /// all changes (DESIGN.md 9.1.3, rule 5). A copy batch's count reaching zero is retired too.
    pub(super) fn give_back_lent(&self, class: SizeClass, frame: u32) {
        let batch = frame / GRAINS;
        self.frames.supply.change(|event| {
            self.header(frame)
                .status
                .store(super::frame::FREE, Ordering::Release);
            let word = self.memory.return_lent(class, batch, frame);
            event.arm();
            if word.kind() == LendKind::Bank {
                self.lend.leases.fetch_sub(1, Ordering::SeqCst);
                // What a lease's wait, stepped, waits on: a lease held across a return is held by
                // an image write at the backend, a writer waiting on its copy slot giving its
                // lease back first, so every give-back, as its write lands or before a write, is
                // what that wait needs ([`lease_image`](BufferPool::lease_image)); a claim's
                // stepped wait it finishes too costs that claim one look.
                event.land();
                return;
            }
            if word.count() == 0 && word.kind() == LendKind::Copy {
                self.retire_if_no_cut(batch, class);
            }
        });
    }

    /// Retires empty copy batch `batch` of `class` and gives it back free, if no cut is in effect:
    /// tried once, by whoever took its count to zero. A change of the supply.
    fn retire_if_no_cut(&self, batch: u32, class: SizeClass) {
        self.frames.supply.change(|event| {
            if self.memory.cutting.load(Ordering::SeqCst) == 0 && self.memory.retire(batch, class) {
                self.memory.free_retired(batch);
                event.arm();
            }
        });
    }

    /// `end_cut`'s sweep of the lend words outside the core, after the cut is cleared: every
    /// batch a provide left `LENDING` given back free, and every empty copy batch retired and
    /// given back free, each by the one compare-and-swap its other roads take it by. `end_cut`
    /// signals every class after it.
    pub(super) fn sweep_lend_batches(&self) {
        for batch in CORE..self.memory.batches {
            let word = self.memory.lend_word(batch);
            match word.kind() {
                LendKind::Lending => {
                    if self.memory.claim_lending(batch) {
                        self.memory.unlend(batch);
                    }
                }
                LendKind::Copy if word.count() == 0 => {
                    let class = word.class().expect("a copy batch's class");
                    if self.memory.retire(batch, class) {
                        self.memory.free_retired(batch);
                    }
                }
                _ => {}
            }
        }
    }

    /// Takes the lend batches the pool lends for as long as it is open, from the highest free
    /// batches outside the core, before anything is cached (DESIGN.md 9.1.1): the log's ring and
    /// the image bank. Refused if the budget is below its minimum.
    pub(super) fn lend_at_open(self: &Arc<Self>, at_open: super::LentAtOpen) -> crate::Result<()> {
        if let Some((class, chunks)) = at_open.ring {
            let per = frames_per_batch(class);
            for _ in 0..chunks.div_ceil(per) {
                self.take_lend_batch(LendKind::Ring, class)?;
            }
        }
        if at_open.bank > 0 {
            let class = bank_class();
            let batches = (0..at_open.bank.div_ceil(frames_per_batch(class)))
                .map(|_| self.take_lend_batch(LendKind::Bank, class))
                .collect::<crate::Result<Box<[u32]>>>()?;
            self.lend.bank.set(batches);
            self.lend
                .bank_frames
                .store(at_open.bank as u64, Ordering::Release);
        }
        Ok(())
    }

    /// A free batch outside the core, quiet, published a lend batch of `kind` and `class`,
    /// holding the rare path's token, as every publication of `LENT` does.
    fn take_lend_batch(self: &Arc<Self>, kind: LendKind, class: SizeClass) -> crate::Result<u32> {
        if !self.memory.take_token(LEND_TOKEN) {
            return Err(LimboError::InternalError(
                "the rare path's token is held while the pool opens".into(),
            ));
        }
        let horizon = self.store.epochs().horizon();
        let taken = self
            .memory
            .free_batches_outside_core()
            .find(|&b| self.batch_quiet_at(b) <= horizon && self.memory.take_free_batch(b));
        if let Some(batch) = taken {
            self.memory.publish_lent(batch, kind, class);
        }
        self.let_token_go(LEND_TOKEN);
        taken.ok_or_else(|| {
            LimboError::InternalError(format!(
                "no free batch outside the core for the {kind:?}: the budget is below its minimum"
            ))
        })
    }

    /// A frame of the log's ring, of `class`, from the ring's lend batches, lent for as long as the
    /// log is open.
    pub(crate) fn lend_ring(self: &Arc<Self>, class: SizeClass) -> crate::Result<AlignedBlock> {
        let frame = (CORE..self.memory.batches)
            .rev()
            .find_map(|b| self.memory.lend_one(b, LendKind::Ring, class));
        let frame = match frame {
            Some(frame) => frame,
            None => {
                let batch = self.take_lend_batch(LendKind::Ring, class)?;
                self.memory
                    .lend_one(batch, LendKind::Ring, class)
                    .expect("a ring batch just taken has a frame")
            }
        };
        Ok(self.lent_frame_block(frame, class, class.size(), super::LentUse::Log))
    }

    /// A frame of the image bank leased for the image of a page of `class` (DESIGN.md 9.1.1): the
    /// lease ends when its block is dropped by its last owner. With all `S` leased, the
    /// completion a lease's drop finishes, taken before the count.
    pub(crate) fn lease_image(
        self: &Arc<Self>,
        class: SizeClass,
    ) -> crate::Result<IOResult<AlignedBlock>> {
        let _step = self.store.epochs().enter();
        // Taken before the count: a lease dropped after it finishes it. Of the stepped kind, as
        // the bank's leases are given back as image writes land, which may be this thread's IO.
        let ticket = self.frames.supply.ticket();
        let limit = self.lend.bank_frames.load(Ordering::Acquire);
        if self.lend.leases.fetch_add(1, Ordering::SeqCst) >= limit {
            self.lend.leases.fetch_sub(1, Ordering::SeqCst);
            return Ok(IOResult::IO(IOCompletions(ticket.stepped())));
        }
        drop(ticket);
        let bank = bank_class();
        let frame = self
            .lend
            .bank
            .get_ref()
            .into_iter()
            .flat_map(|batches| batches.iter())
            .find_map(|&b| self.memory.lend_one(b, LendKind::Bank, bank));
        match frame {
            Some(frame) => Ok(IOResult::Done(self.lent_frame_block(
                frame,
                bank,
                class.size(),
                super::LentUse::Image,
            ))),
            None => {
                self.lend.leases.fetch_sub(1, Ordering::SeqCst);
                Err(LimboError::InternalError(format!(
                    "the image bank holds fewer frames than its {limit} leases"
                )))
            }
        }
    }

    /// `frame`, of a lend batch of `class`, as a block of `len` bytes lent for `what`.
    fn lent_frame_block(
        self: &Arc<Self>,
        frame: u32,
        class: SizeClass,
        len: usize,
        what: super::LentUse,
    ) -> AlignedBlock {
        let h = self.header(frame);
        let hold = self.take_free_latch(frame);
        h.pid.store(NO_PAGE, Ordering::Release);
        h.flags.store(0, Ordering::Release);
        h.status.store(LENT, Ordering::Release);
        drop(hold);
        let token = self.lends.lent(frame, class, what);
        let ptr =
            std::ptr::NonNull::new(self.memory.region.ptr(frame)).expect("the region's memory");
        // SAFETY: a frame of the region, which the lender keeps mapped; frames are aligned to
        // 4 KiB and `len` is at most the frame's class; and a frame of a lend batch is no one
        // else's until its block gives it back.
        unsafe { AlignedBlock::lent(ptr, len, self.lends.clone(), token) }
    }

    /// Grows `class` into every free batch, so no free batch is left for the lend road: for a
    /// test of the provide. How many batches it took.
    #[cfg(test)]
    pub fn hold_free_batches(&self, class: SizeClass) -> usize {
        let mut n = 0;
        while self.memory.grow(class) {
            n += 1;
        }
        n
    }

    /// The lend road's counts, for a test or the sustained benchmark.
    #[cfg(test)]
    pub fn lend_counts(&self) -> LendCounts {
        let c = &self.lend.counts;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        LendCounts {
            fast: get(&c.fast),
            rare: get(&c.rare),
            slow: get(&c.why[Rare::Slow as usize]),
            no_word: get(&c.why[Rare::NoWord as usize]),
            overshoot: get(&c.why[Rare::Overshoot as usize]),
            lost_clear: get(&c.why[Rare::LostClear as usize]),
            mixed: get(&c.why[Rare::Mixed as usize]),
            owner_lost: get(&c.owner_lost),
            planned_again: get(&c.planned_again),
            turned_down: get(&c.turned_down),
            provided: get(&c.provided),
        }
    }
}

/// The lend road's counts (DESIGN.md 9.1.1, Measured).
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LendCounts {
    pub fast: u64,
    pub rare: u64,
    pub slow: u64,
    pub no_word: u64,
    pub overshoot: u64,
    pub lost_clear: u64,
    pub mixed: u64,
    pub owner_lost: u64,
    pub planned_again: u64,
    pub turned_down: u64,
    pub provided: u64,
}

fn frames_per_batch(class: SizeClass) -> u32 {
    memory::frames_per_batch(class)
}

fn for_bits(mut mask: u64, mut f: impl FnMut(u32)) {
    while mask != 0 {
        let b = mask.trailing_zeros();
        mask &= mask - 1;
        f(b);
    }
}
