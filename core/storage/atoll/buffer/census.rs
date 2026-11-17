//! The census of taken frames and the park report (SHORT-HOLD1), for tests and the simulator. Each
//! is called where no operation of the pool runs, or from another thread while the one that runs
//! them waits. Each reads the pool's atomics and takes the shortage queue's mutex alone, for as long
//! as it copies the places out, as every other taker does (CONCURRENCY.md §8, *The one lock
//! order*); neither takes a latch, and neither formats under a lock.
//!
//! A taken frame is a frame of a batch a class or a lend holds whose free bit is clear. Each is in
//! exactly one of these places:
//!
//! 1. the page table, which finds the frame for the page its header names: resident, loading or
//!    outgoing. A load's group may keep it too ([`super::group`]), which is noted, not a place;
//! 2. the shortage queue: `R` of a live place, or the frame handed to the gate holder's demand;
//! 3. the lend road: a frame of a lend batch, lent for a kept copy, an image or the log's ring;
//! 4. deferred: freed while its record held a request or its load's group kept it, and given to the
//!    free frames once neither does ([`super::frame::FrameHeader::free_deferred`]);
//! 5. the checkpoint's memory demand ([`super::upkeep`]): the frame handed to it, or adopted and
//!    kept until its claim takes it, which may be steps later while the frame is not yet quiet for
//!    a read. The demand lives in the checkpointer's state, not the pool's, so the registry of
//!    keepers names that frame ([`super::keepers`]).

use super::frame::{self, FREE, LENT, LOADING, NO_PAGE, OUTGOING, RESIDENT};
use super::keepers::{Entry, Keeper};
use super::memory::{self, Beneficiary};
use super::shortage::PlaceSeen;
use super::{group, BufferPool, PageId, Purpose, SizeClass};
use crate::sync::atomic::Ordering;
use std::collections::HashMap;

/// A place a taken frame is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Place {
    Table,
    /// `R` of the shortage queue's place in this slot.
    Reserved(usize),
    /// Handed to the shortage gate holder's demand.
    Handed,
    Lend,
    Deferred,
    /// Handed to the checkpoint's demand.
    CheckpointHanded,
    /// Adopted by the checkpoint's demand and kept for its claim.
    CheckpointKept,
}

impl std::fmt::Display for Place {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Place::Table => write!(f, "the page table"),
            Place::Reserved(slot) => write!(f, "R of the shortage place in slot {slot}"),
            Place::Handed => write!(f, "handed to the gate holder's demand"),
            Place::Lend => write!(f, "the lend road"),
            Place::Deferred => write!(f, "deferred"),
            Place::CheckpointHanded => write!(f, "handed to the checkpoint's demand"),
            Place::CheckpointKept => write!(f, "kept for the checkpoint's demand"),
        }
    }
}

impl BufferPool {
    /// Every frame that is taken and in no place or in more than one, every free frame a place
    /// names, and every frame a place names that is in no batch a class or a lend holds, each with
    /// its header's words; and a line if the registry of keepers missed any. Empty when every taken
    /// frame is in exactly one place.
    pub fn taken_frames_census(&self) -> Vec<String> {
        let mut named = self.frames_held_for_claims();
        let entries = self.keepers.entries();
        let mut out = Vec::new();
        for seen in self.memory.seen_frames() {
            let mut places = named.remove(&seen.frame).unwrap_or_default();
            let h = self.header(seen.frame);
            let pid = h.pid();
            if pid != NO_PAGE && self.table.find(&self.frames.headers, pid) == Some(seen.frame) {
                places.push(Place::Table);
            }
            if seen.lend && seen.taken {
                places.push(Place::Lend);
            }
            if frame::record_freed(h.record_word()) {
                places.push(Place::Deferred);
            }
            let wrong = match (seen.taken, places.len()) {
                (true, 1) | (false, 0) => continue,
                (true, 0) => "taken and in no place",
                (true, _) => "taken and in more than one place",
                (false, _) => "free and in a place",
            };
            out.push(format!(
                "frame {} of class {}: {wrong} [{}]; {}",
                seen.frame,
                seen.class.index(),
                listed(&places),
                self.frame_words(seen.frame, &entries)
            ));
        }
        let mut stray: Vec<_> = named.into_iter().collect();
        stray.sort_unstable_by_key(|(frame, _)| *frame);
        for (frame, places) in stray {
            out.push(format!(
                "frame {frame}: in no batch a class or a lend holds, and in a place [{}]; {}",
                listed(&places),
                self.frame_words(frame, &entries)
            ));
        }
        let missed = self.keepers.missed();
        if missed > 0 {
            out.push(format!(
                "the registry of keepers had no slot for {missed} keepers: a frame kept for the \
                 checkpoint's demand is named in no place"
            ));
        }
        out
    }

    /// What a claim parked on memory waits behind: every batch draining or released and each
    /// frame still taken in it, with what holds it; the shortage queue's places, head first; what
    /// each demand was handed; and the memory's cut, rare path and reserved run.
    pub fn park_report(&self) -> String {
        let places = self.shortage.places();
        let entries = self.keepers.entries();
        let mut out = format!(
            "buffer pool: {} batches of 2 MiB, failure {:?}\n",
            self.memory.batches,
            self.failure.get()
        );
        let leaving = self.memory.leaving();
        if leaving.is_empty() {
            out.push_str("leaving batches: none\n");
        }
        for batch in leaving {
            let class = batch.holder;
            let state = match (batch.draining, batch.releasing) {
                (true, true) => "draining, released",
                (true, false) => "draining",
                _ => "released",
            };
            let to = match batch.beneficiary {
                Some(Beneficiary::Class(to)) => format!("class {}", to.index()),
                Some(Beneficiary::LendRoad) => "the lend road".to_string(),
                None => "the free runs".to_string(),
            };
            let per = memory::frames_per_batch(class);
            out.push_str(&format!(
                "batch {} of class {} ({} KiB frames): {state} for {to}, {} of {per} frames \
                 taken, state word {:#x}\n",
                batch.batch,
                class.index(),
                class.size() / 1024,
                batch.taken,
                batch.word
            ));
            for i in 0..per {
                let frame = memory::frame_at(class, batch.batch, i);
                if self.memory.is_free(class, frame) {
                    continue;
                }
                out.push_str(&format!(
                    "  frame {frame}: {}; R of a place: {}\n",
                    self.frame_words(frame, &entries),
                    reserving(frame, &places)
                ));
            }
        }
        out.push_str(&format!(
            "shortage queue, head first: {} places\n",
            places.len()
        ));
        for place in &places {
            out.push_str(&format!(
                "  place in slot {} (generation {}): {}, {}, connection {:#x}, demand {}, \
                 reserved {}, published {}\n",
                place.slot,
                place.generation,
                if place.holds_gate {
                    "holds the gate"
                } else {
                    "waits its turn"
                },
                if place.running {
                    "running"
                } else {
                    "suspended"
                },
                place.conn,
                place
                    .demand
                    .map_or("none".to_string(), |(c, p)| claim_for(c, p)),
                place.reserved.map_or("none".to_string(), |(c, p, frame)| {
                    format!("frame {frame} for {}", claim_for(c, p))
                }),
                place.published
            ));
        }
        let supply = &self.frames.supply;
        for (name, handoff) in [
            ("the gate holder", &*supply.handoff),
            ("the checkpoint", &supply.upkeep),
        ] {
            let (wanted, handed) = handoff.seen();
            out.push_str(&format!(
                "demand of {name}: {}, handed {}\n",
                wanted.map_or("none".to_string(), |c| format!("class {}", c.index())),
                handed.map_or("none".to_string(), |frame| format!("frame {frame}"))
            ));
        }
        let rare = match self.memory.rare_token() {
            memory::FREE_TOKEN => "free".to_string(),
            memory::LEND_TOKEN => "held by the lend road".to_string(),
            memory::RUN_TOKEN => "held by a run's drain".to_string(),
            other => format!("held by {other}"),
        };
        let reserved = self.memory.reserved_run().map_or("none".to_string(), |r| {
            format!(
                "batches {}..{} for class {}, {} missing",
                r.start,
                r.start + r.batches,
                r.class.index(),
                r.missing
            )
        });
        out.push_str(&format!(
            "memory: cutting {}, rare path {rare}, reserved run {reserved}\n",
            self.memory.cutting.load(Ordering::Acquire)
        ));
        out.push_str(&format!(
            "registry of keepers: {} entries, {} missed\n",
            entries.len(),
            self.keepers.missed()
        ));
        out
    }

    /// The frames held for a claim that has yet to take them (places 2 and 5), by frame.
    fn frames_held_for_claims(&self) -> HashMap<u32, Vec<Place>> {
        let mut named: HashMap<u32, Vec<Place>> = HashMap::new();
        for place in self.shortage.places() {
            if let Some((_, _, frame)) = place.reserved {
                named
                    .entry(frame)
                    .or_default()
                    .push(Place::Reserved(place.slot));
            }
        }
        let supply = &self.frames.supply;
        if let (_, Some(frame)) = supply.handoff.seen() {
            named.entry(frame).or_default().push(Place::Handed);
        }
        if let (_, Some(frame)) = supply.upkeep.seen() {
            named
                .entry(frame)
                .or_default()
                .push(Place::CheckpointHanded);
        }
        for entry in self.keepers.entries() {
            if entry.keeper == Keeper::Checkpoint {
                named
                    .entry(entry.frame)
                    .or_default()
                    .push(Place::CheckpointKept);
            }
        }
        named
    }

    /// What `frame`'s header and words show: its page, status, latch, record and group, and who
    /// the registry of keepers says keeps it.
    fn frame_words(&self, frame: u32, entries: &[Entry]) -> String {
        let h = self.header(frame);
        let pid = h.pid();
        let page = if pid == NO_PAGE {
            "no page".to_string()
        } else {
            let p = PageId::from_raw(pid);
            format!(
                "page {pid:#x} (class {}, number {}{})",
                p.class().index(),
                p.number(),
                if p.is_temporary() { ", temporary" } else { "" }
            )
        };
        let status = match h.status() {
            FREE => "free".to_string(),
            LOADING => "loading".to_string(),
            RESIDENT => "resident".to_string(),
            OUTGOING => "outgoing".to_string(),
            LENT => "lent".to_string(),
            other => format!("status {other}"),
        };
        let record = h.record_word();
        let word = self.group(frame).word();
        let keepers: Vec<String> = entries
            .iter()
            .filter(|e| e.frame == frame)
            .map(|e| match e.keeper {
                Keeper::Owner { conn, serial, at } => format!(
                    "owner #{serial} (connection {conn:#x}, at {at:#x}, page {:#x})",
                    e.pid
                ),
                Keeper::Checkpoint => "the checkpoint's demand".to_string(),
            })
            .collect();
        format!(
            "{page}, {status}, {}, flags {:#x}, latch {:#x}, record {record:#x} ({}), group \
             {word:#x} ({}, references {}), keepers [{}]",
            if h.dirty() { "dirty" } else { "clean" },
            h.flags.load(Ordering::Acquire),
            h.latch.word(),
            frame::record_marks(record),
            if group::word_open(word) {
                "open"
            } else {
                "closed"
            },
            group::word_references(word),
            keepers.join(", ")
        )
    }
}

/// `places`, by name.
fn listed(places: &[Place]) -> String {
    places
        .iter()
        .map(Place::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The places whose `R` is `frame`, by slot, or "no".
fn reserving(frame: u32, places: &[PlaceSeen]) -> String {
    let slots: Vec<String> = places
        .iter()
        .filter(|p| p.reserved.is_some_and(|(_, _, f)| f == frame))
        .map(|p| format!("slot {}", p.slot))
        .collect();
    if slots.is_empty() {
        "no".to_string()
    } else {
        slots.join(", ")
    }
}

/// A claim's class and purpose, by name.
fn claim_for(class: SizeClass, purpose: Purpose) -> String {
    let purpose = match purpose {
        Purpose::Read => "a read",
        Purpose::New => "a new page",
    };
    format!("class {} for {purpose}", class.index())
}
