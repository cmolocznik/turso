//! `PRAGMA integrity_check`'s page arm in Atoll mode (DESIGN.md §17, §12; R36).
//!
//! The statement holds the write gate exclusive, so no transaction writes, no commit hands a tree
//! to the drain, and no statement steps the checkpointer; a full checkpoint first leaves no
//! writeback in flight, and the check holds the checkpointer while it walks. Then:
//!
//! - **Every tree the database keeps** is checked whole ([`Tree::check`]): the system trees, every
//!   tree a catalog row names at the statement's snapshot, and every tree on the abandoned and
//!   dropped lists, each once however many name it. Its node pages and its values' pages are what
//!   it reaches.
//! - **Every page a tree reaches** is reached once, is allocated (looked up by its own entry, so a
//!   page under a count of zero is still found), and is not retired.
//! - **Every page allocated** is reached, retired (a version a snapshot may still read, §6.5), or
//!   orphaned (named by nothing, its free waiting for the checkpointer): one that is none of these
//!   is never used.
//! - **Every copy** sits in a slot free space marks in use, no two pages share a slot, and every
//!   slot in use holds a copy.
//! - **Every count** above the leaves of the page table and free space agrees
//!   with what is in use below it.
//!
//! Temporary pages (version indexes, §6.4) are not the page store's and are not counted.

use super::catalog::{key as catalog_key, System};
use super::AtollDb;
use crate::storage::atoll::btree::value;
use crate::storage::atoll::btree::{Check, Cursor, Seek};
use crate::storage::atoll::drive;
use crate::storage::atoll::meta::Tree as Radix;
use crate::storage::atoll::page::{PageId, SizeClass, CLASS_COUNT};
use crate::storage::atoll::radix::{FREE, SLOT_BASE};
use crate::storage::atoll::store::Census;
use crate::storage::atoll::txn::versioned::{VersionedCursor, HEADER};
use crate::storage::atoll::txn::Txn;
use crate::{LimboError, Result};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// What reached a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reach {
    Node(PageId),
    Value(PageId),
}

impl std::fmt::Display for Reach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reach::Node(root) => write!(f, "a node of tree {}", root.raw()),
            Reach::Value(root) => write!(f, "a value of tree {}", root.raw()),
        }
    }
}

/// The findings, at most `max` of them, stopping the check at the cap.
struct Findings {
    max: usize,
    found: Vec<String>,
}

impl Findings {
    fn add(&mut self, finding: String) {
        if self.found.len() < self.max {
            self.found.push(finding);
        }
    }

    fn full(&self) -> bool {
        self.found.len() >= self.max
    }
}

impl AtollDb {
    /// The page arm of an integrity check, in `txn`, which holds the write gate exclusive: what
    /// it found wrong, at most `max` of it, none for a database whose pages are all accounted for.
    pub fn check_pages(&self, txn: &mut Txn, max: usize) -> Result<Vec<String>> {
        let mut findings = Findings {
            max,
            found: Vec::new(),
        };
        super::mode::full_checkpoint(self)?;
        let _held = self.atoll().checkpointer().hold_waiting();
        let reached = self.reached(txn, &mut findings)?;
        if !findings.full() {
            self.account(&reached, &mut findings)?;
        }
        Ok(findings.found)
    }

    /// Every tree the database keeps, by root, each once; a tree the catalog names and a list
    /// holds to free is a finding.
    fn roots(&self, txn: &mut Txn, findings: &mut Findings) -> Result<BTreeSet<PageId>> {
        let mut roots: BTreeSet<PageId> = System::ALL.iter().map(|s| s.root()).collect();
        let mut named = BTreeSet::new();
        // Every catalog row the statement's snapshot sees.
        let txns = self.atoll().transactions();
        let catalog = self.catalog().tree();
        let mut cursor = VersionedCursor::new();
        let mut found = drive(self.io(), || {
            catalog.seek(
                txns,
                self.versions(),
                txn,
                &mut cursor,
                &catalog_key(0),
                Seek::Ge,
            )
        })?;
        while found {
            drive(self.io(), || catalog.load(&mut cursor))?;
            let row = cursor.value().expect("a row");
            named.insert(super::catalog::Entry::decode(row)?.root);
            found = drive(self.io(), || {
                catalog.next(txns, self.versions(), txn, &mut cursor)
            })?;
        }
        // Every tree on a list, whatever its entry's version: listed, it is not freed yet.
        for list in [System::Abandoned, System::Dropped] {
            let tree = self.open_plain(list.root());
            let mut cursor = Cursor::new();
            let mut found = drive(self.io(), || tree.seek(&mut cursor, &[], Seek::Ge))?;
            while found {
                let key = cursor.key().expect("an entry");
                let raw: [u8; 8] = key.try_into().map_err(|_| {
                    LimboError::Corrupt(format!("a {list:?} list key of {} bytes", key.len()))
                })?;
                let root = PageId::from_raw(u64::from_be_bytes(raw));
                if named.contains(&root) {
                    findings.add(format!(
                        "tree {} is named by the catalog and on the {list:?} list, to be freed",
                        root.raw()
                    ));
                }
                roots.insert(root);
                found = drive(self.io(), || tree.next(&mut cursor))?;
            }
        }
        roots.extend(named);
        Ok(roots)
    }

    /// Every page every tree reaches, with what reached it; a page reached twice, or a tree that
    /// does not check, is a finding.
    fn reached(&self, txn: &mut Txn, findings: &mut Findings) -> Result<BTreeMap<PageId, Reach>> {
        let mut reached = BTreeMap::new();
        let mut reach = |pid: PageId, by: Reach, findings: &mut Findings| {
            if let Some(first) = reached.insert(pid, by) {
                findings.add(format!(
                    "page {} is reached twice: by {first} and by {by}",
                    pid.raw()
                ));
            }
        };
        for root in self.roots(txn, findings)? {
            if findings.full() {
                break;
            }
            // Every tree is versioned, its values behind a version head, but the abandoned list
            // and the sequence values, which are plain.
            let plain = System::ALL.iter().any(|s| s.plain() && s.root() == root);
            let tree = match plain {
                true => self.open_plain(root),
                false => self.open_plain(root).with_head(HEADER).with_reserves(),
            };
            let mut op = Check::default();
            let stats = match drive(self.io(), || tree.check(&mut op)) {
                Ok(stats) => stats,
                Err(e) => {
                    findings.add(format!("tree {}: {e}", root.raw()));
                    continue;
                }
            };
            for pid in stats.pages {
                reach(pid, Reach::Node(root), findings);
            }
            for reference in stats.values {
                let pool = self.atoll().pool();
                match drive(self.io(), || value::pages(pool, reference, &|| true))? {
                    Ok(pages) => pages
                        .into_iter()
                        .for_each(|pid| reach(pid, Reach::Value(root), findings)),
                    Err(read) => findings.add(format!(
                        "a value of tree {} at page {} cannot be read: {}",
                        root.raw(),
                        reference.pid.raw(),
                        match read {
                            value::Read::Changed => "its pages hold another value",
                            value::Read::Unheld => "its holder is gone",
                            value::Read::Value(_) => "it read",
                        }
                    )),
                }
            }
        }
        Ok(reached)
    }

    /// Accounts for every page and slot of every class against what the trees reached.
    fn account(&self, reached: &BTreeMap<PageId, Reach>, findings: &mut Findings) -> Result<()> {
        let store = self.atoll().store();
        let held: BTreeSet<PageId> = store.orphans().into_iter().collect();
        // A slot whose copy a newer one replaced stays in use until the checkpoint that replaced
        // it is durable.
        let deferred = store.deferred_slots();
        for c in 0..CLASS_COUNT {
            let class = SizeClass::new(c).expect("a class");
            let census = |tree| -> Result<Census> {
                let mut op = Census::new(tree, class);
                drive(self.io(), || store.census(&mut op))?;
                Ok(op)
            };
            let (map, space) = (census(Radix::Map)?, census(Radix::Space)?);
            for miscount in [&map, &space].into_iter().flat_map(|t| t.miscounts()) {
                findings.add(miscount);
            }
            let allocated: HashMap<u64, u64> = map.units.iter().copied().collect();
            let retired: BTreeSet<u64> = map
                .units
                .iter()
                .map(|&(u, _)| u)
                .filter(|&u| store.retired(PageId::new(class, u)))
                .collect();
            let in_use: BTreeSet<u64> = space.units.iter().map(|&(u, _)| u).collect();
            let page = |number: u64| PageId::new(class, number);
            // Every page a tree reaches of this class.
            for (&pid, by) in reached.range(page(0)..=page(u64::MAX >> 8)) {
                let entry = match allocated.get(&pid.number()) {
                    Some(&e) => e,
                    None => drive(self.io(), || store.entry(pid))?,
                };
                if entry == FREE {
                    findings.add(format!("page {} is reached by {by} and free", pid.raw()));
                } else if !allocated.contains_key(&pid.number()) {
                    findings.add(format!(
                        "page {} is reached by {by} and allocated, under a count that says none is",
                        pid.raw()
                    ));
                }
                if retired.contains(&pid.number()) {
                    findings.add(format!("page {} is reached by {by} and retired", pid.raw()));
                }
            }
            // Every page allocated, and the slot of every copy.
            let mut slots: HashMap<u64, u64> = HashMap::new();
            for &(number, entry) in &map.units {
                let pid = page(number);
                if !reached.contains_key(&pid) && !retired.contains(&number) && !held.contains(&pid)
                {
                    findings.add(format!("page {} is allocated and never used", pid.raw()));
                }
                if let Some(slot) = entry.checked_sub(SLOT_BASE) {
                    if !in_use.contains(&slot) {
                        findings.add(format!(
                            "page {}'s copy is in slot {slot} of class {c}, which free space marks free",
                            pid.raw()
                        ));
                    }
                    if let Some(other) = slots.insert(slot, number) {
                        findings.add(format!(
                            "slot {slot} of class {c} holds the copies of pages {} and {}",
                            page(other).raw(),
                            pid.raw()
                        ));
                    }
                }
            }
            for slot in in_use {
                if !slots.contains_key(&slot) && !deferred.contains(&(class, slot)) {
                    findings.add(format!(
                        "slot {slot} of class {c} is in use and holds no page's copy"
                    ));
                }
            }
            if findings.full() {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
