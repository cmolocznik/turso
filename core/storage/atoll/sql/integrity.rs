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
use crate::storage::atoll::txn::{Hold, Txn};
use crate::types::IOResult;
use crate::{LimboError, Result};
use std::collections::{BTreeMap, BTreeSet, HashMap};

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

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
    /// A blocking host: the full checkpoint is driven, the checkpointer's state held for the whole
    /// walk, and the walk's steps driven in place.
    pub fn check_pages(&self, txn: &mut Txn, max: usize) -> Result<Vec<String>> {
        assert!(
            matches!(txn.hold(), Hold::Exclusive { .. }),
            "an integrity check walked the pages without the write gate exclusive"
        );
        crate::race::point("integrity.host_checkpoint");
        super::mode::full_checkpoint(self)?;
        let _held = self.atoll().checkpointer().hold_waiting();
        crate::race::point("integrity.host_walking");
        let mut pages = Pages::new(max);
        drive(self.io(), || pages.step(self, txn))
    }
}

/// The page walk of an integrity check, resumable (ASYNC1 D2e (iii-d)): each call goes on from
/// where the last stopped, every effect of a step recorded before it moves on, so a step resumed
/// after its IO never records twice. The synchronous check drives it in place; a walk in a
/// cooperative frame returns each IO. Its findings are the same either way: they are the same
/// steps.
pub(super) struct Pages {
    findings: Findings,
    at: PagesAt,
}

enum PagesAt {
    /// Every catalog row the statement's snapshot sees: each tree it names, by root, with its id.
    Catalog {
        cursor: VersionedCursor,
        at: CatalogAt,
        named: BTreeMap<PageId, u64>,
    },
    /// Every entry of the dropped list the statement's snapshot sees, so a finding about a tree on
    /// the list can say whether the snapshot sees its entry (DROPPED-LIST1).
    DroppedSeen {
        cursor: VersionedCursor,
        at: ListAt,
        named: BTreeMap<PageId, u64>,
        seen: BTreeSet<PageId>,
    },
    /// Every tree on the abandoned list, then on the dropped list, each read as it is stored.
    Lists {
        list: usize,
        cursor: Cursor,
        at: ListAt,
        named: BTreeMap<PageId, u64>,
        seen: BTreeSet<PageId>,
        roots: BTreeSet<PageId>,
    },
    /// Each tree checked whole, then each of its values' pages read.
    Trees {
        roots: Vec<PageId>,
        index: usize,
        at: TreeAt,
        reached: BTreeMap<PageId, Reach>,
    },
    /// Every page and slot of each class accounted for.
    Account {
        reached: BTreeMap<PageId, Reach>,
        held: BTreeSet<PageId>,
        deferred: std::collections::HashSet<(SizeClass, u64)>,
        class: usize,
        at: AccountAt,
    },
    Done,
}

#[derive(Clone, Copy)]
enum CatalogAt {
    Seek,
    Load,
    Next,
}

#[derive(Clone, Copy)]
enum ListAt {
    Seek,
    Next,
}

enum TreeAt {
    Check(Check),
    /// The tree's values, the next one at `value`.
    Values {
        values: Vec<value::ValueRef>,
        value: usize,
    },
}

enum AccountAt {
    Map(Census),
    Space(Census, Census),
    /// The reached pages of the class, the next one at `next`, each looked up when the page table's
    /// census did not count it; the census's allocated units and those of them retired, made once.
    Entries {
        map: Census,
        space: Census,
        allocated: HashMap<u64, u64>,
        retired: BTreeSet<u64>,
        todo: Vec<(PageId, Reach)>,
        next: usize,
    },
}

const LISTS: [System; 2] = [System::Abandoned, System::Dropped];

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
crate::thread::thread_local! {
    /// The step of a walk on this thread whose next call fails, once, for a test: `tree.check`,
    /// `value.pages`, `value.changed`, `census` or `entry`.
    pub(crate) static WALK_FAILS_AT: std::cell::Cell<Option<&'static str>> =
        const { std::cell::Cell::new(None) };
}

/// Whether this walk's step `at` is to fail, once, as a test asked.
#[inline]
fn injected(at: &'static str) -> bool {
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    if WALK_FAILS_AT.with(|f| f.get()) == Some(at) {
        WALK_FAILS_AT.with(|f| f.set(None));
        return true;
    }
    let _ = at;
    false
}

/// A list's or the catalog's 8-byte big-endian key.
fn list_key(key: &[u8], what: &str) -> Result<u64> {
    let raw: [u8; 8] = key
        .try_into()
        .map_err(|_| LimboError::Corrupt(format!("a {what} key of {} bytes", key.len())))?;
    Ok(u64::from_be_bytes(raw))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn injected_failure(at: &str) -> LimboError {
    LimboError::InternalError(format!("an injected failure at {at}"))
}

#[cfg(test)]
impl Pages {
    /// Where the walk stands, for a test.
    pub(super) fn at_for_test(&self) -> &'static str {
        match &self.at {
            PagesAt::Catalog {
                at: CatalogAt::Seek,
                ..
            } => "catalog.seek",
            PagesAt::Catalog {
                at: CatalogAt::Load,
                ..
            } => "catalog.load",
            PagesAt::Catalog {
                at: CatalogAt::Next,
                ..
            } => "catalog.next",
            PagesAt::DroppedSeen {
                at: ListAt::Seek, ..
            } => "dropped.seek",
            PagesAt::DroppedSeen {
                at: ListAt::Next, ..
            } => "dropped.next",
            PagesAt::Lists {
                at: ListAt::Seek, ..
            } => "lists.seek",
            PagesAt::Lists {
                at: ListAt::Next, ..
            } => "lists.next",
            PagesAt::Trees {
                at: TreeAt::Check(_),
                ..
            } => "trees.check",
            PagesAt::Trees {
                at: TreeAt::Values { .. },
                ..
            } => "trees.values",
            PagesAt::Account {
                at: AccountAt::Map(_),
                ..
            } => "account.map",
            PagesAt::Account {
                at: AccountAt::Space(..),
                ..
            } => "account.space",
            PagesAt::Account {
                at: AccountAt::Entries { .. },
                ..
            } => "account.entries",
            PagesAt::Done => "done",
        }
    }
}

impl Pages {
    pub(super) fn new(max: usize) -> Self {
        Self {
            findings: Findings {
                max,
                found: Vec::new(),
            },
            at: PagesAt::Catalog {
                cursor: VersionedCursor::new(),
                at: CatalogAt::Seek,
                named: BTreeMap::new(),
            },
        }
    }

    /// One call of the walk: Done with the findings once every page is accounted for, or the cap
    /// is reached; else the IO its next step waits for.
    pub(super) fn step(&mut self, db: &AtollDb, txn: &mut Txn) -> Result<IOResult<Vec<String>>> {
        loop {
            let next = match &mut self.at {
                PagesAt::Catalog { cursor, at, named } => {
                    let (txns, catalog) = (db.atoll().transactions(), db.catalog().tree());
                    match at {
                        CatalogAt::Seek | CatalogAt::Next => {
                            let found = match at {
                                CatalogAt::Seek => done!(catalog.seek(
                                    txns,
                                    db.versions(),
                                    txn,
                                    cursor,
                                    &catalog_key(0),
                                    Seek::Ge,
                                )),
                                _ => done!(catalog.next(txns, db.versions(), txn, cursor)),
                            };
                            if found {
                                *at = CatalogAt::Load;
                                None
                            } else {
                                Some(PagesAt::DroppedSeen {
                                    cursor: VersionedCursor::new(),
                                    at: ListAt::Seek,
                                    named: std::mem::take(named),
                                    seen: BTreeSet::new(),
                                })
                            }
                        }
                        CatalogAt::Load => {
                            done!(catalog.load(cursor));
                            let id = list_key(cursor.key().expect("a key"), "catalog")?;
                            let row = cursor.value().expect("a row");
                            named.insert(super::catalog::Entry::decode(row)?.root, id);
                            *at = CatalogAt::Next;
                            None
                        }
                    }
                }
                PagesAt::DroppedSeen {
                    cursor,
                    at,
                    named,
                    seen,
                } => {
                    let (txns, dropped) = (db.atoll().transactions(), db.dropped());
                    let found = match at {
                        ListAt::Seek => {
                            done!(dropped.seek(txns, db.versions(), txn, cursor, &[0; 8], Seek::Ge,))
                        }
                        ListAt::Next => done!(dropped.next(txns, db.versions(), txn, cursor)),
                    };
                    if found {
                        let raw = list_key(cursor.key().expect("an entry"), "Dropped")?;
                        seen.insert(PageId::from_raw(raw));
                        *at = ListAt::Next;
                        None
                    } else {
                        Some(PagesAt::Lists {
                            list: 0,
                            cursor: Cursor::new(),
                            at: ListAt::Seek,
                            named: std::mem::take(named),
                            seen: std::mem::take(seen),
                            roots: System::ALL.iter().map(|s| s.root()).collect(),
                        })
                    }
                }
                PagesAt::Lists {
                    list,
                    cursor,
                    at,
                    named,
                    seen,
                    roots,
                } => {
                    let which = LISTS[*list];
                    // The dropped list is versioned: its own tree's handle, whose leaves reserve
                    // room for version headers; the abandoned list is plain.
                    let plain;
                    let tree = match which {
                        System::Dropped => db.dropped().tree(),
                        _ => {
                            plain = db.open_plain(which.root());
                            &plain
                        }
                    };
                    let found = match at {
                        ListAt::Seek => done!(tree.seek(cursor, &[], Seek::Ge)),
                        ListAt::Next => done!(tree.next(cursor)),
                    };
                    if found {
                        let root = PageId::from_raw(list_key(
                            cursor.key().expect("an entry"),
                            &format!("{which:?}"),
                        )?);
                        if let Some(id) = named.get(&root) {
                            let stored = cursor.value().unwrap_or(&[]);
                            self.findings.add(format!(
                                "tree {} is named by the catalog and on the {which:?} list, to be freed: \
                                 the catalog names it as tree id {id} at the check's snapshot \
                                 (transaction {:?}); the list's entry is {} that snapshot, its \
                                 stored value {}",
                                root.raw(),
                                txn.start(),
                                if seen.contains(&root) { "seen by" } else { "not seen by" },
                                hex(&stored[..stored.len().min(48)]),
                            ));
                        }
                        roots.insert(root);
                        *at = ListAt::Next;
                        None
                    } else if *list + 1 < LISTS.len() {
                        *list += 1;
                        *cursor = Cursor::new();
                        *at = ListAt::Seek;
                        None
                    } else {
                        let mut all = std::mem::take(roots);
                        all.extend(std::mem::take(named).into_keys());
                        Some(PagesAt::Trees {
                            roots: all.into_iter().collect(),
                            index: 0,
                            at: TreeAt::Check(Check::default()),
                            reached: BTreeMap::new(),
                        })
                    }
                }
                PagesAt::Trees {
                    roots,
                    index,
                    at,
                    reached,
                } => {
                    if *index == roots.len() || self.findings.full() {
                        if self.findings.full() {
                            Some(PagesAt::Done)
                        } else {
                            let store = db.atoll().store();
                            Some(PagesAt::Account {
                                reached: std::mem::take(reached),
                                held: store.orphans().into_iter().collect(),
                                deferred: store.deferred_slots(),
                                class: 0,
                                at: AccountAt::Map(Census::new(
                                    Radix::Map,
                                    SizeClass::new(0).expect("a class"),
                                )),
                            })
                        }
                    } else {
                        let root = roots[*index];
                        match at {
                            TreeAt::Check(op) => {
                                // Every tree is versioned, its values behind a version head, but
                                // the abandoned list and the sequence values, which are plain.
                                let plain =
                                    System::ALL.iter().any(|s| s.plain() && s.root() == root);
                                let tree = match plain {
                                    true => db.open_plain(root),
                                    false => db.open_plain(root).with_head(HEADER).with_reserves(),
                                };
                                let checked = match injected("tree.check") {
                                    true => Err(injected_failure("tree.check")),
                                    false => tree.check(op),
                                };
                                match checked {
                                    Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                                    Ok(IOResult::Done(stats)) => {
                                        for pid in stats.pages {
                                            reach(
                                                reached,
                                                pid,
                                                Reach::Node(root),
                                                &mut self.findings,
                                            );
                                        }
                                        *at = TreeAt::Values {
                                            values: stats.values,
                                            value: 0,
                                        };
                                    }
                                    Err(e) => {
                                        self.findings.add(format!("tree {}: {e}", root.raw()));
                                        *index += 1;
                                        *at = TreeAt::Check(Check::default());
                                    }
                                }
                                None
                            }
                            TreeAt::Values { values, value } => {
                                if *value == values.len() {
                                    *index += 1;
                                    *at = TreeAt::Check(Check::default());
                                } else {
                                    // Each value read whole in one call: resumed, it reads its
                                    // pages again from the first, one value's worth.
                                    let reference = values[*value];
                                    let pool = db.atoll().pool();
                                    if injected("value.pages") {
                                        return Err(injected_failure("value.pages"));
                                    }
                                    let read = match injected("value.changed") {
                                        true => Err(value::Read::Changed),
                                        false => done!(value::pages(pool, reference, &|| true)),
                                    };
                                    match read {
                                        Ok(pages) => pages.into_iter().for_each(|pid| {
                                            reach(
                                                reached,
                                                pid,
                                                Reach::Value(root),
                                                &mut self.findings,
                                            )
                                        }),
                                        Err(read) => self.findings.add(format!(
                                            "a value of tree {} at page {} cannot be read: {}",
                                            root.raw(),
                                            reference.pid.raw(),
                                            match read {
                                                value::Read::Changed =>
                                                    "its pages hold another value",
                                                value::Read::Unheld => "its holder is gone",
                                                value::Read::Value(_) => "it read",
                                            }
                                        )),
                                    }
                                    *value += 1;
                                }
                                None
                            }
                        }
                    }
                }
                PagesAt::Account {
                    reached,
                    held,
                    deferred,
                    class,
                    at,
                } => {
                    let store = db.atoll().store();
                    let size = SizeClass::new(*class).expect("a class");
                    match at {
                        AccountAt::Map(op) => {
                            if injected("census") {
                                return Err(injected_failure("census"));
                            }
                            done!(store.census(op));
                            let map = std::mem::replace(op, Census::new(Radix::Map, size));
                            *at = AccountAt::Space(map, Census::new(Radix::Space, size));
                            None
                        }
                        AccountAt::Space(map, op) => {
                            done!(store.census(op));
                            for miscount in [&*map, &*op].into_iter().flat_map(|t| t.miscounts()) {
                                self.findings.add(miscount);
                            }
                            let page = |number: u64| PageId::new(size, number);
                            let todo = reached
                                .range(page(0)..=page(u64::MAX >> 8))
                                .map(|(&pid, &by)| (pid, by))
                                .collect();
                            let map = std::mem::replace(map, Census::new(Radix::Map, size));
                            let space = std::mem::replace(op, Census::new(Radix::Space, size));
                            let allocated = map.units.iter().copied().collect();
                            let retired = map
                                .units
                                .iter()
                                .map(|&(u, _)| u)
                                .filter(|&u| store.retired(PageId::new(size, u)))
                                .collect();
                            *at = AccountAt::Entries {
                                map,
                                space,
                                allocated,
                                retired,
                                todo,
                                next: 0,
                            };
                            None
                        }
                        AccountAt::Entries {
                            map,
                            space,
                            allocated,
                            retired,
                            todo,
                            next,
                        } => {
                            while *next < todo.len() {
                                let (pid, by) = todo[*next];
                                let entry = match allocated.get(&pid.number()) {
                                    Some(&e) => e,
                                    None if injected("entry") => {
                                        return Err(injected_failure("entry"))
                                    }
                                    None => done!(store.entry(pid)),
                                };
                                if entry == FREE {
                                    self.findings.add(format!(
                                        "page {} is reached by {by} and free",
                                        pid.raw()
                                    ));
                                } else if !allocated.contains_key(&pid.number()) {
                                    self.findings.add(format!(
                                        "page {} is reached by {by} and allocated, under a count that says none is",
                                        pid.raw()
                                    ));
                                }
                                if retired.contains(&pid.number()) {
                                    self.findings.add(format!(
                                        "page {} is reached by {by} and retired",
                                        pid.raw()
                                    ));
                                }
                                *next += 1;
                            }
                            account_allocated(
                                size,
                                *class,
                                map,
                                space,
                                reached,
                                retired,
                                held,
                                deferred,
                                &mut self.findings,
                            );
                            if self.findings.full() || *class + 1 == CLASS_COUNT {
                                Some(PagesAt::Done)
                            } else {
                                *class += 1;
                                let next_size = SizeClass::new(*class).expect("a class");
                                *at = AccountAt::Map(Census::new(Radix::Map, next_size));
                                None
                            }
                        }
                    }
                }
                PagesAt::Done => {
                    return Ok(IOResult::Done(std::mem::take(&mut self.findings.found)));
                }
            };
            if let Some(next) = next {
                self.at = next;
            }
        }
    }
}

/// Records that `by` reached `pid`; a page reached twice is a finding.
fn reach(reached: &mut BTreeMap<PageId, Reach>, pid: PageId, by: Reach, findings: &mut Findings) {
    if let Some(first) = reached.insert(pid, by) {
        findings.add(format!(
            "page {} is reached twice: by {first} and by {by}",
            pid.raw()
        ));
    }
}

/// Every page of class `c` allocated, and the slot of every copy, against what the trees reached.
#[allow(clippy::too_many_arguments)]
fn account_allocated(
    size: SizeClass,
    c: usize,
    map: &Census,
    space: &Census,
    reached: &BTreeMap<PageId, Reach>,
    retired: &BTreeSet<u64>,
    held: &BTreeSet<PageId>,
    deferred: &std::collections::HashSet<(SizeClass, u64)>,
    findings: &mut Findings,
) {
    let page = |number: u64| PageId::new(size, number);
    let in_use: BTreeSet<u64> = space.units.iter().map(|&(u, _)| u).collect();
    let mut slots: HashMap<u64, u64> = HashMap::new();
    for &(number, entry) in &map.units {
        let pid = page(number);
        if !reached.contains_key(&pid) && !retired.contains(&number) && !held.contains(&pid) {
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
        if !slots.contains_key(&slot) && !deferred.contains(&(size, slot)) {
            findings.add(format!(
                "slot {slot} of class {c} is in use and holds no page's copy"
            ));
        }
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
