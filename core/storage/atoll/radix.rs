//! The page table, free space and retired marks: radix trees of metadata pages, one of each per
//! size class.
//!
//! - **Leaves.** In the page table, a `u64` per page number: [`FREE`], [`ALLOCATED`] (no copy on
//!   disk yet), or the slot of its copy plus [`SLOT_BASE`]. In free space, a bit per slot, set
//!   while the slot is in use. In the retired marks, a bit per page number, set while the page is
//!   retired (DESIGN.md §6.5).
//! - **Above the leaves,** a `u64` per child: how many units below it are in use, so a free one is
//!   found without reading pages that have none, and so is every retired page.
//!
//! A page of zeros is all free, which is how every page starts. A node's place is fixed by its
//! level and index; a tree grows by a level at the top, the old root becoming child 0 of the new
//! one.

use super::meta::{MetaId, Tree, INDEX_BITS, LEVELS, META_PAGE, TREES};
use super::page::{SizeClass, HEADER_SIZE};
use super::shared::{Bytes, BytesMut};

pub const FREE: u64 = 0;
pub const ALLOCATED: u64 = 1;
pub const SLOT_BASE: u64 = 2;

/// Page numbers per page-table leaf.
pub const MAP_ENTRIES: u64 = ((META_PAGE - HEADER_SIZE) / 8) as u64;
/// Slots per free-space leaf, and page numbers per retired-marks leaf.
pub const SPACE_BITS: u64 = ((META_PAGE - HEADER_SIZE) * 8) as u64;
/// Children per node above the leaves.
pub const FANOUT: u64 = ((META_PAGE - HEADER_SIZE) / 8) as u64;

pub fn leaf_units(tree: Tree) -> u64 {
    match tree {
        Tree::Map => MAP_ENTRIES,
        Tree::Space => SPACE_BITS,
    }
}

/// Units below one node at `level`.
pub fn span(tree: Tree, level: u8) -> u64 {
    (1..=level).fold(leaf_units(tree), |n, _| n.saturating_mul(FANOUT))
}

/// Units a tree of `height` levels holds.
pub fn capacity(tree: Tree, height: u8) -> u64 {
    span(tree, height - 1)
}

/// Units below the leaves a metadata id can name, those whose index is below `1 << INDEX_BITS`
/// ([`MetaId::pid`]): a tree `MAX_HEIGHT` levels high holds more, which are never used.
pub const fn nameable(tree: Tree) -> u64 {
    let leaf = match tree {
        Tree::Map => MAP_ENTRIES,
        Tree::Space => SPACE_BITS,
    };
    leaf << INDEX_BITS
}

/// The first index at `level` of `tree` whose node holds no unit a metadata id names
/// ([`nameable`]): a page at or past it is never looked for ([`MetaCache::join`]'s guard, read on
/// every look, so a table made at compile time).
pub fn first_unnameable(tree: Tree, level: u8) -> u64 {
    FIRST_UNNAMEABLE[tree as usize][level as usize]
}

/// `nameable(tree).div_ceil(span(tree, level))` for every tree and level an id has room for.
const FIRST_UNNAMEABLE: [[u64; LEVELS]; TREES] = {
    let mut table = [[0u64; LEVELS]; TREES];
    let trees = [Tree::Map, Tree::Space];
    let mut t = 0;
    while t < TREES {
        let tree = trees[t];
        let mut span = match tree {
            Tree::Map => MAP_ENTRIES,
            Tree::Space => SPACE_BITS,
        };
        let mut level = 0;
        while level < LEVELS {
            table[tree as usize][level] = nameable(tree).div_ceil(span);
            span = span.saturating_mul(FANOUT);
            level += 1;
        }
        t += 1;
    }
    table
};

/// The units a search of a tree `height` levels high may find: its capacity, up to the nameable.
pub fn searchable(tree: Tree, height: u8) -> u64 {
    capacity(tree, height).min(nameable(tree))
}

/// Levels a tree ever has: the fewest whose root reaches every leaf a metadata id can name, one
/// whose index is below `1 << INDEX_BITS` ([`MetaId::pid`]), as FANOUT^(h - 1) >= 2^46 first holds
/// at h = 7. Both trees' leaves are counted alike, so it bounds either; a taller tree reaches no
/// leaf a shorter one does not, and the store refuses a growth past it.
pub const MAX_HEIGHT: u8 = {
    let (mut height, mut leaves) = (1u8, 1u128);
    while leaves < 1u128 << INDEX_BITS {
        leaves *= FANOUT as u128;
        height += 1;
    }
    height
};
const _: () = assert!(MAX_HEIGHT as usize <= LEVELS);

/// The leaf holding a unit, then each ancestor up to the root, held inline.
#[derive(Clone, Copy, Debug)]
pub struct Path {
    ids: [MetaId; MAX_HEIGHT as usize],
    len: u8,
}

impl std::ops::Deref for Path {
    type Target = [MetaId];

    fn deref(&self) -> &[MetaId] {
        &self.ids[..usize::from(self.len)]
    }
}

/// The leaf holding `unit`, then each ancestor up to the root of a tree of `height` levels: the one
/// door from a height to its pages, so a height is 1 to [`MAX_HEIGHT`] wherever a path is made (the
/// superblock's heights are refused past it, and a growth past it is refused).
pub fn path(tree: Tree, class: SizeClass, height: u8, unit: u64) -> Path {
    assert!(
        (1..=MAX_HEIGHT).contains(&height),
        "a {tree:?} tree of class {} {height} levels high: a tree has 1 to {MAX_HEIGHT}",
        class.index()
    );
    let mut ids = [root(tree, class, 1); MAX_HEIGHT as usize];
    for level in 0..height {
        ids[usize::from(level)] = MetaId {
            tree,
            class,
            level,
            index: unit / span(tree, level),
        };
    }
    Path { ids, len: height }
}

/// The root of a tree of `height` levels.
pub fn root(tree: Tree, class: SizeClass, height: u8) -> MetaId {
    MetaId {
        tree,
        class,
        level: height - 1,
        index: 0,
    }
}

/// Units in use below the root of a tree `height` levels high, as the root itself counts them: its
/// units in use if it is a leaf, its children's counts summed if not.
pub fn in_use<P: Bytes + ?Sized>(tree: Tree, height: u8, root: &P) -> u64 {
    if height == 1 {
        (0..leaf_units(tree))
            .filter(|&u| used(tree, root, u))
            .count() as u64
    } else {
        (0..FANOUT).map(|c| used_below(root, c)).sum()
    }
}

/// Which child of the node at `level` holds `unit`.
pub fn child(tree: Tree, level: u8, unit: u64) -> u64 {
    (unit / span(tree, level - 1)) % FANOUT
}

pub fn map_entry<P: Bytes + ?Sized>(leaf: &P, number: u64) -> u64 {
    leaf.u64_at(HEADER_SIZE + 8 * (number % MAP_ENTRIES) as usize)
}

pub fn set_map_entry<P: BytesMut + ?Sized>(leaf: &mut P, number: u64, value: u64) {
    leaf.put_u64(HEADER_SIZE + 8 * (number % MAP_ENTRIES) as usize, value);
}

pub fn space_bit<P: Bytes + ?Sized>(leaf: &P, slot: u64) -> bool {
    let bit = slot % SPACE_BITS;
    leaf.u8_at(HEADER_SIZE + (bit / 8) as usize) & (1 << (bit % 8)) != 0
}

pub fn set_space_bit<P: BytesMut + ?Sized>(leaf: &mut P, slot: u64, used: bool) {
    let bit = slot % SPACE_BITS;
    let at = HEADER_SIZE + (bit / 8) as usize;
    let byte = leaf.u8_at(at);
    let byte = if used {
        byte | 1 << (bit % 8)
    } else {
        byte & !(1 << (bit % 8))
    };
    leaf.put_u8(at, byte);
}

pub fn used_below<P: Bytes + ?Sized>(node: &P, child: u64) -> u64 {
    node.u64_at(HEADER_SIZE + 8 * child as usize)
}

pub fn set_used_below<P: BytesMut + ?Sized>(node: &mut P, child: u64, used: u64) {
    node.put_u64(HEADER_SIZE + 8 * child as usize, used);
}

pub fn add_used_below<P: BytesMut + ?Sized>(node: &mut P, child: u64, delta: i64) {
    let now = used_below(node, child)
        .checked_add_signed(delta)
        .expect("a used count below zero");
    set_used_below(node, child, now);
}

/// Whether `unit` is in use in its leaf.
pub fn used<P: Bytes + ?Sized>(tree: Tree, leaf: &P, unit: u64) -> bool {
    match tree {
        Tree::Map => map_entry(leaf, unit) != FREE,
        Tree::Space => space_bit(leaf, unit),
    }
}

/// The first unit at or after `from` in this leaf that is free, if any.
pub fn free_in_leaf<P: Bytes + ?Sized>(
    tree: Tree,
    leaf: &P,
    leaf_index: u64,
    from: u64,
) -> Option<u64> {
    let first = leaf_index * leaf_units(tree);
    let start = from.clamp(first, first + leaf_units(tree));
    (start..first + leaf_units(tree)).find(|&u| !used(tree, leaf, u))
}

/// The first child at or after `from_child` and before `to_child` of a node at `level` with a free
/// unit below it.
pub fn child_with_room<P: Bytes + ?Sized>(
    tree: Tree,
    node: &P,
    level: u8,
    from_child: u64,
    to_child: u64,
) -> Option<u64> {
    let room = span(tree, level - 1);
    (from_child..to_child.min(FANOUT)).find(|&c| used_below(node, c) < room)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_unit_is_found_through_its_ancestors() {
        let class = SizeClass::new(0).unwrap();
        let unit = 3 * MAP_ENTRIES * FANOUT + 5 * MAP_ENTRIES + 7;
        let p = path(Tree::Map, class, 3, unit);
        assert_eq!(
            p.iter().map(|m| (m.level, m.index)).collect::<Vec<_>>(),
            vec![(0, 3 * FANOUT + 5), (1, 3), (2, 0)]
        );
        assert_eq!(child(Tree::Map, 1, unit), 5);
        assert_eq!(child(Tree::Map, 2, unit), 3);
        assert_eq!(capacity(Tree::Map, 1), MAP_ENTRIES);
        assert_eq!(capacity(Tree::Space, 2), SPACE_BITS * FANOUT);
    }

    #[test]
    fn leaves_and_counts_start_free_and_read_back_as_set() {
        let mut leaf = vec![0u8; META_PAGE];
        assert_eq!(free_in_leaf(Tree::Space, &leaf, 2, 0), Some(2 * SPACE_BITS));
        set_space_bit(&mut leaf, 2 * SPACE_BITS, true);
        assert_eq!(
            free_in_leaf(Tree::Space, &leaf, 2, 0),
            Some(2 * SPACE_BITS + 1)
        );
        set_map_entry(&mut leaf, 4, SLOT_BASE + 9);
        assert_eq!(map_entry(&leaf, 4), SLOT_BASE + 9);
        let mut node = vec![0u8; META_PAGE];
        add_used_below(&mut node, 0, MAP_ENTRIES as i64);
        assert_eq!(child_with_room(Tree::Map, &node, 1, 0, FANOUT), Some(1));
    }

    /// A search stops short of the leaves no metadata id names, at every level: a child that
    /// starts at or past its bound has no room, whatever its count (M2b 1c).
    /// The guard's table, for every tree and every level an id has room for: the first index whose
    /// node starts at or past the last nameable unit, the one before it holding some.
    #[test]
    fn the_first_unnameable_index_is_where_a_node_starts_past_every_nameable_unit() {
        for tree in [Tree::Map, Tree::Space] {
            for level in 0..LEVELS as u8 {
                let (first, span) = (first_unnameable(tree, level), span(tree, level));
                assert!(
                    first >= 1
                        && first.saturating_mul(span) >= nameable(tree)
                        && (first - 1).saturating_mul(span) < nameable(tree),
                    "{tree:?} level {level}: first unnameable index {first}"
                );
            }
        }
    }

    #[test]
    fn a_search_never_reaches_a_unit_no_metadata_id_names() {
        let node = vec![0u8; META_PAGE];
        for level in 1..MAX_HEIGHT {
            assert_eq!(child_with_room(Tree::Space, &node, level, 0, 3), Some(0));
            assert_eq!(child_with_room(Tree::Space, &node, level, 3, 3), None);
        }
        for tree in [Tree::Map, Tree::Space] {
            assert_eq!(
                nameable(tree) % leaf_units(tree),
                0,
                "{tree:?} bound mid-leaf"
            );
            assert_eq!(nameable(tree) / leaf_units(tree), 1 << INDEX_BITS);
            assert!(searchable(tree, MAX_HEIGHT) == nameable(tree));
            assert!(searchable(tree, MAX_HEIGHT - 1) == capacity(tree, MAX_HEIGHT - 1));
            assert!(capacity(tree, MAX_HEIGHT - 1) < nameable(tree));
        }
    }
}
