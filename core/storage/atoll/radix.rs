//! The page table and free space: radix trees of metadata pages, one of each per size class.
//!
//! - **Leaves.** In the page table, a `u64` per page number: [`FREE`], [`ALLOCATED`] (no copy on
//!   disk yet), or the slot of its copy plus [`SLOT_BASE`]. In free space, a bit per slot, set
//!   while the slot is in use.
//! - **Above the leaves,** a `u64` per child: how many units below it are in use, so a free one is
//!   found without reading pages that have none.
//!
//! A page of zeros is all free, which is how every page starts. A node's place is fixed by its
//! level and index; a tree grows by a level at the top, the old root becoming child 0 of the new
//! one.

use super::meta::{MetaId, Tree, META_PAGE};
use super::page::{SizeClass, HEADER_SIZE};

pub const FREE: u64 = 0;
pub const ALLOCATED: u64 = 1;
pub const SLOT_BASE: u64 = 2;

/// Page numbers per page-table leaf.
pub const MAP_ENTRIES: u64 = ((META_PAGE - HEADER_SIZE) / 8) as u64;
/// Slots per free-space leaf.
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

/// The leaf holding `unit`, then each ancestor up to the root of a tree of `height` levels.
pub fn path(tree: Tree, class: SizeClass, height: u8, unit: u64) -> Vec<MetaId> {
    (0..height)
        .map(|level| MetaId {
            tree,
            class,
            level,
            index: unit / span(tree, level),
        })
        .collect()
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

/// Which child of the node at `level` holds `unit`.
pub fn child(tree: Tree, level: u8, unit: u64) -> u64 {
    (unit / span(tree, level - 1)) % FANOUT
}

pub fn map_entry(leaf: &[u8], number: u64) -> u64 {
    let at = HEADER_SIZE + 8 * (number % MAP_ENTRIES) as usize;
    u64::from_le_bytes(leaf[at..at + 8].try_into().unwrap())
}

pub fn set_map_entry(leaf: &mut [u8], number: u64, value: u64) {
    let at = HEADER_SIZE + 8 * (number % MAP_ENTRIES) as usize;
    leaf[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

pub fn space_bit(leaf: &[u8], slot: u64) -> bool {
    let bit = slot % SPACE_BITS;
    leaf[HEADER_SIZE + (bit / 8) as usize] & (1 << (bit % 8)) != 0
}

pub fn set_space_bit(leaf: &mut [u8], slot: u64, used: bool) {
    let bit = slot % SPACE_BITS;
    let byte = &mut leaf[HEADER_SIZE + (bit / 8) as usize];
    if used {
        *byte |= 1 << (bit % 8);
    } else {
        *byte &= !(1 << (bit % 8));
    }
}

pub fn used_below(node: &[u8], child: u64) -> u64 {
    let at = HEADER_SIZE + 8 * child as usize;
    u64::from_le_bytes(node[at..at + 8].try_into().unwrap())
}

pub fn set_used_below(node: &mut [u8], child: u64, used: u64) {
    let at = HEADER_SIZE + 8 * child as usize;
    node[at..at + 8].copy_from_slice(&used.to_le_bytes());
}

pub fn add_used_below(node: &mut [u8], child: u64, delta: i64) {
    let now = used_below(node, child)
        .checked_add_signed(delta)
        .expect("a used count below zero");
    set_used_below(node, child, now);
}

/// Whether `unit` is in use in its leaf.
pub fn used(tree: Tree, leaf: &[u8], unit: u64) -> bool {
    match tree {
        Tree::Map => map_entry(leaf, unit) != FREE,
        Tree::Space => space_bit(leaf, unit),
    }
}

/// The first unit at or after `from` in this leaf that is free, if any.
pub fn free_in_leaf(tree: Tree, leaf: &[u8], leaf_index: u64, from: u64) -> Option<u64> {
    let first = leaf_index * leaf_units(tree);
    let start = from.clamp(first, first + leaf_units(tree));
    (start..first + leaf_units(tree)).find(|&u| !used(tree, leaf, u))
}

/// The first child at or after `from_child` of a node at `level` with a free unit below it.
pub fn child_with_room(tree: Tree, node: &[u8], level: u8, from_child: u64) -> Option<u64> {
    let room = span(tree, level - 1);
    (from_child..FANOUT).find(|&c| used_below(node, c) < room)
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
        assert_eq!(child_with_room(Tree::Map, &node, 1, 0), Some(1));
    }
}
