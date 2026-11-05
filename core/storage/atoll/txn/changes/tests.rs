//! A transaction's own log: what it appends reads back in order, a statement's rollback drops its
//! changes and every later one's, and bytes that do not hold a change together read as corrupt.

use super::*;
use crate::storage::atoll::page::SizeClass;

fn tree(n: u64) -> PageId {
    PageId::new(SizeClass::new(0).unwrap(), n)
}

fn owned(changes: &Changes) -> Vec<(char, u64, Vec<u8>, Vec<u8>)> {
    changes
        .iter()
        .map(|c| match c {
            Change::Put {
                tree,
                plain,
                key,
                value,
                ..
            } => (
                if plain { 'P' } else { 'p' },
                tree.raw(),
                key.to_vec(),
                value.to_vec(),
            ),
            Change::Delete {
                tree, plain, key, ..
            } => (
                if plain { 'D' } else { 'd' },
                tree.raw(),
                key.to_vec(),
                Vec::new(),
            ),
            Change::Create { root, list } => (
                'c',
                root.raw(),
                Vec::new(),
                list.raw().to_le_bytes().to_vec(),
            ),
            Change::Drain { root, list } => (
                'x',
                root.raw(),
                Vec::new(),
                list.raw().to_le_bytes().to_vec(),
            ),
        })
        .collect()
}

/// Changes read back in the order made, whatever their trees, kinds and lengths, a value given in
/// pieces as one.
#[test]
fn changes_read_back_in_the_order_made() {
    let mut c = Changes::new();
    c.create(1, tree(7), tree(2));
    c.put(1, tree(7), b"a", b"one");
    c.delete(1, tree(7), b"b");
    c.plain_put(2, tree(3), b"s", b"9");
    c.plain_delete(2, tree(3), b"t");
    c.put_pieces(2, tree(9), b"", &[b"tw", b"", b"o"]);
    c.put(2, tree(7), &[0xff; 300], &[1; 70_000]);
    c.drained(tree(5), tree(2));
    assert_eq!(
        owned(&c),
        vec![
            (
                'c',
                tree(7).raw(),
                Vec::new(),
                tree(2).raw().to_le_bytes().to_vec()
            ),
            ('p', tree(7).raw(), b"a".to_vec(), b"one".to_vec()),
            ('d', tree(7).raw(), b"b".to_vec(), Vec::new()),
            ('P', tree(3).raw(), b"s".to_vec(), b"9".to_vec()),
            ('D', tree(3).raw(), b"t".to_vec(), Vec::new()),
            ('p', tree(9).raw(), Vec::new(), b"two".to_vec()),
            ('p', tree(7).raw(), vec![0xff; 300], vec![1; 70_000]),
            (
                'x',
                tree(5).raw(),
                Vec::new(),
                tree(2).raw().to_le_bytes().to_vec()
            ),
        ]
    );
    let decoded: Vec<_> = Decode::new(c.bytes()).collect::<Result<_>>().unwrap();
    assert_eq!(decoded, c.iter().collect::<Vec<_>>());
}

/// A rollback to a statement drops the changes of every statement after it, and keeps its own and
/// every earlier one's; to 0 it drops them all. Statements that changed nothing leave no mark.
#[test]
fn a_rollback_to_a_statement_drops_every_later_statements_changes() {
    let mut c = Changes::new();
    c.put(1, tree(1), b"k1", b"v1");
    c.put(3, tree(1), b"k3", b"v3");
    c.put(3, tree(1), b"k3b", b"v3b");
    c.put(5, tree(1), b"k5", b"v5");
    c.back_to(4);
    let keys: Vec<Vec<u8>> = c.iter().map(|c| c.key().to_vec()).collect();
    assert_eq!(keys, [b"k1".to_vec(), b"k3".to_vec(), b"k3b".to_vec()]);
    c.back_to(2);
    let keys: Vec<Vec<u8>> = c.iter().map(|c| c.key().to_vec()).collect();
    assert_eq!(keys, [b"k1".to_vec()]);
    c.put(6, tree(1), b"k6", b"v6");
    c.back_to(1);
    let keys: Vec<Vec<u8>> = c.iter().map(|c| c.key().to_vec()).collect();
    assert_eq!(keys, [b"k1".to_vec()]);
    c.back_to(0);
    assert!(c.is_empty());
    assert_eq!(c.iter().count(), 0);
}

/// Bytes cut short, of an unknown kind or a delete with a value read as corrupt, after every whole
/// change before them, and nothing after.
#[test]
fn bytes_that_hold_no_change_together_read_as_corrupt() {
    let mut c = Changes::new();
    c.put(1, tree(1), b"key", b"value");
    let whole = c.bytes().to_vec();
    let one = |bytes: &[u8]| -> Vec<bool> { Decode::new(bytes).map(|r| r.is_ok()).collect() };
    for cut in 1..whole.len() {
        let mut bytes = whole.clone();
        bytes.extend_from_slice(&whole[..cut]);
        assert_eq!(one(&bytes), vec![true, false], "cut at {cut}");
    }
    let mut kind = whole.clone();
    kind[0] = 9;
    assert_eq!(one(&kind), vec![false]);
    let mut delete = whole.clone();
    delete[0] = DELETE;
    assert_eq!(one(&delete), vec![false]);
    let mut create = whole.clone();
    create[0] = CREATE;
    assert_eq!(one(&create), vec![false], "a creation with a key");
    let mut drain = whole.clone();
    drain[0] = DRAIN;
    assert_eq!(one(&drain), vec![false], "a drain with a key");
}
