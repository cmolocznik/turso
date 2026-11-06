//! A transaction's own log: what it appends reads back in order, a statement's rollback drops its
//! changes and every later one's, and bytes that do not hold a change together read as corrupt.

use super::*;
use crate::storage::atoll::log::GroupStart;
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

/// In chunks, a value runs on across chunks while every change's head and key lie whole in one;
/// the changes are found again from any chunk or part where they start; a rollback into what was
/// spilled cuts the parts there, and the next chunk begins there.
#[test]
fn chunks_hold_heads_whole_and_a_rollback_into_a_part_cuts_it() {
    let chunk = 4096;
    let mut c = Changes::in_chunks(None, chunk, None);
    let mut made = Vec::new();
    let mut statement = 1;
    for i in 0..40u32 {
        if i % 10 == 0 {
            statement += 1;
        }
        let value = vec![
            i as u8;
            if i % 7 == 3 {
                9000
            } else {
                100 + i as usize * 13
            }
        ];
        made.push((c.len(), statement, i.to_be_bytes().to_vec()));
        c.put(statement, tree(1), &i.to_be_bytes(), &value);
        // Spilled as a transaction's write does, each chunk sealed by the change before.
        while c.has_sealed() {
            let at = GroupStart::at((c.parts().len() as u64 + 1) * 4096);
            let (from, len, first, _block) = c.take_sealed();
            c.spilled(Part {
                from,
                len: len as u64,
                first,
                at,
                lsn: at.get() + 1,
            });
        }
        assert!(
            c.capacity() <= 2 * chunk,
            "{} bytes of chunks held",
            c.capacity()
        );
    }
    assert!(c.parts().len() > 10, "{} parts", c.parts().len());
    // Every change found where it starts, from the chunk in memory and the parts spilled, as the
    // parts' bytes would be read back.
    let mut found = Vec::new();
    let tail = c.chunk(0);
    found.extend(starts(tail.2, tail.0, tail.1, 0).unwrap());
    assert_eq!(c.sources_back(0).len(), c.parts().len() + 1);
    for &(at, s, ref key) in &made {
        assert_eq!(c.statement_at(at), s);
        if at >= tail.0 {
            assert_eq!(c.keyed_at(at).unwrap().key, &key[..]);
        }
    }
    let (in_memory, _) = made.iter().partition::<Vec<_>, _>(|m| m.0 >= tail.0);
    assert_eq!(found, in_memory.iter().map(|m| m.0).collect::<Vec<_>>());
    // Back to statement 3: statement 4's changes and every later one's go, those spilled cut off.
    let from = c.begin_after(3).unwrap();
    assert_eq!(from, made[20].0);
    c.back_to(3);
    assert_eq!(c.len(), from);
    assert!(c
        .parts()
        .iter()
        .all(|p| p.from < from && p.from + p.len <= from));
    let last = c.parts().last().unwrap();
    assert_eq!(
        last.from + last.len,
        from,
        "the parts end where statement 4 began"
    );
    assert_eq!(c.tail(), (from, &[][..]), "the next chunk begins there");
    c.put(5, tree(1), b"after", b"v");
    assert_eq!(c.tail().0, from);
    assert_eq!(c.statement_at(from), 5);
}

/// Its memory counts every statement's mark beside its chunks and parts, and its slot's next
/// transaction inherits no more room for them than a chunk's (audit 2026-11-06).
#[test]
fn memory_counts_statement_marks_and_a_recycle_gives_back_a_large_transactions() {
    let chunk = 4096;
    let mut c = Changes::in_chunks(None, chunk, None);
    let mark = std::mem::size_of::<(Statement, u64)>();
    let statements = 20_000u32;
    for s in 1..=statements {
        c.put(s, tree(1), &s.to_be_bytes(), b"v");
        while c.has_sealed() {
            let at = GroupStart::at((c.parts().len() as u64 + 1) * 4096);
            let (from, len, first, _block) = c.take_sealed();
            c.spilled(Part {
                from,
                len: len as u64,
                first,
                at,
                lsn: at.get() + 1,
            });
        }
    }
    let marks = statements as usize * mark;
    assert!(
        c.memory() >= c.capacity() + marks,
        "{} bytes counted for {marks} bytes of marks and {} of chunks",
        c.memory(),
        c.capacity()
    );
    c.recycle();
    assert!(
        c.memory() <= 3 * chunk,
        "{} bytes kept for the next transaction",
        c.memory()
    );
}
