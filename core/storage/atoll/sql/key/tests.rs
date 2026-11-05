use super::*;
use crate::numeric::nonnan::NonNan;
use crate::types::cmp_in_column;
use crate::Value;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::cmp::Ordering;

/// Values chosen to meet every edge the encoding has: integers and reals next to 2^53 and at the
/// ends of their ranges, negative zero, infinities and subnormals; texts with NULs, case and
/// trailing spaces, accents, numbers and arrays in text form; blobs with zeros and 0xff.
fn values() -> Vec<Value> {
    let p53 = 1i64 << 53;
    let mut out = vec![Value::Null];
    for i in [
        0,
        1,
        -1,
        2,
        -2,
        7,
        p53,
        p53 + 1,
        p53 - 1,
        -p53,
        -p53 - 1,
        i64::MAX,
        i64::MIN,
        i64::MAX - 1,
        i64::MIN + 1,
        1000,
    ] {
        out.push(Value::Numeric(Numeric::Integer(i)));
    }
    for f in [
        0.0,
        -0.0,
        1.0,
        -1.0,
        0.5,
        -0.5,
        1.5,
        2.0,
        7.0,
        p53 as f64,
        (p53 as f64) * 2.0,
        1e300,
        -1e300,
        f64::MIN_POSITIVE,
        5e-324,
        -5e-324,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
        f64::MIN,
        0.1,
        1000.0,
        9.223372036854776e18,
    ] {
        out.push(Value::Numeric(Numeric::Float(NonNan::new(f).unwrap())));
    }
    for t in [
        "", "a", "A", "b", "B", "ab", "aB", "a ", "a  ", " a", "a\0", "a\0b", "a\0c", "A\0zz",
        "\0", "é", "e\u{301}", "E", "z", "Z", "1", "1.5", "1.50", "-2e3", "-2000", "0.000", "abc",
        "{1,2}", "{1,2,3}", "{}", "{2}", "{1}", "{1.0}", "{a,b}", "{a}", "{NULL,1}",
    ] {
        out.push(Value::build_text(t.to_string()));
    }
    for b in [
        &[][..],
        &[0],
        &[0, 0],
        &[0, 1],
        &[1],
        &[0xff],
        &[0xff, 0],
        &[1, 2, 3],
    ] {
        let mut blob = crate::alloc::vec![0u8; b.len()];
        blob.copy_from_slice(b);
        out.push(Value::from_blob(blob));
    }
    out
}

fn infos() -> Vec<KeyInfo> {
    let collations = [
        CollationSeq::BINARY,
        CollationSeq::NOCASE,
        CollationSeq::RTRIM,
        CollationSeq::new("en").unwrap(),
        CollationSeq::new("de").unwrap(),
        CollationSeq::Type(TypeOrdering::Numeric),
        CollationSeq::Type(TypeOrdering::Array),
        CollationSeq::Type(TypeOrdering::TextArray),
    ];
    let mut out = Vec::new();
    for collation in collations {
        for sort_order in [SortOrder::Asc, SortOrder::Desc] {
            for nulls_order in [None, Some(NullsOrder::First), Some(NullsOrder::Last)] {
                out.push(KeyInfo {
                    sort_order,
                    collation,
                    nulls_order,
                });
            }
        }
    }
    out
}

fn encoded(values: &[&Value], infos: &[KeyInfo]) -> Vec<u8> {
    let mut out = Vec::new();
    encode(values.iter().map(|v| v.as_value_ref()), infos, &mut out).unwrap();
    out
}

/// Every pair of values, under every collation, type ordering, sort order and NULLS order, compares
/// by its encodings as [`cmp_in_column`] compares it, and two values it calls equal encode alike.
#[test]
fn one_column_encodes_in_turso_order() {
    let values = values();
    let mut checked = 0usize;
    let mut equal_apart = 0usize;
    for info in infos() {
        for a in &values {
            let ea = encoded(&[a], &[info]);
            for b in &values {
                let want = cmp_in_column(&a.as_value_ref(), &b.as_value_ref(), &info);
                let eb = encoded(&[b], &[info]);
                assert_eq!(
                    ea.cmp(&eb),
                    want,
                    "{a:?} against {b:?} under {info:?}: {ea:02x?} {eb:02x?}"
                );
                equal_apart += usize::from(want == Ordering::Equal && a != b);
                checked += 1;
            }
        }
    }
    let n = values.len();
    assert_eq!(checked, infos().len() * n * n);
    // Distinct values a collation calls equal were met: 1 and 1.0, 'a' and 'A' under NOCASE, 'a'
    // and 'a ' under RTRIM, '1.5' and '1.50' as numerics.
    assert!(equal_apart > 0, "no two distinct values compared equal");
}

/// Keys of several columns compare as their columns do, the first that differs deciding, and the
/// encoding of the first columns is a prefix of the key's.
#[test]
fn keys_of_several_columns_encode_in_turso_order_and_prefixes_are_prefixes() {
    let values = values();
    let infos = infos();
    let mut rng = ChaCha8Rng::seed_from_u64(7);
    for _ in 0..200_000 {
        let n = rng.random_range(1..=3);
        let key_infos: Vec<KeyInfo> = (0..n)
            .map(|_| infos[rng.random_range(0..infos.len())])
            .collect();
        let row = |rng: &mut ChaCha8Rng| -> Vec<&Value> {
            (0..n)
                .map(|_| &values[rng.random_range(0..values.len())])
                .collect()
        };
        let (a, b) = (row(&mut rng), row(&mut rng));
        let want = a
            .iter()
            .zip(&b)
            .zip(&key_infos)
            .map(|((x, y), info)| cmp_in_column(&x.as_value_ref(), &y.as_value_ref(), info))
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal);
        let (ea, eb) = (encoded(&a, &key_infos), encoded(&b, &key_infos));
        assert_eq!(ea.cmp(&eb), want, "{a:?} against {b:?} under {key_infos:?}");
        let k = rng.random_range(1..=n);
        assert!(ea.starts_with(&encoded(&a[..k], &key_infos[..k])));
    }
}

/// A seek's bounds on a prefix: every key starting with the prefix lies at or above it and below
/// its successor, and nothing else does.
#[test]
fn a_prefixs_successor_bounds_exactly_the_keys_that_start_with_it() {
    let values = values();
    let info = KeyInfo {
        sort_order: SortOrder::Asc,
        collation: CollationSeq::BINARY,
        nulls_order: None,
    };
    let keys: Vec<Vec<u8>> = values
        .iter()
        .flat_map(|a| values.iter().map(move |b| (a, b)))
        .map(|(a, b)| encoded(&[a, b], &[info, info]))
        .collect();
    for a in &values {
        let prefix = encoded(&[a], &[info]);
        let upper = successor(&prefix).expect("a key prefix that is not all 0xff");
        for k in &keys {
            let inside = k.as_slice() >= prefix.as_slice() && k.as_slice() < upper.as_slice();
            assert_eq!(inside, k.starts_with(&prefix), "{a:?}: {k:02x?}");
        }
    }
    assert_eq!(successor(&[0xff, 0xff]), None);
    assert_eq!(successor(&[1, 0xff]), Some(vec![2]));
}

/// Rowid keys sort as their rowids, and read back.
#[test]
fn rowid_keys_sort_as_rowids_and_read_back() {
    let ids = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
    for &a in &ids {
        assert_eq!(rowid_of(&rowid(a)).unwrap(), a);
        for &b in &ids {
            assert_eq!(rowid(a).cmp(&rowid(b)), a.cmp(&b), "{a} against {b}");
        }
    }
    assert!(rowid_of(&[1, 2, 3]).is_err());
}

/// Golden bytes: the encoding of a fixed set of values under every collation and ordering. A change
/// to any of them is a new key encoding, which needs a new encoding revision (DESIGN.md §17, R6).
#[test]
fn golden_encodings_hold() {
    let cases: [(&str, Value, KeyInfo, &[u8]); 9] = [
        (
            "integer 1",
            Value::Numeric(Numeric::Integer(1)),
            info(CollationSeq::BINARY, SortOrder::Asc),
            &[0x24, 0x08, 0x00, 0x00],
        ),
        (
            "real 1.0",
            Value::Numeric(Numeric::Float(NonNan::new(1.0).unwrap())),
            info(CollationSeq::BINARY, SortOrder::Asc),
            &[0x24, 0x08, 0x00, 0x00],
        ),
        (
            "integer -3",
            Value::Numeric(Numeric::Integer(-3)),
            info(CollationSeq::BINARY, SortOrder::Asc),
            &[0x22, 0xf7, 0xfe, 0x7f],
        ),
        (
            "text a\\0b",
            Value::build_text("a\0b".to_string()),
            info(CollationSeq::BINARY, SortOrder::Asc),
            &[0x30, b'a', 0x00, 0xff, b'b', 0x00, 0x00],
        ),
        (
            "text Ab nocase",
            Value::build_text("Ab".to_string()),
            info(CollationSeq::NOCASE, SortOrder::Asc),
            &[0x30, b'a', b'b', 0x00, 0x00],
        ),
        (
            "text a\\0 nocase",
            Value::build_text("a\0".to_string()),
            info(CollationSeq::NOCASE, SortOrder::Asc),
            &[0x30, b'a', 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 2],
        ),
        (
            "null desc",
            Value::Null,
            info(CollationSeq::BINARY, SortOrder::Desc),
            &[NULL_LAST],
        ),
        (
            "numeric 1.50",
            Value::build_text("1.50".to_string()),
            info(CollationSeq::Type(TypeOrdering::Numeric), SortOrder::Asc),
            &[0x24, 0x80, 0, 0, 0, 0, 0, 0, 0x01, 0x1e],
        ),
        (
            "array {1}",
            Value::build_text("{1}".to_string()),
            info(CollationSeq::Type(TypeOrdering::Array), SortOrder::Asc),
            &[0x50, 0x24, 0x08, 0x00, 0x00, 0x00],
        ),
    ];
    for (name, value, info, want) in cases {
        assert_eq!(encoded(&[&value], &[info]), want, "{name}");
    }
}

fn info(collation: CollationSeq, sort_order: SortOrder) -> KeyInfo {
    KeyInfo {
        sort_order,
        collation,
        nulls_order: None,
    }
}

/// The digits below a number's leading one, as integer operations emit them, are the bytes of the
/// digit-by-digit reading: each digit in turn, trailing zeros dropped, seven to a byte with the
/// low bit saying whether more follow, for every position of the leading digit and values with
/// every count of trailing zeros.
#[test]
fn digits_below_the_leading_one_are_the_digit_by_digit_bytes() {
    fn by_digit(value: u64, top: u32) -> Vec<u8> {
        let mut digits: Vec<bool> = (0..top).rev().map(|i| (value >> i) & 1 == 1).collect();
        while digits.last() == Some(&false) {
            digits.pop();
        }
        if digits.is_empty() {
            return vec![0];
        }
        let groups: Vec<&[bool]> = digits.chunks(7).collect();
        let mut out = Vec::new();
        for (i, group) in groups.iter().enumerate() {
            let mut v = 0u8;
            for (j, &digit) in group.iter().enumerate() {
                v |= (digit as u8) << (6 - j);
            }
            out.push((v << 1) | (i + 1 < groups.len()) as u8);
        }
        out
    }
    let mut rng = ChaCha8Rng::seed_from_u64(23);
    let mut cases = 0;
    for top in 0..64u32 {
        for zeros in 0..=top {
            for _ in 0..8 {
                let low = if top == 0 {
                    0
                } else {
                    rng.random::<u64>() >> (64 - top)
                };
                let value = (1u64 << top) | (low >> zeros << zeros);
                let mut out = Vec::new();
                digits_below(value, top, &mut out);
                assert_eq!(out, by_digit(value, top), "{value:#x} below digit {top}");
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 8 * (1..=64).sum::<usize>());
}
