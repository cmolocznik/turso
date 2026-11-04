//! Keys of SQL trees as byte strings whose order is Turso's (DESIGN.md §13, §17).
//!
//! - **Rowid tables**: the rowid, 8 bytes big-endian with the sign bit flipped.
//! - **Indexes and WITHOUT ROWID tables**: each key column encoded so that comparing the bytes
//!   compares the values as [`cmp_in_column`](crate::types::cmp_in_column) does under the column's
//!   [`KeyInfo`]: its collation or type ordering, its sort order and its NULLS order.
//!
//! Every column's encoding is self-delimiting, so the encoding of a prefix of the columns is a
//! prefix of the key, and two values a collation calls equal encode to the same bytes, which makes
//! an insert of an equal key the overwrite SQLite's B-tree makes. Values out of their column's
//! order are refused: a custom collation, which no table or index may use.
//!
//! A column's encoding is a tag, then its body:
//!
//! - NULL is one byte, [`NULL_FIRST`] or [`NULL_LAST`], never inverted, so its place holds whatever
//!   the sort order.
//! - Other values have a tag between those, so a descending column inverts every byte of the tag and
//!   body and stays between them.
//! - Numbers under a text collation are exact binary values: 2^53 + 1 and 2^53 stay apart, and 1
//!   and 1.0 meet. Under the numeric type ordering they are decimals, normalized, as that order
//!   compares them through their shortest decimal form.
//! - Text and blob bodies escape each zero byte as `00 ff` and end with `00 00`.

use crate::numeric::decimal::NumericSortKey;
use crate::numeric::Numeric;
use crate::translate::collate::{CollationSeq, TextCollation};
use crate::type_ordering::TypeOrdering;
use crate::types::{AsValueRef, KeyInfo, ValueRef};
use crate::vdbe::array::{array_values_from_blob, parse_text_array, parse_text_array_of_texts};
use crate::{LimboError, Result};
use bigdecimal::{BigDecimal, Zero};
use num_bigint::Sign;
use turso_parser::ast::{NullsOrder, SortOrder};

/// The encoding revision this build writes, recorded on every tree with encoded keys (DESIGN.md
/// §17): any change to the bytes of an encoding bumps it.
pub const ENCODING: u32 = 1;

/// A NULL that sorts before every value of its column.
pub const NULL_FIRST: u8 = 0x01;
/// A NULL that sorts after every value of its column.
pub const NULL_LAST: u8 = 0xfe;
const NEGATIVE_INFINITY: u8 = 0x21;
const NEGATIVE: u8 = 0x22;
const ZERO: u8 = 0x23;
const POSITIVE: u8 = 0x24;
const INFINITY: u8 = 0x25;
const TEXT: u8 = 0x30;
const BLOB: u8 = 0x40;
const ARRAY: u8 = 0x50;
const OTHER: u8 = 0x60;
/// The end of an array's elements: below every element's first byte, so a shorter array sorts
/// first.
const ARRAY_END: u8 = 0x00;

/// The bias that makes a binary exponent, from -1074 for the least subnormal real to 63 for the
/// largest integer magnitude, a positive big-endian number.
const BINARY_EXPONENT_BIAS: i32 = 2048;

/// A rowid table's key for `rowid`.
pub fn rowid(rowid: i64) -> [u8; 8] {
    ((rowid as u64) ^ (1 << 63)).to_be_bytes()
}

/// The rowid a rowid table's key holds.
pub fn rowid_of(key: &[u8]) -> Result<i64> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| LimboError::Corrupt(format!("a rowid key of {} bytes", key.len())))?;
    Ok((u64::from_be_bytes(bytes) ^ (1 << 63)) as i64)
}

/// Appends the encoding of the key whose columns are `values`, each under its `KeyInfo`.
pub fn encode<'a>(
    values: impl IntoIterator<Item = ValueRef<'a>>,
    infos: &[KeyInfo],
    out: &mut Vec<u8>,
) -> Result<()> {
    for (value, info) in values.into_iter().zip(infos) {
        column(value, info, out)?;
    }
    Ok(())
}

/// Appends the encoding of one column's value.
pub fn column(value: ValueRef, info: &KeyInfo, out: &mut Vec<u8>) -> Result<()> {
    if let ValueRef::Null = value {
        let first = match info.nulls_order {
            Some(NullsOrder::First) => true,
            Some(NullsOrder::Last) => false,
            // Every collation and type ordering puts NULL first; descending turns that around.
            None => info.sort_order == SortOrder::Asc,
        };
        out.push(if first { NULL_FIRST } else { NULL_LAST });
        return Ok(());
    }
    let start = out.len();
    match info.collation {
        CollationSeq::Text(collation) => sqlite_value(value, collation, out)?,
        CollationSeq::Type(TypeOrdering::Numeric) => numeric(value, out),
        CollationSeq::Type(TypeOrdering::Array) => array(value, parse_text_array, out),
        CollationSeq::Type(TypeOrdering::TextArray) => array(value, parse_text_array_of_texts, out),
    }
    if info.sort_order == SortOrder::Desc {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
    Ok(())
}

/// A value in SQLite's order: numbers, then texts by `collation`, then blobs.
fn sqlite_value(value: ValueRef, collation: TextCollation, out: &mut Vec<u8>) -> Result<()> {
    match value {
        ValueRef::Null => out.push(NULL_FIRST),
        ValueRef::Numeric(n) => binary_number(n, out),
        ValueRef::Text(text) => {
            out.push(TEXT);
            let text = text.as_str();
            match collation {
                TextCollation::Binary => escaped(text.as_bytes(), out),
                TextCollation::Rtrim => escaped(text.trim_end_matches(' ').as_bytes(), out),
                TextCollation::NoCase => nocase(text, out),
                TextCollation::Locale(id) => {
                    escaped(&TextCollation::locale_sort_key(id, text), out)
                }
                TextCollation::Custom(_) => {
                    return Err(LimboError::InternalError(
                        "a custom collation has no key encoding: no table or index may use one"
                            .into(),
                    ))
                }
            }
        }
        ValueRef::Blob(blob) => {
            out.push(BLOB);
            escaped(blob, out);
        }
    }
    Ok(())
}

/// `bytes` with each zero escaped as `00 ff`, ended by `00 00`: the encodings of two byte strings
/// compare as the strings do, a prefix first.
fn escaped(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == 0 {
            out.push(0xff);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// NOCASE as Turso compares it: byte by byte, ASCII folded to lower case, up to the first NUL both
/// share, where the total lengths decide. So the folded bytes before a string's first NUL, then
/// `00 01` and the whole length, or `00 00` for a string with no NUL.
fn nocase(text: &str, out: &mut Vec<u8>) {
    for b in text.bytes() {
        if b == 0 {
            out.extend_from_slice(&[0, 1]);
            out.extend_from_slice(&(text.len() as u64).to_be_bytes());
            return;
        }
        out.push(b.to_ascii_lowercase());
    }
    out.extend_from_slice(&[0, 0]);
}

/// An integer or a real by its exact value: the tag of its sign, then the position of its leading
/// binary digit, biased, and the digits after it, seven to a byte with the low bit saying whether
/// more follow. A negative number's body is inverted, so a larger magnitude sorts first.
fn binary_number(n: Numeric, out: &mut Vec<u8>) {
    let (negative, exponent, fraction) = match n {
        Numeric::Integer(0) => return out.push(ZERO),
        Numeric::Integer(i) => {
            let magnitude = i.unsigned_abs();
            let top = 63 - magnitude.leading_zeros();
            (i < 0, top as i32, bits_below(magnitude, top))
        }
        Numeric::Float(f) => {
            let f = f64::from(f);
            if f == 0.0 {
                return out.push(ZERO);
            }
            if f.is_infinite() {
                return out.push(if f < 0.0 { NEGATIVE_INFINITY } else { INFINITY });
            }
            let bits = f.to_bits();
            let biased = ((bits >> 52) & 0x7ff) as i32;
            let stored = bits & ((1 << 52) - 1);
            // value = significand × 2^power
            let (significand, power) = if biased == 0 {
                (stored, -1074)
            } else {
                (stored | (1 << 52), biased - 1075)
            };
            let top = 63 - significand.leading_zeros();
            (f < 0.0, power + top as i32, bits_below(significand, top))
        }
    };
    out.push(if negative { NEGATIVE } else { POSITIVE });
    let start = out.len();
    out.extend_from_slice(&((exponent + BINARY_EXPONENT_BIAS) as u16).to_be_bytes());
    seven_bit_groups(&fraction, out);
    if negative {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
}

/// The `top` binary digits of `value` below its digit `top`, most significant first, without the
/// trailing zeros.
fn bits_below(value: u64, top: u32) -> Vec<bool> {
    let mut bits: Vec<bool> = (0..top).rev().map(|i| ((value >> i) & 1) == 1).collect();
    while bits.last() == Some(&false) {
        bits.pop();
    }
    bits
}

/// `bits` seven to a byte, zero-padded, each byte's low bit set when another byte follows; none
/// gives one zero byte.
fn seven_bit_groups(bits: &[bool], out: &mut Vec<u8>) {
    if bits.is_empty() {
        return out.push(0);
    }
    let groups: Vec<&[bool]> = bits.chunks(7).collect();
    for (i, group) in groups.iter().enumerate() {
        let mut v = 0u8;
        for (j, &bit) in group.iter().enumerate() {
            v |= (bit as u8) << (6 - j);
        }
        let more = (i + 1 < groups.len()) as u8;
        out.push((v << 1) | more);
    }
}

/// A value in the numeric type's order: NULL, negative infinity, numbers by decimal value,
/// infinity, then texts and blobs that are not numbers, each by their bytes.
fn numeric(value: ValueRef, out: &mut Vec<u8>) {
    match NumericSortKey::of(value) {
        NumericSortKey::Null => out.push(NULL_FIRST),
        NumericSortKey::NegativeInfinity => out.push(NEGATIVE_INFINITY),
        NumericSortKey::Number(number) => decimal(&number, out),
        NumericSortKey::Infinity => out.push(INFINITY),
        NumericSortKey::Text(text) => {
            out.push(TEXT);
            escaped(text.as_bytes(), out);
        }
        NumericSortKey::Blob(blob) => {
            out.push(BLOB);
            escaped(blob, out);
        }
    }
}

/// A decimal by its value: the tag of its sign, then the position of the power of ten just above
/// its leading digit, biased, and its digits without trailing zeros, two to a byte with the low bit
/// saying whether more follow. A negative number's body is inverted.
fn decimal(number: &BigDecimal, out: &mut Vec<u8>) {
    if number.is_zero() {
        return out.push(ZERO);
    }
    let (digits, scale) = number.normalized().into_bigint_and_exponent();
    let negative = digits.sign() == Sign::Minus;
    let digits = digits.magnitude().to_str_radix(10);
    // value = 0.d1 d2 ... × 10^position
    let position = digits.len() as i64 - scale;
    out.push(if negative { NEGATIVE } else { POSITIVE });
    let start = out.len();
    out.extend_from_slice(&((position as u64) ^ (1 << 63)).to_be_bytes());
    let digits = digits.as_bytes();
    let pairs: Vec<&[u8]> = digits.chunks(2).collect();
    for (i, pair) in pairs.iter().enumerate() {
        let high = pair[0] - b'0';
        let low = pair.get(1).map_or(0, |d| d - b'0');
        let more = (i + 1 < pairs.len()) as u8;
        out.push(((high * 10 + low) << 1) | more);
    }
    if negative {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
}

/// A value in an array type's order: NULL; arrays, element by element in SQLite's order and a
/// shorter one first; then any other value in SQLite's order. `parse_text` reads an array in text
/// form.
fn array(value: ValueRef, parse_text: fn(&str) -> Option<Vec<crate::Value>>, out: &mut Vec<u8>) {
    let elements = match value {
        ValueRef::Null => return out.push(NULL_FIRST),
        ValueRef::Blob(blob) => array_values_from_blob(blob).ok(),
        ValueRef::Text(text) => parse_text(text.as_str()),
        ValueRef::Numeric(_) => None,
    };
    match elements {
        Some(elements) => {
            out.push(ARRAY);
            for element in &elements {
                sqlite_value(element.as_value_ref(), TextCollation::Binary, out)
                    .expect("BINARY has an encoding");
            }
            out.push(ARRAY_END);
        }
        None => {
            out.push(OTHER);
            sqlite_value(value, TextCollation::Binary, out).expect("BINARY has an encoding");
        }
    }
}

/// The least byte string above every string that starts with `prefix`: `prefix` without its
/// trailing `ff` bytes, its last byte one higher. `None` when every byte is `ff`: no string is
/// above them all.
pub fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(&last) = out.last() {
        if last == 0xff {
            out.pop();
        } else {
            *out.last_mut().expect("a byte") += 1;
            return Some(out);
        }
    }
    None
}

#[cfg(test)]
mod tests;
