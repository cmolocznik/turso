//! A regular expression's trigram query: a query over the trigrams a text holds that every text
//! the expression matches satisfies, and most others do not, so an index of trigrams can narrow
//! the texts to run the expression on.
//!
//! This is Russ Cox's `index/regexp.go` from Google's codesearch (Copyright 2011 The Go Authors,
//! BSD-3-Clause; see "Regular Expression Matching with a Trigram Index", 2012), ported from Go's
//! `regexp/syntax` to `regex-syntax`'s HIR, step for step. Strings are bytes: a trigram is three
//! bytes of the text's UTF-8, as codesearch indexes them.

use regex_syntax::hir::{Class, Hir, HirKind};

/// Three bytes of a text.
pub(crate) type Trigram = [u8; 3];

/// Exact sets are limited to this many strings (codesearch's `maxExact`).
const MAX_EXACT: usize = 7;
/// Prefix and suffix sets are limited to this many strings (codesearch's `maxSet`).
const MAX_SET: usize = 20;
/// A character class of more characters than this is any character (codesearch's limit).
const MAX_CLASS: u32 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    /// Every text.
    All,
    /// No text.
    None,
    /// Every trigram and every sub-query.
    And,
    /// At least one trigram or sub-query.
    Or,
}

/// A query over the trigrams of a text. `trigrams` is kept sorted where codesearch keeps it so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Query {
    pub(crate) op: Op,
    pub(crate) trigrams: Vec<Trigram>,
    pub(crate) sub: Vec<Query>,
}

impl Query {
    pub(crate) fn all() -> Self {
        Self::of(Op::All)
    }

    pub(crate) fn none() -> Self {
        Self::of(Op::None)
    }

    fn of(op: Op) -> Self {
        Self {
            op,
            trigrams: Vec::new(),
            sub: Vec::new(),
        }
    }

    fn and(self, r: Query) -> Query {
        self.and_or(r, Op::And)
    }

    fn or(self, r: Query) -> Query {
        self.and_or(r, Op::Or)
    }

    /// `self` AND `r`, or `self` OR `r`, simplified as codesearch's `andOr` simplifies.
    fn and_or(mut self, mut r: Query, op: Op) -> Query {
        if self.trigrams.is_empty() && self.sub.len() == 1 {
            self = self.sub.pop().expect("one sub-query");
        }
        if r.trigrams.is_empty() && r.sub.len() == 1 {
            r = r.sub.pop().expect("one sub-query");
        }
        // If q ⇒ r, q AND r ≡ q and q OR r ≡ r.
        if self.implies(&r) {
            return if op == Op::And { self } else { r };
        }
        if r.implies(&self) {
            return if op == Op::And { r } else { self };
        }
        // Both are AND or OR: if they match or can be made to, merge.
        let q_atom = self.trigrams.len() == 1 && self.sub.is_empty();
        let r_atom = r.trigrams.len() == 1 && r.sub.is_empty();
        if self.op == op && (r.op == op || r_atom) {
            self.trigrams = union(self.trigrams, r.trigrams);
            self.sub.extend(r.sub);
            return self;
        }
        if r.op == op && q_atom {
            r.trigrams = union(r.trigrams, self.trigrams);
            return r;
        }
        if q_atom && r_atom {
            self.op = op;
            self.trigrams.extend(r.trigrams);
            return self;
        }
        // If one matches the op, add the other to it.
        if self.op == op {
            self.sub.push(r);
            return self;
        }
        if r.op == op {
            r.sub.push(self);
            return r;
        }
        // An AND of ORs or an OR of ANDs: factor out the trigrams they share, as
        // (abc|def|ghi) AND (abc|def|mno) => (abc|def) OR ((ghi) AND (mno)).
        let mut common = Vec::new();
        let (mut qt, mut rt) = (Vec::new(), Vec::new());
        let (mut i, mut j) = (0, 0);
        while i < self.trigrams.len() && j < r.trigrams.len() {
            let (a, b) = (self.trigrams[i], r.trigrams[j]);
            match a.cmp(&b) {
                std::cmp::Ordering::Less => {
                    qt.push(a);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    rt.push(b);
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    common.push(a);
                    i += 1;
                    j += 1;
                }
            }
        }
        qt.extend_from_slice(&self.trigrams[i..]);
        rt.extend_from_slice(&r.trigrams[j..]);
        self.trigrams = qt;
        r.trigrams = rt;
        if !common.is_empty() {
            let s = self.and_or(r, op);
            let other = if op == Op::And { Op::Or } else { Op::And };
            let t = Query {
                op: other,
                trigrams: common,
                sub: Vec::new(),
            };
            return t.and_or(s, other);
        }
        Query {
            op,
            trigrams: Vec::new(),
            sub: vec![self, r],
        }
    }

    /// Whether `self` implies `r`; a false negative is fine.
    fn implies(&self, r: &Query) -> bool {
        if self.op == Op::None || r.op == Op::All {
            return true;
        }
        if self.op == Op::All || r.op == Op::None {
            return false;
        }
        if self.op == Op::And
            || (self.op == Op::Or && self.trigrams.len() == 1 && self.sub.is_empty())
        {
            return trigrams_imply(&self.trigrams, r);
        }
        self.op == Op::Or
            && r.op == Op::Or
            && !self.trigrams.is_empty()
            && self.sub.is_empty()
            && is_subset(&self.trigrams, &r.trigrams)
    }

    /// `self` AND the OR of the AND of the trigrams of each string of `t`.
    fn and_trigrams(self, t: &[Vec<u8>]) -> Query {
        if min_len(t) < 3 {
            // A short string guarantees no trigram: AND ALL is `self`.
            return self;
        }
        let mut or = Query::none();
        for s in t {
            let mut trigrams: Vec<Trigram> = s.windows(3).map(|w| [w[0], w[1], w[2]]).collect();
            trigrams.sort_unstable();
            trigrams.dedup();
            or = or.or(Query {
                op: Op::And,
                trigrams,
                sub: Vec::new(),
            });
        }
        self.and(or)
    }

    /// Whether a text holding exactly the trigrams `has` answers may satisfy the query.
    #[cfg(test)]
    pub(crate) fn admits(&self, has: &dyn Fn(&Trigram) -> bool) -> bool {
        match self.op {
            Op::All => true,
            Op::None => false,
            Op::And => self.trigrams.iter().all(has) && self.sub.iter().all(|sub| sub.admits(has)),
            Op::Or => self.trigrams.iter().any(has) || self.sub.iter().any(|sub| sub.admits(has)),
        }
    }

    /// Every trigram the query names, sorted, each once.
    pub(crate) fn every_trigram(&self) -> Vec<Trigram> {
        let mut out = Vec::new();
        self.collect_trigrams(&mut out);
        out.sort_unstable();
        out.dedup();
        out
    }

    fn collect_trigrams(&self, out: &mut Vec<Trigram>) {
        out.extend_from_slice(&self.trigrams);
        for sub in &self.sub {
            sub.collect_trigrams(out);
        }
    }
}

impl std::fmt::Display for Query {
    /// Codesearch's notation: `+` for every text, `-` for none, an AND's terms by spaces, an OR's
    /// by `|`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.op {
            Op::None => return f.write_str("-"),
            Op::All => return f.write_str("+"),
            _ => {}
        }
        if self.sub.is_empty() && self.trigrams.len() == 1 {
            return write!(f, "{}", quote(&self.trigrams[0]));
        }
        let (start, sjoin, end, tjoin) = if self.op == Op::And {
            ("", " ", "", " ")
        } else {
            ("(", ")|(", ")", "|")
        };
        f.write_str(start)?;
        for (i, t) in self.trigrams.iter().enumerate() {
            if i > 0 {
                f.write_str(tjoin)?;
            }
            write!(f, "{}", quote(t))?;
        }
        if !self.sub.is_empty() {
            if !self.trigrams.is_empty() {
                f.write_str(sjoin)?;
            }
            write!(f, "{}", self.sub[0])?;
            for sub in &self.sub[1..] {
                write!(f, "{sjoin}{sub}")?;
            }
        }
        f.write_str(end)
    }
}

fn quote(bytes: &[u8]) -> String {
    let mut out = String::from("\"");
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                c if c.is_control() => out.push_str(&format!("\\x{:02x}", c as u32)),
                c => out.push(c),
            }
        }
        for byte in chunk.invalid() {
            out.push_str(&format!("\\x{byte:02x}"));
        }
    }
    out.push('"');
    out
}

fn trigrams_imply(t: &[Trigram], q: &Query) -> bool {
    match q.op {
        Op::Or => {
            q.sub.iter().any(|qq| trigrams_imply(t, qq))
                || t.iter()
                    .any(|x| is_subset(std::slice::from_ref(x), &q.trigrams))
        }
        Op::And => q.sub.iter().all(|qq| trigrams_imply(t, qq)) && is_subset(&q.trigrams, t),
        _ => false,
    }
}

/// Whether every member of `s` is in `t`; both sorted.
fn is_subset(s: &[Trigram], t: &[Trigram]) -> bool {
    let mut j = 0;
    for x in s {
        while j < t.len() && t[j] < *x {
            j += 1;
        }
        if j >= t.len() || t[j] != *x {
            return false;
        }
    }
    true
}

fn union(mut s: Vec<Trigram>, t: Vec<Trigram>) -> Vec<Trigram> {
    s.extend(t);
    s.sort_unstable();
    s.dedup();
    s
}

/// The query for `hir`: every text it matches somewhere satisfies the query.
pub(crate) fn query_of(hir: &Hir) -> Query {
    let mut info = analyze(hir);
    info.simplify(true);
    info.add_exact();
    info.matching
}

/// What analyzing an expression found (codesearch's `regexpInfo`).
#[derive(Debug, Clone)]
struct Info {
    /// Whether it matches the empty string.
    can_empty: bool,
    /// The exact set of strings it matches, if known.
    exact: Option<Vec<Vec<u8>>>,
    /// Without `exact`: the set of possible prefixes and of possible suffixes of a match.
    prefix: Vec<Vec<u8>>,
    suffix: Vec<Vec<u8>>,
    /// A query every match satisfies, beyond what the sets say.
    matching: Query,
}

impl Info {
    fn any_match() -> Self {
        Self {
            can_empty: true,
            exact: None,
            prefix: vec![Vec::new()],
            suffix: vec![Vec::new()],
            matching: Query::all(),
        }
    }

    fn any_char() -> Self {
        Self {
            can_empty: false,
            exact: None,
            prefix: vec![Vec::new()],
            suffix: vec![Vec::new()],
            matching: Query::all(),
        }
    }

    fn no_match() -> Self {
        Self {
            can_empty: false,
            exact: None,
            prefix: Vec::new(),
            suffix: Vec::new(),
            matching: Query::none(),
        }
    }

    fn empty_string() -> Self {
        Self {
            can_empty: true,
            exact: Some(vec![Vec::new()]),
            prefix: Vec::new(),
            suffix: Vec::new(),
            matching: Query::all(),
        }
    }

    fn exact(strings: Vec<Vec<u8>>) -> Self {
        Self {
            can_empty: false,
            exact: Some(strings),
            prefix: Vec::new(),
            suffix: Vec::new(),
            matching: Query::all(),
        }
    }

    /// Adds the trigrams of the exact set to the query.
    fn add_exact(&mut self) {
        if let Some(exact) = &self.exact {
            let matching = std::mem::replace(&mut self.matching, Query::all());
            self.matching = matching.and_trigrams(exact);
        }
    }

    /// When the exact set has grown too large, moves it into the query and the prefix and
    /// suffix sets.
    fn simplify(&mut self, force: bool) {
        if let Some(exact) = &mut self.exact {
            clean(exact, false);
        }
        let too_big = self.exact.as_ref().is_some_and(|exact| {
            exact.len() > MAX_EXACT || (min_len(exact) >= 3 && force) || min_len(exact) >= 4
        });
        if too_big {
            self.add_exact();
            for s in self.exact.take().expect("an exact set") {
                let n = s.len();
                if n < 3 {
                    self.prefix.push(s.clone());
                    self.suffix.push(s);
                } else {
                    self.prefix.push(s[..2].to_vec());
                    self.suffix.push(s[n - 2..].to_vec());
                }
            }
        }
        if self.exact.is_none() {
            let mut prefix = std::mem::take(&mut self.prefix);
            self.simplify_set(&mut prefix, false);
            self.prefix = prefix;
            let mut suffix = std::mem::take(&mut self.suffix);
            self.simplify_set(&mut suffix, true);
            self.suffix = suffix;
        }
    }

    /// Shrinks a prefix or suffix set, moving what it says into the query as it goes.
    fn simplify_set(&mut self, set: &mut Vec<Vec<u8>>, is_suffix: bool) {
        clean(set, is_suffix);
        let matching = std::mem::replace(&mut self.matching, Query::all());
        self.matching = matching.and_trigrams(set);
        let mut n: usize = 3;
        loop {
            if n != 3 && set.len() <= MAX_SET {
                break;
            }
            // Replace the set by its strings of length n-1.
            let mut kept: Vec<Vec<u8>> = Vec::with_capacity(set.len());
            for s in set.drain(..) {
                let s = if s.len() >= n {
                    if is_suffix {
                        s[s.len() - n + 1..].to_vec()
                    } else {
                        s[..n - 1].to_vec()
                    }
                } else {
                    s
                };
                if kept.last() != Some(&s) {
                    kept.push(s);
                }
            }
            *set = kept;
            clean(set, is_suffix);
            n -= 1;
        }
        // A prefix ("ab") makes the longer ones it starts ("abc") say nothing more.
        let mut kept: Vec<Vec<u8>> = Vec::with_capacity(set.len());
        for s in set.drain(..) {
            let redundant = kept.last().is_some_and(|last: &Vec<u8>| {
                if is_suffix {
                    s.ends_with(last)
                } else {
                    s.starts_with(last)
                }
            });
            if !redundant {
                kept.push(s);
            }
        }
        *set = kept;
    }
}

fn analyze(hir: &Hir) -> Info {
    let mut info = match hir.kind() {
        HirKind::Empty | HirKind::Look(_) => return Info::empty_string(),
        HirKind::Literal(literal) => Info::exact(vec![literal.0.to_vec()]),
        HirKind::Class(class) => match class_strings(class) {
            ClassStrings::None => return Info::no_match(),
            ClassStrings::Many => return Info::any_char(),
            ClassStrings::These(strings) => Info::exact(strings),
        },
        HirKind::Capture(capture) => return analyze(&capture.sub),
        HirKind::Concat(subs) => return fold(concat, subs, Info::empty_string()),
        HirKind::Alternation(subs) => return fold(alternate, subs, Info::no_match()),
        HirKind::Repetition(repetition) => match (repetition.min, repetition.max) {
            // x?
            (0, Some(1)) => return alternate(analyze(&repetition.sub), Info::empty_string()),
            // x*, or any other that may be empty: assume the worst.
            (0, _) => return Info::any_match(),
            (1, Some(1)) => return analyze(&repetition.sub),
            // x+: there is at least one x, so prefixes and suffixes stay; an exact x is no more.
            _ => {
                let mut info = analyze(&repetition.sub);
                if let Some(exact) = info.exact.take() {
                    info.prefix = exact.clone();
                    info.suffix = exact;
                }
                info
            }
        },
    };
    info.simplify(false);
    info
}

enum ClassStrings {
    None,
    Many,
    These(Vec<Vec<u8>>),
}

/// The strings a character class matches, unless they are too many.
fn class_strings(class: &Class) -> ClassStrings {
    match class {
        Class::Unicode(class) => {
            let ranges = class.ranges();
            if ranges.is_empty() {
                return ClassStrings::None;
            }
            let n: u32 = ranges
                .iter()
                .map(|range| u32::from(range.end()) - u32::from(range.start()))
                .sum();
            if n > MAX_CLASS {
                return ClassStrings::Many;
            }
            let mut strings = Vec::new();
            for range in ranges {
                for c in
                    (u32::from(range.start())..=u32::from(range.end())).filter_map(char::from_u32)
                {
                    let mut buffer = [0u8; 4];
                    strings.push(c.encode_utf8(&mut buffer).as_bytes().to_vec());
                }
            }
            ClassStrings::These(strings)
        }
        Class::Bytes(class) => {
            let ranges = class.ranges();
            if ranges.is_empty() {
                return ClassStrings::None;
            }
            let n: u32 = ranges
                .iter()
                .map(|range| u32::from(range.end()) - u32::from(range.start()))
                .sum();
            if n > MAX_CLASS {
                return ClassStrings::Many;
            }
            let mut strings = Vec::new();
            for range in ranges {
                for byte in range.start()..=range.end() {
                    strings.push(vec![byte]);
                }
            }
            ClassStrings::These(strings)
        }
    }
}

fn fold(f: fn(Info, Info) -> Info, subs: &[Hir], zero: Info) -> Info {
    match subs {
        [] => zero,
        [only] => analyze(only),
        [first, second, rest @ ..] => {
            let mut info = f(analyze(first), analyze(second));
            for sub in rest {
                info = f(info, analyze(sub));
            }
            info
        }
    }
}

/// The info for xy, given x's and y's.
fn concat(x: Info, y: Info) -> Info {
    let mut xy = Info {
        can_empty: false,
        exact: None,
        prefix: Vec::new(),
        suffix: Vec::new(),
        matching: x.matching.clone().and(y.matching.clone()),
    };
    match (&x.exact, &y.exact) {
        (Some(xe), Some(ye)) => xy.exact = Some(cross(xe, ye, false)),
        _ => {
            if let Some(xe) = &x.exact {
                xy.prefix = cross(xe, &y.prefix, false);
            } else {
                xy.prefix.clone_from(&x.prefix);
                if x.can_empty {
                    xy.prefix = union_strings(xy.prefix, &y.prefix, false);
                }
            }
            if let Some(ye) = &y.exact {
                xy.suffix = cross(&x.suffix, ye, true);
            } else {
                xy.suffix.clone_from(&y.suffix);
                if y.can_empty {
                    xy.suffix = union_strings(xy.suffix, &x.suffix, true);
                }
            }
        }
    }
    // When every string of x's suffixes followed by y's prefixes is long enough, one of their
    // trigrams must be there, which the prefix and suffix sets may not yet say.
    if x.exact.is_none()
        && y.exact.is_none()
        && x.suffix.len() <= MAX_SET
        && y.prefix.len() <= MAX_SET
        && min_len(&x.suffix) + min_len(&y.prefix) >= 3
    {
        let matching = std::mem::replace(&mut xy.matching, Query::all());
        xy.matching = matching.and_trigrams(&cross(&x.suffix, &y.prefix, false));
    }
    xy.simplify(false);
    xy
}

/// The info for x|y, given x's and y's.
fn alternate(mut x: Info, mut y: Info) -> Info {
    let mut xy = Info {
        can_empty: x.can_empty || y.can_empty,
        exact: None,
        prefix: Vec::new(),
        suffix: Vec::new(),
        matching: Query::all(),
    };
    match (&x.exact, &y.exact) {
        (Some(xe), Some(ye)) => xy.exact = Some(union_strings(xe.clone(), ye, false)),
        (Some(xe), None) => {
            xy.prefix = union_strings(xe.clone(), &y.prefix, false);
            xy.suffix = union_strings(xe.clone(), &y.suffix, true);
            x.add_exact();
        }
        (None, Some(ye)) => {
            xy.prefix = union_strings(x.prefix.clone(), ye, false);
            xy.suffix = union_strings(x.suffix.clone(), ye, true);
            y.add_exact();
        }
        (None, None) => {
            xy.prefix = union_strings(x.prefix.clone(), &y.prefix, false);
            xy.suffix = union_strings(x.suffix.clone(), &y.suffix, true);
        }
    }
    xy.matching = x.matching.or(y.matching);
    xy.simplify(false);
    xy
}

/// Sorts a set, by its strings or by their reversed bytes for a suffix set, and drops repeats.
fn clean(set: &mut Vec<Vec<u8>>, is_suffix: bool) {
    if is_suffix {
        set.sort_unstable_by(|a, b| a.iter().rev().cmp(b.iter().rev()));
    } else {
        set.sort_unstable();
    }
    set.dedup();
}

fn union_strings(mut s: Vec<Vec<u8>>, t: &[Vec<u8>], is_suffix: bool) -> Vec<Vec<u8>> {
    s.extend_from_slice(t);
    clean(&mut s, is_suffix);
    s
}

fn cross(s: &[Vec<u8>], t: &[Vec<u8>], is_suffix: bool) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * t.len());
    for a in s {
        for b in t {
            let mut ab = a.clone();
            ab.extend_from_slice(b);
            out.push(ab);
        }
    }
    clean(&mut out, is_suffix);
    out
}

fn min_len(set: &[Vec<u8>]) -> usize {
    set.iter().map(Vec::len).min().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(pattern: &str) -> Query {
        query_of(&regex_syntax::Parser::new().parse(pattern).unwrap())
    }

    /// Codesearch's own table (`index/regexp_test.go`): each expression's query, in its notation.
    #[test]
    fn each_expression_gives_codesearch_s_query() {
        let table = [
            (r"Abcdef", r#""Abc" "bcd" "cde" "def""#),
            (r"(abc)(def)", r#""abc" "bcd" "cde" "def""#),
            (r"abc.*(def|ghi)", r#""abc" ("def"|"ghi")"#),
            (
                r"abc(def|ghi)",
                r#""abc" ("bcd" "cde" "def")|("bcg" "cgh" "ghi")"#,
            ),
            (r"a+hello", r#""ahe" "ell" "hel" "llo""#),
            (
                r"(a+hello|b+world)",
                r#"("ahe" "ell" "hel" "llo")|("bwo" "orl" "rld" "wor")"#,
            ),
            (r"a*bbb", r#""bbb""#),
            (r"a?bbb", r#""bbb""#),
            (r"(bbb)a?", r#""bbb""#),
            (r"(bbb)a*", r#""bbb""#),
            (r"^abc", r#""abc""#),
            (r"abc$", r#""abc""#),
            (r"ab[cde]f", r#"("abc" "bcf")|("abd" "bdf")|("abe" "bef")"#),
            (r"(abc|bac)de", r#""cde" ("abc" "bcd")|("acd" "bac")"#),
            (r"ab[^cde]f", r"+"),
            (r"ab.f", r"+"),
            (r".", r"+"),
            (r"()", r"+"),
            (r"[^\s\S]", r"-"),
            (r"(abc|abc)", r#""abc""#),
            (r"(ab|ab)c", r#""abc""#),
            (r"ab(cab|cat)", r#""abc" "bca" ("cab"|"cat")"#),
            (r"(z*(abc|def)z*)(z*(abc|def)z*)", r#"("abc"|"def")"#),
            (r"(z*abcz*defz*)|(z*abcz*defz*)", r#""abc" "def""#),
            (
                r"(z*abcz*defz*(ghi|jkl)z*)|(z*abcz*defz*(mno|prs)z*)",
                r#""abc" "def" ("ghi"|"jkl"|"mno"|"prs")"#,
            ),
            (
                r"(z*(abcz*def)|(ghiz*jkl)z*)|(z*(mnoz*prs)|(tuvz*wxy)z*)",
                r#"("abc" "def")|("ghi" "jkl")|("mno" "prs")|("tuv" "wxy")"#,
            ),
            (
                r"(z*abcz*defz*)(z*(ghi|jkl)z*)",
                r#""abc" "def" ("ghi"|"jkl")"#,
            ),
            (
                r"(z*abcz*defz*)|(z*(ghi|jkl)z*)",
                r#"("ghi"|"jkl")|("abc" "def")"#,
            ),
            (
                r"[ab][cd][ef]",
                r#"("ace"|"acf"|"ade"|"adf"|"bce"|"bcf"|"bde"|"bdf")"#,
            ),
            (r"ab[cd]e", r#"("abc" "bce")|("abd" "bde")"#),
            (r"(a|ab)cde", r#""cde" ("abc" "bcd")|("acd")"#),
            (r"(a|b|c|d)(ef|g|hi|j)", r"+"),
            (r"(?s).", r"+"),
            (r"(?i)a~~", r#"("A~~"|"a~~")"#),
            (r"(?i)ab~", r#"("AB~"|"Ab~"|"aB~"|"ab~")"#),
            (
                r"(?i)abc",
                r#"("ABC"|"ABc"|"AbC"|"Abc"|"aBC"|"aBc"|"abC"|"abc")"#,
            ),
            (
                r"(?i)abc|def",
                r#"("ABC"|"ABc"|"AbC"|"Abc"|"DEF"|"DEf"|"DeF"|"Def"|"aBC"|"aBc"|"abC"|"abc"|"dEF"|"dEf"|"deF"|"def")"#,
            ),
            (
                r"(?i)abcd",
                r#"("ABC"|"ABc"|"AbC"|"Abc"|"aBC"|"aBc"|"abC"|"abc") ("BCD"|"BCd"|"BcD"|"Bcd"|"bCD"|"bCd"|"bcD"|"bcd")"#,
            ),
            (
                r"(?i)abc|abc",
                r#"("ABC"|"ABc"|"AbC"|"Abc"|"aBC"|"aBc"|"abC"|"abc")"#,
            ),
            (r"\b", r"+"),
            (r"\B", r"+"),
            (r"\babc", r#""abc""#),
            (r"\Babc", r#""abc""#),
            (r"abc\b", r#""abc""#),
            (r"abc\B", r#""abc""#),
            (r"ab\bc", r#""abc""#),
            (r"ab\Bc", r#""abc""#),
            // A class of more than 100 characters is any character, as codesearch has it.
            (r"[\x{800}-\x{bff}]", r"+"),
        ];
        let mut wrong = Vec::new();
        for (pattern, expected) in table {
            let got = query(pattern).to_string();
            if got != expected {
                wrong.push(format!("{pattern}: {got}, not {expected}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// xorshift64*, for expressions and texts that are random and the same at each run.
    struct Rand(u64);

    impl Rand {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn expression(rand: &mut Rand, depth: u32) -> String {
        let atoms = [
            "a", "b", "c", "ab", "abc", "bca", "[ab]", "[^a]", ".", r"\b", "é", "(?i)a",
        ];
        if depth == 0 {
            return atoms[rand.below(atoms.len())].to_owned();
        }
        let sub = |rand: &mut Rand| expression(rand, depth - 1);
        match rand.below(9) {
            0..=2 => format!("{}{}", sub(rand), sub(rand)),
            3 => format!("({}|{})", sub(rand), sub(rand)),
            4 => format!("({})*", sub(rand)),
            5 => format!("({})+", sub(rand)),
            6 => format!("({})?", sub(rand)),
            7 => format!("({}){{2,3}}", sub(rand)),
            _ => sub(rand),
        }
    }

    /// Every text a random expression matches satisfies its query: the query may only let
    /// through too much. Texts are drawn from many letters and from three, which an expression
    /// matches far more often, and so with repetitions of more than one letter.
    #[test]
    fn a_query_admits_every_text_its_expression_matches() {
        let mut rand = Rand(0x5EED_C0DE);
        let many = ["a", "b", "c", "A", "é", " ", "\n"];
        let few = ["a", "b", "c"];
        let mut checked = 0;
        for _ in 0..3000 {
            let depth = 1 + rand.below(4) as u32;
            let pattern = expression(&mut rand, depth);
            let hir = regex_syntax::Parser::new().parse(&pattern).unwrap();
            let regex = regex::Regex::new(&pattern).unwrap();
            let query = query_of(&hir);
            for n in 0..60 {
                let letters: &[&str] = if n % 2 == 0 { &many } else { &few };
                let text: String = (0..rand.below(14))
                    .map(|_| letters[rand.below(letters.len())])
                    .collect();
                if !regex.is_match(&text) {
                    continue;
                }
                checked += 1;
                let bytes = text.as_bytes();
                let has = |t: &Trigram| bytes.windows(3).any(|w| w == t);
                assert!(
                    query.admits(&has),
                    "{pattern} matches {text:?}, but its query {query} does not"
                );
            }
        }
        assert!(checked > 20_000, "only {checked} matches checked");
    }

    /// A repetition is no exact string: `c[ab]+c` matches `cabc`, which holds neither `cac` nor
    /// `cbc`.
    #[test]
    fn a_repetition_of_a_class_keeps_only_its_ends() {
        let query = query("c[ab]+c");
        let text = b"cabc";
        let has = |t: &Trigram| text.windows(3).any(|w| w == t);
        assert!(query.admits(&has), "{query}");
    }
}
