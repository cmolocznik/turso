//! Reading the crate's own source, for the censuses that hold a rule over every site of a kind.

/// A file's code without its comments and the contents of its string literals, raw ones included,
/// which may name what a census looks for (a test's stand-in crate does) without using it. Line
/// breaks stay, so a finding's line is the source's.
pub(crate) fn code_of(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    // The two characters before the current one, for telling a raw string's `r` from a name's.
    let (mut before, mut last) = (' ', ' ');
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    while let Some(c) = chars.next() {
        match c {
            '/' if chars.peek() == Some(&'/') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = ' ';
                for c in chars.by_ref() {
                    if c == '\n' {
                        out.push('\n');
                    }
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
            }
            // A raw string, `r"..."` or `r#"..."#`, a byte one's `br` too: its contents go.
            'r' if (!ident(last) || (last == 'b' && !ident(before)))
                && matches!(chars.peek(), Some('"' | '#')) =>
            {
                let mut ahead = chars.clone();
                let mut hashes = 0;
                while ahead.peek() == Some(&'#') {
                    ahead.next();
                    hashes += 1;
                }
                if ahead.peek() != Some(&'"') {
                    out.push(c);
                } else {
                    for _ in 0..=hashes {
                        chars.next();
                    }
                    out.push_str("r\"");
                    let mut quote_then = None;
                    for c in chars.by_ref() {
                        if c == '\n' {
                            out.push('\n');
                        }
                        quote_then = match (quote_then, c) {
                            (_, '"') if hashes == 0 => break,
                            (_, '"') => Some(0),
                            (Some(n), '#') if n + 1 == hashes => break,
                            (Some(n), '#') => Some(n + 1),
                            _ => None,
                        };
                    }
                    out.push('"');
                }
            }
            '"' => {
                out.push('"');
                while let Some(c) = chars.next() {
                    match c {
                        '\\' => {
                            chars.next();
                        }
                        '"' => break,
                        '\n' => out.push('\n'),
                        _ => {}
                    }
                }
                out.push('"');
            }
            // A char literal holding a quote, or a lifetime: neither opens a string.
            '\'' => {
                out.push('\'');
                let mut ahead = chars.clone();
                match (ahead.next(), ahead.next(), ahead.next()) {
                    (Some('\\'), Some(_), Some('\'')) | (Some(_), Some('\''), _) => {
                        let n = if chars.peek() == Some(&'\\') { 3 } else { 2 };
                        for _ in 0..n {
                            chars.next();
                        }
                        out.push('\'');
                    }
                    _ => {}
                }
            }
            _ => out.push(c),
        }
        (before, last) = (last, c);
    }
    out
}

/// `code` without the modules compiled for tests alone (an inline `mod name { ... }` under a `cfg`
/// naming `test`, outside a `not`), whose sites are no production code's.
pub(crate) fn without_test_modules(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    let mut rest = code;
    while let Some(at) = rest.find("#[cfg(") {
        let Some(attr_end) = rest[at..].find(")]").map(|e| at + e + 2) else {
            break;
        };
        let attr = &rest[at..attr_end];
        let names_test = attr
            .split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .any(|w| w == "test")
            && !attr.contains("not(test");
        let after = rest[attr_end..].trim_start();
        let after = ["pub(crate) ", "pub(super) ", "pub "]
            .iter()
            .find_map(|v| after.strip_prefix(v))
            .unwrap_or(after);
        let open = after.find(['{', ';']);
        let inline_module =
            after.starts_with("mod ") && open.is_some_and(|o| after.as_bytes()[o] == b'{');
        if !(names_test && inline_module) {
            out.push_str(&rest[..attr_end]);
            rest = &rest[attr_end..];
            continue;
        }
        out.push_str(&rest[..at]);
        let body = &rest[attr_end..];
        let open = body.find('{').expect("an inline module opens");
        let mut depth = 0usize;
        let mut end = body.len();
        for (i, c) in body[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + i + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        // Line breaks stay, so a finding's line is the source's.
        out.extend(body[..end].chars().filter(|&c| c == '\n'));
        rest = &body[end..];
    }
    out.push_str(rest);
    out
}

/// The methods the trait or impl block `header` opens in `code` declares at its own level, each
/// with its body, braces included, or an empty one for a declaration with none: a method's own
/// braces, and a default body's, are deeper and never read as the block's.
pub(crate) fn methods_of<'a>(code: &'a str, header: &str) -> Vec<(String, &'a str)> {
    let start = code
        .find(header)
        .unwrap_or_else(|| panic!("no {header:?} in the code read"));
    let open = start + code[start..].find('{').unwrap();
    let end = open + closing(&code[open..]);
    let block = &code[open + 1..end - 1];
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    let (mut methods, mut at) = (Vec::new(), 0);
    while at < block.len() {
        let rest = &block[at..];
        let Some(c) = rest.chars().next() else { break };
        match c {
            // A nested item's braces at the block's level (an associated const's, say): passed over.
            '{' => {
                at += closing(rest);
                continue;
            }
            'f' if rest.starts_with("fn ")
                && !block[..at].chars().next_back().is_some_and(ident) =>
            {
                let name: String = rest[3..].chars().take_while(|&c| ident(c)).collect();
                // The body opens at the first brace outside the signature's parentheses and
                // brackets; a semicolon there first is a declaration with none.
                let (mut depth, mut body) = (0i32, "");
                let mut past = rest.len();
                for (i, c) in rest.char_indices() {
                    match c {
                        '(' | '[' => depth += 1,
                        ')' | ']' => depth -= 1,
                        ';' if depth == 0 => {
                            past = i + 1;
                            break;
                        }
                        '{' if depth == 0 => {
                            let length = closing(&rest[i..]);
                            body = &rest[i..i + length];
                            past = i + length;
                            break;
                        }
                        _ => {}
                    }
                }
                methods.push((name, body));
                at += past;
                continue;
            }
            _ => {}
        }
        at += c.len_utf8();
    }
    methods
}

/// The length of the braced block `code` opens with, its closing brace included.
fn closing(code: &str) -> usize {
    let mut depth = 0usize;
    for (i, c) in code.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return i + 1;
                }
            }
            _ => {}
        }
    }
    panic!("a block that never closes")
}

/// Every Rust source file under `dir`, sorted.
pub(crate) fn rust_sources(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, &mut out);
    out.sort();
    out
}
