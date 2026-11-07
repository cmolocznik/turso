//! Atoll's locks (DESIGN.md 22.11): every lock that guards Atoll's shared state is one of these,
//! over the engine's mutex and read-write lock, and each guard, before it lets the lock go, ends
//! the process if its thread is unwinding ([`abort_if_unwinding`]), so no other thread takes the
//! lock over what a panic left half made.
//!
//! No other lock type is named in Atoll. `clippy::disallowed_types` (`core/clippy.toml`), denied
//! in Atoll's module and allowed here alone, resolves each path to the item it names: a name
//! brought in to the lock it names, and a type alias to the alias, which is listed there too, as
//! is every alias of an alias (a test below finds them all). A second test scans Atoll's source
//! for the spellings, so a stray lock fails before a push.
#![allow(clippy::disallowed_types)]

use super::unwind::abort_if_unwinding;
use std::fmt;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

pub struct Mutex<T: ?Sized>(crate::sync::Mutex<T>);

pub struct MutexGuard<'a, T: ?Sized>(ManuallyDrop<crate::sync::MutexGuard<'a, T>>);

// For tests alone while its one taker, `stream`, is (mod.rs).
#[cfg(test)]
pub struct RwLock<T: ?Sized>(crate::sync::RwLock<T>);

#[cfg(test)]
pub struct RwLockReadGuard<'a, T: ?Sized>(ManuallyDrop<crate::sync::RwLockReadGuard<'a, T>>);

#[cfg(test)]
pub struct RwLockWriteGuard<'a, T: ?Sized>(ManuallyDrop<crate::sync::RwLockWriteGuard<'a, T>>);

impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Self(crate::sync::Mutex::new(value))
    }
}

impl<T: ?Sized> Mutex<T> {
    pub fn lock(&self) -> MutexGuard<'_, T> {
        MutexGuard(ManuallyDrop::new(self.0.lock()))
    }

    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        self.0.try_lock().map(|g| MutexGuard(ManuallyDrop::new(g)))
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// Formats the value under the checked guard, so a panic in the value's formatting ends the
/// process before the lock is let go, as any other panic under the guard does. A lock held
/// elsewhere formats as held.
impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = f.debug_struct("Mutex");
        match self.try_lock() {
            Some(value) => out.field("data", &&*value),
            None => out.field("data", &format_args!("<held>")),
        };
        out.finish()
    }
}

#[cfg(test)]
impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        Self(crate::sync::RwLock::new(value))
    }
}

#[cfg(test)]
impl<T: ?Sized> RwLock<T> {
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        RwLockReadGuard(ManuallyDrop::new(self.0.read()))
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        RwLockWriteGuard(ManuallyDrop::new(self.0.write()))
    }
}

#[cfg(test)]
impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

macro_rules! checked_guard {
    ($guard:ident, $($mut:ident)?) => {
        impl<T: ?Sized> Deref for $guard<'_, T> {
            type Target = T;

            fn deref(&self) -> &T {
                &self.0
            }
        }

        $(
            impl<T: ?Sized> DerefMut for $guard<'_, T> {
                fn $mut(&mut self) -> &mut T {
                    &mut self.0
                }
            }
        )?

        impl<T: ?Sized> Drop for $guard<'_, T> {
            fn drop(&mut self) {
                abort_if_unwinding();
                // SAFETY: the inner guard is dropped here once, and nothing reads it after.
                unsafe { ManuallyDrop::drop(&mut self.0) }
            }
        }
    };
}

checked_guard!(MutexGuard, deref_mut);
#[cfg(test)]
checked_guard!(RwLockReadGuard,);
#[cfg(test)]
checked_guard!(RwLockWriteGuard, deref_mut);

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod tests {
    use crate::storage::atoll::testing::{atoll_dir, atoll_sources, code_of};
    use std::path::Path;

    /// The lock types Atoll may not name outside this module, as spelled through each crate
    /// that provides one; the engine's `crate::sync` re-exports parking_lot's and shuttle's.
    const FORBIDDEN: &[&str] = &[
        "parking_lot",
        "lock_api",
        "std::sync::Mutex",
        "std::sync::RwLock",
        "std::sync::Condvar",
        "shuttle::sync::Mutex",
        "shuttle::sync::RwLock",
        "shuttle::sync::Condvar",
        "crate::sync::Mutex",
        "crate::sync::RwLock",
        "crate::sync::MutexGuard",
        "crate::sync::ArcMutexGuard",
        "crate::sync::*",
    ];

    /// Names `crate::sync` lends Atoll: everything else it exports is a lock or a guard.
    const LENT: &[&str] = &["atomic", "Arc", "Weak", "OnceLock", "LazyLock"];

    /// The names a lock or its guard goes by, here and in every crate that provides one.
    const LOCK_NAMES: &[&str] = &[
        "Mutex",
        "RwLock",
        "Condvar",
        "MutexGuard",
        "RwLockReadGuard",
        "RwLockWriteGuard",
        "ArcMutexGuard",
        "ReentrantMutex",
    ];

    fn names_a_lock(text: &str) -> bool {
        text.split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .any(|word| LOCK_NAMES.contains(&word))
    }

    /// The top-level items of the use list that opens at `list[0] == '{'`, and where it closes.
    fn use_list(list: &str) -> (Vec<&str>, usize) {
        let (mut depth, mut start, mut items) = (0usize, 1, Vec::new());
        for (i, c) in list.char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        items.push(list[start..i].trim());
                        return (items.into_iter().filter(|s| !s.is_empty()).collect(), i);
                    }
                }
                ',' if depth == 1 => {
                    items.push(list[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            }
        }
        panic!("a use list never closed");
    }

    /// Every `use` statement of `code`, whole.
    fn uses(code: &str) -> Vec<&str> {
        let mut out = Vec::new();
        let mut rest = code;
        while let Some(at) = rest.find("use ") {
            let before = rest[..at].chars().last();
            let after = &rest[at..];
            let end = after.find(';').unwrap_or(after.len());
            if before.is_none_or(|c| !(c.is_alphanumeric() || c == '_')) {
                out.push(&after[..end]);
            }
            rest = &after[end.min(after.len())..];
            if rest.is_empty() {
                break;
            }
            rest = &rest[1..];
        }
        out
    }

    /// A name for a lock defined in the crate outside Atoll, at the path that defines it: a
    /// `type` whose right side names a lock or another such name, or a lock or such a name
    /// brought in under a name of its own (`use ... as`).
    #[derive(Debug)]
    struct Alias {
        path: String,
        /// A `type`: an item of its own, which the lint knows only by this path. A name brought
        /// in is resolved by the lint to what it names.
        defines: bool,
        /// Atoll can name it: it is not private, or it is private to a module Atoll is inside
        /// (the crate's root, `storage`), whose private items Atoll sees.
        nameable: bool,
    }

    /// An item's visibility, `""` for a private one, and the item after it.
    fn visibility(item: &str) -> (&str, &str) {
        if item.starts_with("pub(") {
            let close = item.find(')').unwrap() + 1;
            (&item[..close], item[close..].trim_start())
        } else if let Some(rest) = item.strip_prefix("pub ") {
            ("pub", rest.trim_start())
        } else {
            ("", item)
        }
    }

    /// `text` past the attributes that open it.
    fn past_attributes(text: &str) -> &str {
        let mut text = text.trim_start();
        while text.starts_with('#') {
            let mut depth = 0;
            let Some(end) = text
                .char_indices()
                .find(|&(_, c)| {
                    match c {
                        '[' => depth += 1,
                        ']' => depth -= 1,
                        _ => {}
                    }
                    c == ']' && depth == 0
                })
                .map(|(i, _)| i + 1)
            else {
                break;
            };
            text = text[end..].trim_start();
        }
        text
    }

    /// The statements of `code` at module level, each with the inline modules (`mod name { }`)
    /// it is inside, past its attributes. A statement in a function, an impl, a trait or a
    /// type's body is not the module's, and a use list's braces stay inside its statement.
    fn module_statements(code: &str) -> Vec<(Vec<String>, String)> {
        let (mut out, mut frames, mut start) = (Vec::new(), Vec::<Option<String>>::new(), 0);
        let mut chars = code.char_indices().peekable();
        while let Some((at, c)) = chars.next() {
            match c {
                '{' => {
                    let item = visibility(past_attributes(&code[start..at])).1;
                    if item.starts_with("use ") {
                        let mut depth = 1;
                        for (_, c) in chars.by_ref() {
                            match c {
                                '{' => depth += 1,
                                '}' => depth -= 1,
                                _ => {}
                            }
                            if depth == 0 {
                                break;
                            }
                        }
                        continue;
                    }
                    frames.push(item.strip_prefix("mod ").map(|m| m.trim().to_string()));
                    start = at + 1;
                }
                '}' => {
                    frames.pop();
                    start = at + 1;
                }
                ';' => {
                    if frames.iter().all(Option::is_some) {
                        let inline = frames.iter().flatten().cloned().collect();
                        out.push((inline, past_attributes(&code[start..at]).to_string()));
                    }
                    start = at + 1;
                }
                _ => {}
            }
        }
        out
    }

    fn is_word(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }

    /// The lock aliases among `files`, each a module's path and code: a name is one if it names
    /// a lock or a name found before it, followed to a fixpoint, so an alias of an alias, at any
    /// depth, is one too.
    fn lock_aliases_in(files: &[(String, String)]) -> Vec<Alias> {
        // (path, name, private, module, the words its right side names)
        let mut types = Vec::new();
        // (the name brought in, the module it is brought into, its new name)
        let mut renames = Vec::new();
        for (file_module, code) in files {
            for (inline, statement) in module_statements(code) {
                let module = std::iter::once(file_module.clone())
                    .chain(inline)
                    .collect::<Vec<_>>()
                    .join("::");
                let (vis, item) = visibility(&statement);
                if let Some((head, right)) =
                    item.strip_prefix("type ").and_then(|t| t.split_once('='))
                {
                    let name: String = head.trim().chars().take_while(|c| is_word(*c)).collect();
                    // Its own generic parameters name nothing outside it.
                    let params: Vec<String> = head
                        .split_once('<')
                        .map(|(_, ps)| {
                            ps.split(',')
                                .map(|p| {
                                    p.trim_start_matches(|c: char| !is_word(c))
                                        .chars()
                                        .take_while(|c| is_word(*c))
                                        .collect()
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    let words: Vec<String> = right
                        .split(|c: char| !is_word(c))
                        .filter(|w| !w.is_empty() && !params.iter().any(|p| p == w))
                        .map(String::from)
                        .collect();
                    types.push((
                        format!("{module}::{name}"),
                        name,
                        vis.is_empty(),
                        module,
                        words,
                    ));
                } else if item.starts_with("use ") {
                    let words: Vec<&str> = item
                        .split(|c: char| c.is_whitespace() || matches!(c, '{' | '}' | ','))
                        .filter(|w| !w.is_empty())
                        .collect();
                    for w in words.windows(3).filter(|w| w[1] == "as") {
                        let from = w[0].rsplit("::").next().unwrap().to_string();
                        renames.push((from, module.clone(), w[2].to_string()));
                    }
                }
            }
        }
        let mut known: std::collections::HashSet<String> =
            LOCK_NAMES.iter().map(|n| n.to_string()).collect();
        let mut found: Vec<Alias> = Vec::new();
        loop {
            let before = found.len();
            for (path, name, private, module, words) in &types {
                if !found.iter().any(|a| &a.path == path) && words.iter().any(|w| known.contains(w))
                {
                    known.insert(name.clone());
                    let inside = module == "crate" || module == "crate::storage";
                    found.push(Alias {
                        path: path.clone(),
                        defines: true,
                        nameable: !private || inside,
                    });
                }
            }
            for (from, module, to) in &renames {
                let path = format!("{module}::{to}");
                if known.contains(from) && !found.iter().any(|a| a.path == path) {
                    known.insert(to.clone());
                    found.push(Alias {
                        path,
                        defines: false,
                        nameable: true,
                    });
                }
            }
            if found.len() == before {
                break;
            }
        }
        found.sort_by(|a, b| a.path.cmp(&b.path));
        found
    }

    /// The lock aliases of the crate outside Atoll.
    fn lock_aliases() -> Vec<Alias> {
        let core = Path::new(env!("CARGO_MANIFEST_DIR"));
        let atoll = atoll_dir();
        let mut paths = Vec::new();
        fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    out.push(path);
                }
            }
        }
        walk(core, &mut paths);
        let files: Vec<(String, String)> = paths
            .iter()
            .filter(|p| !p.starts_with(&atoll))
            .map(|path| {
                let rel = path.strip_prefix(core).unwrap().with_extension("");
                let mut module: Vec<String> =
                    rel.iter().map(|c| c.to_string_lossy().into()).collect();
                if module.last().is_some_and(|m| m == "mod" || m == "lib") {
                    module.pop();
                }
                let module = std::iter::once("crate".to_string())
                    .chain(module)
                    .collect::<Vec<_>>()
                    .join("::");
                (module, code_of(&std::fs::read_to_string(path).unwrap()))
            })
            .collect();
        lock_aliases_in(&files)
    }

    fn findings(file: &str, text: &str, aliases: &[String]) -> Vec<String> {
        let code = code_of(text);
        let mut found: Vec<String> = FORBIDDEN
            .iter()
            .filter(|f| code.contains(*f))
            .map(|f| format!("{file}: names {f}"))
            .collect();
        // Lists brought in from `crate::sync`, possibly across lines and nested.
        let mut rest = code.as_str();
        while let Some(at) = rest.find("crate::sync::{") {
            let list = &rest[at + "crate::sync::".len()..];
            let (items, end) = use_list(list);
            for name in items {
                let head = name.split("::").next().unwrap().split(' ').next().unwrap();
                if !LENT.contains(&head) {
                    found.push(format!("{file}: brings {name} in from crate::sync"));
                }
            }
            rest = &list[end..];
        }
        // A lock's name comes in from Atoll's own locks, or from a loom model's.
        for u in uses(&code) {
            let own = u.contains("locks::") || u.contains("loom::") || u.contains("self::sync::");
            if names_a_lock(u) && !own {
                found.push(format!(
                    "{file}: `{}` brings in a lock not Atoll's own",
                    u.trim()
                ));
            }
        }
        // An alias of a lock defined elsewhere in the crate, named whole or from a use list.
        for alias in aliases {
            let (module, name) = alias.rsplit_once("::").unwrap();
            let listed = code.match_indices(&format!("{module}::{{")).any(|(at, _)| {
                use_list(&code[at + module.len() + 2..])
                    .0
                    .iter()
                    .any(|item| item.split(' ').next() == Some(name))
            });
            if code.contains(alias.as_str()) || listed {
                found.push(format!("{file}: names {alias}, an alias of a lock"));
            }
        }
        found
    }

    /// A panic while one of Atoll's locks is held ends the process before the lock is let go
    /// (DESIGN.md 22.11, proof 9). In a child process the holder writes a half-made value, an
    /// observer thread then tries the lock, and the holder panics: the child must end by abort,
    /// and the observer, recording what it sees to a synced file, must never have taken the
    /// lock. A guard that let it go (a raw parking_lot one) would hand the observer the half-made
    /// value, and the child would exit.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn a_panic_while_holding_an_atoll_lock_ends_the_process_before_it_is_let_go() {
        use crate::storage::atoll::testing::{in_child, record, recorded};
        use crate::sync::Arc;
        let ended = in_child(
            concat!(
                module_path!(),
                "::a_panic_while_holding_an_atoll_lock_ends_the_process_before_it_is_let_go"
            ),
            || {
                let lock = Arc::new(super::Mutex::new(0u32));
                let (held, watched) = (std::sync::mpsc::channel(), std::sync::mpsc::channel());
                let holder = {
                    let lock = lock.clone();
                    let (held, watched) = (held.0, watched.1);
                    std::thread::spawn(move || {
                        let mut state = lock.lock();
                        *state = 1;
                        held.send(()).unwrap();
                        watched.recv().unwrap();
                        panic!("a holder failed");
                    })
                };
                held.1.recv().unwrap();
                let observer = std::thread::spawn(move || {
                    record("seen", "watching");
                    watched.0.send(()).unwrap();
                    loop {
                        if let Some(state) = lock.try_lock() {
                            record("seen", &format!("taken {}", *state));
                            return;
                        }
                        std::thread::yield_now();
                    }
                });
                let _ = holder.join();
                observer.join().unwrap();
            },
        );
        let Some(ended) = ended else { return };
        let seen = recorded(&ended, "seen");
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(ended.stderr.contains("a holder failed"), "{}", ended.stderr);
        assert!(
            seen.contains("watching"),
            "the observer never ran: {seen:?}"
        );
        assert!(
            !seen.contains("taken"),
            "the observer took the lock: {seen:?}"
        );
    }

    /// A value whose formatting changes the first of its two halves, then panics.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    struct HalfFormats(std::cell::Cell<u8>, std::cell::Cell<u8>);

    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    impl std::fmt::Debug for HalfFormats {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.set(1);
            panic!("a value's formatting failed");
        }
    }

    /// Formatting a lock's value takes the checked guard: a value whose formatting panics
    /// halfway through a change ends the process before another thread can take the lock. In a
    /// child process one thread formats the lock and panics; the other then takes the lock and
    /// records what it holds. A format through the raw lock would let it go half changed.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn a_panic_formatting_an_atoll_locks_value_ends_the_process_before_it_is_let_go() {
        use crate::storage::atoll::testing::{in_child, record, recorded};
        use crate::sync::Arc;
        use std::cell::Cell;
        let ended = in_child(
            concat!(
                module_path!(),
                "::a_panic_formatting_an_atoll_locks_value_ends_the_process_before_it_is_let_go"
            ),
            || {
                let lock = Arc::new(super::Mutex::new(HalfFormats(Cell::new(0), Cell::new(0))));
                let formatter = {
                    let lock = lock.clone();
                    std::thread::spawn(move || format!("{lock:?}"))
                };
                let _ = formatter.join();
                let guard = lock.lock();
                let value: &HalfFormats = &guard;
                record(
                    "seen",
                    &format!("taken {} {}", value.0.get(), value.1.get()),
                );
            },
        );
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("a value's formatting failed"),
            "{}",
            ended.stderr
        );
        let seen = recorded(&ended, "seen");
        assert!(
            seen.is_empty(),
            "the lock was taken after the panic: {seen:?}"
        );
    }

    /// The inner lock of each of Atoll's locks is reached only to take a guard that is wrapped
    /// at once in its checked guard: no method of a lock, a trait's included, takes or lets go
    /// of a raw guard of its own.
    #[test]
    fn a_lock_reaches_its_inner_lock_only_to_wrap_the_guard_it_takes() {
        let code = crate::storage::atoll::testing::code_of(include_str!("locks.rs"));
        let code = &code[..code.find("mod tests {").unwrap()];
        let (mut uses, mut wrong) = (0, Vec::new());
        let mut rest = code;
        while let Some(at) = rest.find("impl") {
            let after = &rest[at..];
            let word = !rest[..at]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric() || c == '_')
                && after[4..].starts_with(['<', ' ']);
            if !word {
                rest = &after[4..];
                continue;
            }
            let open = after.find('{').unwrap();
            let header = &after[..open];
            let ty = match header.rsplit_once(" for ") {
                Some((_, ty)) => ty.trim(),
                // An inherent impl: the type follows the generics.
                None => header[header.find('>').map_or(4, |i| i + 1)..].trim(),
            };
            let mut depth = 0;
            let close = after[open..]
                .char_indices()
                .find(|&(_, c)| {
                    match c {
                        '{' => depth += 1,
                        '}' => depth -= 1,
                        _ => {}
                    }
                    depth == 0
                })
                .map(|(i, _)| open + i)
                .unwrap();
            if ty.starts_with("Mutex<") || ty.starts_with("RwLock<") {
                for line in after[open..close].lines().filter(|l| l.contains("self.0")) {
                    uses += 1;
                    let wraps = [
                        "self.0.lock()",
                        "self.0.try_lock()",
                        "self.0.read()",
                        "self.0.write()",
                    ]
                    .iter()
                    .any(|take| line.contains(take))
                        && line.contains("ManuallyDrop::new(");
                    if !wraps || line.matches("self.0").count() != 1 {
                        wrong.push(format!("{ty}: `{}`", line.trim()));
                    }
                }
            }
            rest = &after[close..];
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
        assert!(uses >= 4, "the census found {uses} uses of an inner lock");
    }

    /// No lock but Atoll's own is named in Atoll's source, under its own name or an alias the
    /// rest of the crate defines; loom's are, in the models compiled only for `atoll_loom`,
    /// which check orderings and never run in a build Turso ships.
    #[test]
    fn atoll_names_no_lock_but_its_own() {
        let me = atoll_dir().join("locks.rs");
        let files = atoll_sources();
        assert!(files.len() > 100, "the scan found {} files", files.len());
        let found = lock_aliases();
        for known in [
            "crate::index_method::ivf_rq::Shared",
            "crate::io::windows_lock::ProcessFileLockRegistry",
        ] {
            assert!(
                found.iter().any(|a| a.path == known),
                "{known} not found in {found:?}"
            );
        }
        let aliases: Vec<String> = found.into_iter().map(|a| a.path).collect();
        let found: Vec<String> = files
            .iter()
            .filter(|p| **p != me)
            .flat_map(|p| {
                findings(
                    &p.display().to_string(),
                    &std::fs::read_to_string(p).unwrap(),
                    &aliases,
                )
            })
            .collect();
        assert!(
            found.is_empty(),
            "locks not Atoll's own:\n{}",
            found.join("\n")
        );
    }

    /// The lint resolves a path to the item it names: a name brought in to what it names, a
    /// lock itself, but a `type` alias to the alias, known by the path that defines it. So every
    /// lock alias Atoll could name is listed in `core/clippy.toml` by that path, aliases of
    /// aliases included, and the lint refuses it in Atoll however it is spelled or brought in.
    #[test]
    fn every_lock_alias_atoll_could_name_is_refused_by_the_lint() {
        let toml =
            std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("clippy.toml"))
                .unwrap();
        let listed: Vec<&str> = toml
            .split("path = \"")
            .skip(1)
            .map(|s| s.split('"').next().unwrap())
            .collect();
        let owed: Vec<String> = lock_aliases()
            .into_iter()
            .filter(|a| a.defines && a.nameable)
            .map(|a| a.path.replacen("crate::", "turso_core::", 1))
            .collect();
        assert!(
            owed.iter()
                .any(|p| p == "turso_core::sync::std_adapter::ArcMutexGuard"),
            "the census found {owed:?}"
        );
        let missing: Vec<&String> = owed
            .iter()
            .filter(|p| !listed.contains(&p.as_str()))
            .collect();
        assert!(
            missing.is_empty(),
            "lock aliases Atoll could name that core/clippy.toml does not list: {missing:?}"
        );
    }

    /// The alias census follows an alias of an alias, in an inline module or brought in under a
    /// new name, and tells what Atoll can name from what it cannot.
    #[test]
    fn the_alias_census_follows_aliases_of_aliases() {
        let files = [
            (
                "crate::outside",
                "pub type One<T> = parking_lot::Mutex<T>;
                 pub type Two<T> = One<T>;
                 pub(crate) mod deeper {
                     pub type Three<T> = super::Two<T>;
                 }
                 type Private = One<u8>;
                 use crate::outside::{Other, Two as Four};
                 pub type Five = Four<u8>;
                 pub type Plain<Mutex> = Vec<Mutex>;
                 impl Holder { type Item = One<u8>; }
                 fn f() { type Local = One<u8>; }",
            ),
            ("crate::storage", "type Near = crate::outside::Two<u8>;"),
        ]
        .map(|(m, c)| (m.to_string(), c.to_string()));
        let found = lock_aliases_in(&files);
        let owed: Vec<&str> = found
            .iter()
            .filter(|a| a.defines && a.nameable)
            .map(|a| a.path.as_str())
            .collect();
        assert_eq!(
            owed,
            [
                "crate::outside::Five",
                "crate::outside::One",
                "crate::outside::Two",
                "crate::outside::deeper::Three",
                "crate::storage::Near",
            ],
            "{found:?}"
        );
        assert!(
            found
                .iter()
                .any(|a| a.path == "crate::outside::Private" && !a.nameable),
            "{found:?}"
        );
        assert!(
            found
                .iter()
                .any(|a| a.path == "crate::outside::Four" && !a.defines),
            "{found:?}"
        );
    }

    /// The scan is not blind: each spelling a stray lock could take is found, one in a comment
    /// or a string is not, and Atoll's own locks and the names `crate::sync` lends pass.
    #[test]
    fn the_lock_scan_finds_every_spelling_and_nothing_else() {
        let aliases = vec!["crate::index_method::ivf_rq::Shared".to_string()];
        let stray = [
            "use parking_lot::Mutex;",
            "use std::sync::Mutex;",
            "struct A(std::sync::RwLock<u8>);",
            "use crate::sync::Mutex;",
            "use crate::sync::{Arc,\n    Mutex};",
            "use crate::sync::{Arc, RwLock as R};",
            "use crate::sync::*;",
            "use crate::sync::{atomic::{fence, AtomicU8}, ArcMutexGuard};",
            "use crate::io::Mutex;",
            "use crate::vdbe::{Program, RwLockReadGuard as G};",
            "use crate::index_method::ivf_rq::Shared;",
            "use crate::index_method::ivf_rq::{Other, Shared};",
            "fn f(s: crate::index_method::ivf_rq::Shared) {}",
        ];
        for s in stray {
            assert!(!findings("stray", s, &aliases).is_empty(), "missed: {s}");
        }
        let clean = [
            "use crate::sync::{Arc, Weak};",
            "use crate::sync::atomic::{AtomicU64, Ordering};",
            "use crate::sync::{atomic::{fence, AtomicBool}, Arc};",
            "use crate::storage::atoll::locks::{Mutex, RwLock};",
            "use super::locks::{Mutex, MutexGuard};",
            "use self::sync::{fence, Arc, Mutex};",
            "use crate::index_method::ivf_rq::Other;",
            "// parking_lot::Mutex is not used here",
            "let s = \"std::sync::Mutex\";",
            "fn f<'a>(x: &'a u8) -> char { '\"' }",
        ];
        for s in clean {
            assert!(
                findings("clean", s, &aliases).is_empty(),
                "flagged: {s}: {:?}",
                findings("clean", s, &aliases)
            );
        }
    }
}
