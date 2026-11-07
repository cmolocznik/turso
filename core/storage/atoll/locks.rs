//! Atoll's locks (DESIGN.md 22.11): every lock that guards Atoll's shared state is one of these,
//! over the engine's mutex and read-write lock, and each guard, before it lets the lock go, ends
//! the process if its thread is unwinding ([`abort_if_unwinding`]), so no other thread takes the
//! lock over what a panic left half made.
//!
//! No other lock type is named in Atoll: `clippy::disallowed_types` (`core/clippy.toml`), denied
//! in Atoll's module and allowed here alone, resolves every path and alias to the type it names,
//! and a test scans Atoll's source for the spellings, so a stray lock fails before a push.
#![allow(clippy::disallowed_types)]

use super::unwind::abort_if_unwinding;
use std::fmt;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

pub struct Mutex<T: ?Sized>(crate::sync::Mutex<T>);

pub struct MutexGuard<'a, T: ?Sized>(ManuallyDrop<crate::sync::MutexGuard<'a, T>>);

pub struct RwLock<T: ?Sized>(crate::sync::RwLock<T>);

pub struct RwLockReadGuard<'a, T: ?Sized>(ManuallyDrop<crate::sync::RwLockReadGuard<'a, T>>);

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

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        Self(crate::sync::RwLock::new(value))
    }
}

impl<T: ?Sized> RwLock<T> {
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        RwLockReadGuard(ManuallyDrop::new(self.0.read()))
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        RwLockWriteGuard(ManuallyDrop::new(self.0.write()))
    }
}

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
checked_guard!(RwLockReadGuard,);
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

    /// Aliases of a lock defined in the crate outside Atoll, as `module::Name`: a `type` whose
    /// right side names one, or a lock brought in under another name.
    fn lock_aliases() -> Vec<String> {
        let core = Path::new(env!("CARGO_MANIFEST_DIR"));
        let atoll = atoll_dir();
        let mut files = Vec::new();
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
        walk(core, &mut files);
        let mut out = Vec::new();
        for path in files.iter().filter(|p| !p.starts_with(&atoll)) {
            let rel = path.strip_prefix(core).unwrap().with_extension("");
            let mut module: Vec<String> = rel.iter().map(|c| c.to_string_lossy().into()).collect();
            if module.last().is_some_and(|m| m == "mod" || m == "lib") {
                module.pop();
            }
            let module = std::iter::once("crate".to_string())
                .chain(module)
                .collect::<Vec<_>>()
                .join("::");
            let code = code_of(&std::fs::read_to_string(path).unwrap());
            for line in code.split(';') {
                let words: Vec<&str> = line.split_whitespace().collect();
                if let Some(at) = words.iter().position(|w| *w == "type") {
                    if line.contains('=') && names_a_lock(line.split('=').nth(1).unwrap()) {
                        let name: String = words
                            .get(at + 1)
                            .unwrap_or(&"")
                            .chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect();
                        if !name.is_empty() {
                            out.push(format!("{module}::{name}"));
                        }
                    }
                }
                for pair in words.windows(3) {
                    if pair[1] == "as" && names_a_lock(pair[0]) {
                        let name: String = pair[2]
                            .chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect();
                        out.push(format!("{module}::{name}"));
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        out
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
    #[cfg(all(unix, not(miri)))]
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

    /// No lock but Atoll's own is named in Atoll's source, under its own name or an alias the
    /// rest of the crate defines; loom's are, in the models compiled only for `atoll_loom`,
    /// which check orderings and never run in a build Turso ships.
    #[test]
    fn atoll_names_no_lock_but_its_own() {
        let me = atoll_dir().join("locks.rs");
        let files = atoll_sources();
        assert!(files.len() > 100, "the scan found {} files", files.len());
        let aliases = lock_aliases();
        for known in [
            "crate::index_method::ivf_rq::Shared",
            "crate::io::windows_lock::ProcessFileLockRegistry",
        ] {
            assert!(
                aliases.iter().any(|a| a == known),
                "{known} not found in {aliases:?}"
            );
        }
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
