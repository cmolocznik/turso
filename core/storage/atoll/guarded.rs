//! State another holder may hold (DESIGN.md 22.2 item 3): [`Guarded`] owns the state and the
//! takers waiting for it, and [`Hold`] is its guard.
//!
//! The state is a private field here, and [`Guarded::try_hold`] and [`Guarded::hold_or_contend`]
//! are the only ways to it, so no other code can take it without the guard (a test compiles the
//! attempt and expects it refused). A taker that finds the state held may register as a contender
//! and try again; if it then holds the state it takes its registration back, so a holder is never
//! a contender of its own hold. Letting a hold go lets the state go first, then wakes every
//! contender and empties the list: a contender woken while the state was still held would find it
//! held, wait again, and nothing would wake it. Nobody but a contender is woken.

use crate::sync::{Mutex, MutexGuard};
use crate::thread::Thread;
use std::ops::{Deref, DerefMut};

#[cfg(test)]
std::thread_local! {
    /// Run by this thread's next [`Guarded::hold_or_contend`] between the try that found the state
    /// held and its registration: a test lets the state go there.
    static BEFORE_CONTENDING: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// The takers that may wait for one state at once: the database's checkpointing thread, and an
/// integrity check, which holds the write gate exclusive, so at most one runs at a time.
const CONTENDERS: usize = 2;

pub struct Guarded<T> {
    state: Mutex<T>,
    /// Threads parked until the state is let go, at most [`CONTENDERS`], each once.
    contenders: Mutex<[Option<Thread>; CONTENDERS]>,
}

/// The state, held: letting it go wakes whoever waits for it.
pub struct Hold<'a, T> {
    state: Option<MutexGuard<'a, T>>,
    guarded: &'a Guarded<T>,
}

impl<T> Guarded<T> {
    pub fn new(state: T) -> Self {
        Self {
            state: Mutex::new(state),
            contenders: Mutex::new([const { None }; CONTENDERS]),
        }
    }

    /// The state, if no one holds it.
    pub fn try_hold(&self) -> Option<Hold<'_, T>> {
        self.state.try_lock().map(|state| Hold {
            state: Some(state),
            guarded: self,
        })
    }

    /// The state, if no one holds it; else `me` is a contender, unparked once the state is let
    /// go, which parks and tries again. Registered first and then tried again, so a hold let go
    /// in between either finds `me` listed or is seen let go.
    pub fn hold_or_contend(&self, me: &Thread) -> Option<Hold<'_, T>> {
        if let Some(held) = self.try_hold() {
            return Some(held);
        }
        #[cfg(test)]
        if let Some(between) = BEFORE_CONTENDING.with(|b| b.borrow_mut().take()) {
            between();
        }
        {
            let mut listed = self.contenders.lock();
            if !listed.iter().flatten().any(|t| t.id() == me.id()) {
                let free = listed
                    .iter_mut()
                    .find(|t| t.is_none())
                    .expect("more contenders for one state than may wait for it");
                *free = Some(me.clone());
            }
        }
        let held = self.try_hold()?;
        // Held now: its own letting go must not wake it.
        for t in self.contenders.lock().iter_mut() {
            if t.as_ref().is_some_and(|t| t.id() == me.id()) {
                *t = None;
            }
        }
        Some(held)
    }

    /// Whether `who` waits for the state.
    #[cfg(test)]
    pub(crate) fn contends(&self, who: &Thread) -> bool {
        self.contenders
            .lock()
            .iter()
            .flatten()
            .any(|t| t.id() == who.id())
    }
}

impl<T> Deref for Hold<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.state.as_deref().expect("the state held")
    }
}

impl<T> DerefMut for Hold<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.state.as_deref_mut().expect("the state held")
    }
}

impl<T> Drop for Hold<'_, T> {
    fn drop(&mut self) {
        drop(self.state.take());
        // Unparking runs no code of anyone's and takes no lock, so it is done under the mutex.
        for t in self.guarded.contenders.lock().iter_mut() {
            if let Some(t) = t.take() {
                t.unpark();
            }
        }
    }
}

#[cfg(all(test, not(shuttle)))]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::Duration;

    /// The state is reachable only through the guard: a crate built from this module, with
    /// stand-ins for the crate's `sync` and `thread`, compiles when it takes the state through
    /// `try_hold` and is refused, with the private field named, when it reaches for the field.
    #[test]
    fn the_state_is_reachable_only_through_the_guard() {
        let module = concat!(env!("CARGO_MANIFEST_DIR"), "/storage/atoll/guarded.rs");
        let crate_root = |body: &str| {
            format!(
                "mod sync {{\n\
                     pub struct Mutex<T>(std::sync::Mutex<T>);\n\
                     pub type MutexGuard<'a, T> = std::sync::MutexGuard<'a, T>;\n\
                     impl<T> Mutex<T> {{\n\
                         pub fn new(t: T) -> Self {{ Self(std::sync::Mutex::new(t)) }}\n\
                         pub fn lock(&self) -> MutexGuard<'_, T> {{ self.0.lock().unwrap() }}\n\
                         pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {{ self.0.try_lock().ok() }}\n\
                     }}\n\
                 }}\n\
                 mod thread {{ pub use std::thread::*; }}\n\
                 #[path = {module:?}]\n\
                 mod guarded;\n\
                 pub fn take(g: &guarded::Guarded<u32>) -> u32 {{ {body} }}\n"
            )
        };
        let compile = |name: &str, body: &str| {
            let dir =
                std::env::temp_dir().join(format!("atoll-guarded-{}-{name}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let src = dir.join("lib.rs");
            std::fs::write(&src, crate_root(body)).unwrap();
            let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
            let out = std::process::Command::new(rustc)
                .args([
                    "--edition",
                    "2021",
                    "--crate-type",
                    "lib",
                    "--emit",
                    "metadata",
                ])
                .arg("--out-dir")
                .arg(&dir)
                .arg(&src)
                .output()
                .expect("rustc runs");
            let _ = std::fs::remove_dir_all(&dir);
            (
                out.status.success(),
                String::from_utf8_lossy(&out.stderr).into_owned(),
            )
        };
        let (ok, stderr) = compile("guard", "*g.try_hold().unwrap()");
        assert!(ok, "taking the state through its guard compiles:\n{stderr}");
        let (ok, stderr) = compile("field", "*g.state.lock()");
        assert!(!ok, "reaching the state's field compiled");
        assert!(
            stderr.contains("E0616") && stderr.contains("`state`"),
            "refused, but not for the private field:\n{stderr}"
        );
    }

    /// A contender parked on a held state is woken once it is let go, and finds it free: a hold let
    /// go with no wake leaves it parked.
    #[test]
    fn a_contender_is_woken_once_the_state_is_let_go_and_finds_it_free() {
        for _ in 0..20 {
            let guarded = Arc::new(Guarded::new(0u32));
            let held = guarded.try_hold().expect("free");
            let (parking, parked) = mpsc::channel();
            let (done, finished) = mpsc::channel();
            let g = guarded.clone();
            let contender = std::thread::spawn(move || {
                let me = std::thread::current();
                let mut hold = loop {
                    if let Some(hold) = g.hold_or_contend(&me) {
                        break hold;
                    }
                    let _ = parking.send(());
                    std::thread::park();
                };
                *hold += 1;
                drop(hold);
                done.send(()).unwrap();
            });
            parked.recv().unwrap();
            let thread = contender.thread().clone();
            assert!(guarded.contends(&thread));
            drop(held);
            finished
                .recv_timeout(Duration::from_secs(10))
                .expect("the contender was left parked after the state was let go");
            contender.join().unwrap();
            assert_eq!(*guarded.try_hold().expect("free"), 1);
            assert!(!guarded.contends(&thread));
        }
    }

    /// A taker whose first try finds the state held, which is let go before it registers, takes
    /// the state on its retry and takes its registration back: its own letting go wakes nobody.
    #[test]
    fn a_holder_is_not_woken_by_its_own_release() {
        let guarded = Arc::new(Guarded::new(0u32));
        let (held, holding) = mpsc::channel();
        let (go, told) = mpsc::channel::<()>();
        let (gone, let_go) = mpsc::channel();
        let g = guarded.clone();
        let other = std::thread::spawn(move || {
            let hold = g.try_hold().expect("free");
            held.send(()).unwrap();
            told.recv().unwrap();
            drop(hold);
            gone.send(()).unwrap();
        });
        holding.recv().unwrap();
        BEFORE_CONTENDING.with(|b| {
            *b.borrow_mut() = Some(Box::new(move || {
                go.send(()).unwrap();
                let_go.recv().unwrap();
            }))
        });
        let me = std::thread::current();
        let hold = guarded
            .hold_or_contend(&me)
            .expect("let go before the registration, so taken on the retry");
        assert!(
            !guarded.contends(&me),
            "the holder is still listed, so its own letting go wakes it"
        );
        drop(hold);
        other.join().unwrap();
    }
}

#[cfg(all(test, shuttle))]
mod shuttle_tests {
    use super::*;
    use crate::sync::Arc;

    /// Two contenders and a holder over every schedule shuttle tries: each contender, parked
    /// until the state is let go, ends holding it once, and none is left parked.
    #[test]
    fn contenders_are_never_left_parked_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(
            || {
                let guarded = Arc::new(Guarded::new(0u32));
                let held = guarded.try_hold();
                let contenders: Vec<_> = (0..2)
                    .map(|_| {
                        let g = guarded.clone();
                        crate::thread::spawn(move || {
                            let me = crate::thread::current();
                            let mut hold = loop {
                                if let Some(hold) = g.hold_or_contend(&me) {
                                    break hold;
                                }
                                crate::thread::park();
                            };
                            *hold += 1;
                        })
                    })
                    .collect();
                drop(held);
                for c in contenders {
                    c.join().unwrap();
                }
                assert_eq!(*guarded.try_hold().expect("free"), 2);
            },
            2_000,
        );
    }
}
