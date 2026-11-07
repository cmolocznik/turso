//! State another holder may hold (DESIGN.md 22.2 item 3): [`Guarded`] owns the state and the
//! takers waiting for it, and [`Hold`] is its guard.
//!
//! The state is a private field here, and [`Guarded::try_hold`] and [`Guarded::hold_or_contend`]
//! are the only ways to it, so no other code can take it without the guard (a test compiles the
//! attempt and expects it refused). A taker that finds the state held may register as a contender
//! and try again; whenever a contending taker holds the state, at its first try or its second, it
//! takes back any registration of its own, so a holder is never a contender of its own hold. Letting a hold go lets the state go first, then wakes every
//! contender and empties the list: a contender woken while the state was still held would find it
//! held, wait again, and nothing would wake it. Nobody but a contender is woken.

use super::locks::{Mutex, MutexGuard};
use crate::thread::Thread;
use std::ops::{Deref, DerefMut};

#[cfg(test)]
std::thread_local! {
    /// Run by this thread's next [`Guarded::hold_or_contend`] between the try that found the state
    /// held and its registration: a test lets the state go there.
    static BEFORE_CONTENDING: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };

    /// Run by this thread's next [`Hold`] let go between letting the state go and waking its
    /// contenders: a test takes the state there.
    static BEFORE_WAKING: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
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
            return Some(self.withdrawn(held, me));
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
        Some(self.withdrawn(held, me))
    }

    /// `held`, with any registration of `me`'s taken back: its own letting go must not wake it.
    /// A contender may hold at its first try while still listed from an earlier call: woken by
    /// another's letting go, or for no reason, before the holder it waited for woke anyone.
    fn withdrawn<'a>(&'a self, held: Hold<'a, T>, me: &Thread) -> Hold<'a, T> {
        for t in self.contenders.lock().iter_mut() {
            if t.as_ref().is_some_and(|t| t.id() == me.id()) {
                *t = None;
            }
        }
        held
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
        super::unwind::abort_if_unwinding();
        drop(self.state.take());
        #[cfg(test)]
        if let Some(between) = BEFORE_WAKING.with(|b| b.borrow_mut().take()) {
            between();
        }
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
    /// stand-ins for Atoll's `locks` and `unwind` and the crate's `thread`, compiles when it takes
    /// the state through `try_hold` and is refused, with the private field named, when it reaches
    /// for it.
    #[test]
    fn the_state_is_reachable_only_through_the_guard() {
        let module = concat!(env!("CARGO_MANIFEST_DIR"), "/storage/atoll/guarded.rs");
        let crate_root = |body: &str| {
            format!(
                "mod locks {{\n\
                     pub struct Mutex<T>(std::sync::Mutex<T>);\n\
                     pub type MutexGuard<'a, T> = std::sync::MutexGuard<'a, T>;\n\
                     impl<T> Mutex<T> {{\n\
                         pub fn new(t: T) -> Self {{ Self(std::sync::Mutex::new(t)) }}\n\
                         pub fn lock(&self) -> MutexGuard<'_, T> {{ self.0.lock().unwrap() }}\n\
                         pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {{ self.0.try_lock().ok() }}\n\
                     }}\n\
                 }}\n\
                 mod thread {{ pub use std::thread::*; }}\n\
                 mod unwind {{ pub fn abort_if_unwinding() {{}} }}\n\
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

    /// A contender still listed from an earlier call that takes the state at its first try,
    /// after the holder it waited for let the state go but before that holder woke anyone, takes
    /// its registration back too: its own letting go wakes nobody, itself included.
    #[test]
    fn a_contender_that_holds_at_its_first_try_is_listed_no_more() {
        let guarded = Arc::new(Guarded::new(0u32));
        let me = std::thread::current();
        let first = guarded.try_hold().expect("free");
        assert!(guarded.hold_or_contend(&me).is_none(), "held, so listed");
        assert!(guarded.contends(&me));
        let listed = std::rc::Rc::new(std::cell::Cell::new(None));
        let (seen, g, taker) = (listed.clone(), guarded.clone(), me.clone());
        BEFORE_WAKING.with(|b| {
            *b.borrow_mut() = Some(Box::new(move || {
                let hold = g
                    .hold_or_contend(&taker)
                    .expect("let go, so taken at the first try");
                seen.set(Some(g.contends(&taker)));
                drop(hold);
            }))
        });
        drop(first);
        assert_eq!(
            listed.get(),
            Some(false),
            "the holder is still listed, so its own letting go wakes it"
        );
    }
}

#[cfg(all(test, unix, not(any(shuttle, atoll_loom, miri))))]
mod child_tests {
    use super::*;
    use crate::storage::atoll::testing::{in_child, record, recorded};
    use std::sync::Arc;

    /// A panic while a guarded state is held ends the process before it is let go or anyone
    /// waiting for it is woken (DESIGN.md 22.11, proof 9). In a child process a contender parks
    /// on the held state and its holder panics: the child must end by abort, and the contender,
    /// recording what it sees to a synced file, must never have been woken to take the state.
    #[test]
    fn a_panic_while_holding_a_guarded_state_ends_the_process_before_a_contender_wakes() {
        let ended = in_child(
            concat!(
                module_path!(),
                "::a_panic_while_holding_a_guarded_state_ends_the_process_before_a_contender_wakes"
            ),
            || {
                let guarded = Arc::new(Guarded::new(0u32));
                let (held, parked) = (std::sync::mpsc::channel(), std::sync::mpsc::channel());
                let holder = {
                    let guarded = guarded.clone();
                    let (held, parked) = (held.0, parked.1);
                    std::thread::spawn(move || {
                        let mut state = guarded.try_hold().expect("free");
                        *state = 1;
                        held.send(()).unwrap();
                        parked.recv().unwrap();
                        panic!("a holder failed");
                    })
                };
                held.1.recv().unwrap();
                let contender = std::thread::spawn(move || {
                    let me = std::thread::current();
                    record("seen", "contending");
                    let state = loop {
                        if let Some(state) = guarded.hold_or_contend(&me) {
                            break state;
                        }
                        let _ = parked.0.send(());
                        std::thread::park();
                    };
                    record("seen", &format!("woken {}", *state));
                });
                let _ = holder.join();
                contender.join().unwrap();
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
            seen.contains("contending"),
            "the contender never ran: {seen:?}"
        );
        assert!(!seen.contains("woken"), "the contender was woken: {seen:?}");
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
