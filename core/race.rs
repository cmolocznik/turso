//! Points where a test runs another party's action between two steps of the code under test, so a
//! race's order is chosen rather than hoped for (DESIGN.md 9.1.2's "To prove"). A test sets an
//! action at a named point on its own thread; the code runs it once, the first time the thread
//! passes that point. Outside tests a point is nothing.

#[cfg(test)]
use std::cell::RefCell;

/// An action set at a named point.
#[cfg(test)]
type Action = (&'static str, Box<dyn FnOnce()>);

#[cfg(test)]
crate::thread::thread_local! {
    static ACTIONS: RefCell<Vec<Action>> = const { RefCell::new(Vec::new()) };
}

/// Runs the action set at `name` on this thread, if one is.
#[inline(always)]
pub(crate) fn point(name: &'static str) {
    #[cfg(test)]
    {
        let action = ACTIONS.with(|a| {
            let mut a = a.borrow_mut();
            let i = a.iter().position(|(n, _)| *n == name)?;
            Some(a.remove(i).1)
        });
        if let Some(action) = action {
            action();
        }
    }
    #[cfg(not(test))]
    let _ = name;
}

/// Sets `action` to run once at `name` on this thread.
#[cfg(test)]
pub(crate) fn at(name: &'static str, action: impl FnOnce() + 'static) {
    ACTIONS.with(|a| a.borrow_mut().push((name, Box::new(action))));
}

/// Whether an action set on this thread is still waiting for its point.
#[cfg(test)]
pub(crate) fn pending(name: &'static str) -> bool {
    ACTIONS.with(|a| a.borrow().iter().any(|(n, _)| *n == name))
}
