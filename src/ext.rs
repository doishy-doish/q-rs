use crate::{attach, defer, settle, Deferred, Outcome, Promise};
use std::sync::{Arc, Mutex, OnceLock};

static ONERROR: OnceLock<Mutex<Option<Arc<dyn Fn(String) + Send + Sync>>>> = OnceLock::new();

fn onerror_slot() -> &'static Mutex<Option<Arc<dyn Fn(String) + Send + Sync>>> {
    ONERROR.get_or_init(|| Mutex::new(None))
}

pub fn set_onerror(hook: Option<Arc<dyn Fn(String) + Send + Sync>>) {
    *onerror_slot().lock().unwrap() = hook;
}

#[derive(Clone, Debug, PartialEq)]
pub enum PromiseState<T> {
    Pending,
    Fulfilled(T),
    Rejected(String),
}

impl<T: Clone + Send + 'static> Deferred<T> {
    pub fn resolve_promise(self, promise: Promise<T>) {
        let inner = self.inner;
        attach(&promise, Box::new(move |o| settle(&inner, o)));
    }
}

impl<T: Clone + Send + 'static> Promise<T> {
    pub fn inspect(&self) -> PromiseState<T> {
        match &self.inner.lock().unwrap().state {
            crate::State::Pending(_) => PromiseState::Pending,
            crate::State::Done(Outcome::Ok(v)) => PromiseState::Fulfilled(v.clone()),
            crate::State::Done(Outcome::Err(e)) => PromiseState::Rejected(e.clone()),
        }
    }

    pub fn is_fulfilled(&self) -> bool {
        matches!(self.inspect(), PromiseState::Fulfilled(_))
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self.inspect(), PromiseState::Rejected(_))
    }

    pub fn is_pending(&self) -> bool {
        matches!(self.inspect(), PromiseState::Pending)
    }

    pub fn and_then<U, F>(self, f: F) -> Promise<U>
    where
        U: Clone + Send + 'static,
        F: FnOnce(T) -> Promise<U> + Send + 'static,
    {
        let (d, p) = defer::<U>();
        attach(
            &self,
            Box::new(move |o| match o {
                Outcome::Ok(v) => d.resolve_promise(f(v)),
                Outcome::Err(e) => d.reject(e),
            }),
        );
        p
    }

    pub fn done(self) {
        attach(
            &self,
            Box::new(move |o| {
                if let Outcome::Err(e) = o {
                    let hook = onerror_slot().lock().unwrap().clone();
                    if let Some(h) = hook {
                        h(e);
                    } else {
                        panic!("unhandled promise rejection: {e}");
                    }
                }
            }),
        );
    }

    pub fn fin_async<U, F>(self, observe: F) -> Promise<T>
    where
        U: Clone + Send + 'static,
        F: FnOnce() -> Promise<U> + Send + 'static,
    {
        let (d, p) = defer::<T>();
        attach(
            &self,
            Box::new(move |o| {
                let waited = std::panic::catch_unwind(std::panic::AssertUnwindSafe(observe));
                let waiter = match waited {
                    Ok(w) => w,
                    Err(payload) => {
                        d.reject(crate::combinators::panic_message(payload));
                        return;
                    }
                };
                match o {
                    Outcome::Ok(v) => {
                        attach(
                            &waiter,
                            Box::new(move |o2| match o2 {
                                Outcome::Ok(_) => d.resolve(v),
                                Outcome::Err(e) => d.reject(e),
                            }),
                        );
                    }
                    Outcome::Err(e) => {
                        attach(
                            &waiter,
                            Box::new(move |o2| match o2 {
                                Outcome::Ok(_) => d.reject(e),
                                Outcome::Err(e2) => d.reject(e2),
                            }),
                        );
                    }
                }
            }),
        );
        p
    }

    pub fn tap_async<U, F>(self, observe: F) -> Promise<T>
    where
        U: Clone + Send + 'static,
        F: FnOnce(&T) -> Promise<U> + Send + 'static,
    {
        let (d, p) = defer::<T>();
        attach(
            &self,
            Box::new(move |o| match o {
                Outcome::Ok(v) => {
                    let waited = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        observe(&v)
                    }));
                    let waiter = match waited {
                        Ok(w) => w,
                        Err(payload) => {
                            d.reject(crate::combinators::panic_message(payload));
                            return;
                        }
                    };
                    attach(
                        &waiter,
                        Box::new(move |o2| match o2 {
                            Outcome::Ok(_) => d.resolve(v),
                            Outcome::Err(e2) => d.reject(e2),
                        }),
                    );
                }
                Outcome::Err(e) => d.reject(e),
            }),
        );
        p
    }
}

pub fn later(ms: u64) -> Promise<()> {
    crate::resolve(()).delay(ms)
}

pub fn delay_value<T: Clone + Send + 'static>(value: T, ms: u64) -> Promise<T> {
    crate::resolve(value).delay(ms)
}
