use crate::{attach, defer, Deferred, Outcome, Promise};
use std::sync::{Arc, Mutex};
use std::time::Duration;

impl<T: Clone + Send + 'static> Promise<T> {
    pub fn then_full<U, F, G>(self, on_ok: F, on_err: G) -> Promise<U>
    where
        U: Clone + Send + 'static,
        F: FnOnce(T) -> U + Send + 'static,
        G: FnOnce(String) -> U + Send + 'static,
    {
        let (d, p) = defer::<U>();
        attach(
            &self,
            Box::new(move |o: Outcome<T>| match o {
                Outcome::Ok(v) => d.resolve(on_ok(v)),
                Outcome::Err(e) => d.resolve(on_err(e)),
            }),
        );
        p
    }

    pub fn fin<F>(self, observe: F) -> Promise<T>
    where
        F: FnOnce() + Send + 'static,
    {
        let (d, p) = defer::<T>();
        attach(
            &self,
            Box::new(move |o: Outcome<T>| {
                observe();
                match o {
                    Outcome::Ok(v) => d.resolve(v),
                    Outcome::Err(e) => d.reject(e),
                }
            }),
        );
        p
    }

    pub fn nodeify<F>(self, nodeback: Option<F>) -> Promise<T>
    where
        F: FnOnce(Result<T, String>) + Send + 'static,
    {
        if let Some(cb) = nodeback {
            attach(
                &self,
                Box::new(move |o: Outcome<T>| match o {
                    Outcome::Ok(v) => cb(Ok(v)),
                    Outcome::Err(e) => cb(Err(e)),
                }),
            );
        }
        self
    }

    pub fn timeout(self, ms: u64, message: Option<String>) -> Promise<T> {
        let (d, p) = defer::<T>();
        let slot: Arc<Mutex<Option<Deferred<T>>>> = Arc::new(Mutex::new(Some(d)));
        let timer_slot = slot.clone();
        let msg = message.unwrap_or_else(|| format!("Timed out after {ms} ms"));
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(ms));
            if let Some(dd) = timer_slot.lock().unwrap().take() {
                dd.reject(msg);
            }
        });
        let winner_slot = slot.clone();
        attach(
            &self,
            Box::new(move |o: Outcome<T>| {
                if let Some(dd) = winner_slot.lock().unwrap().take() {
                    match o {
                        Outcome::Ok(v) => dd.resolve(v),
                        Outcome::Err(e) => dd.reject(e),
                    }
                }
            }),
        );
        p
    }

    pub fn delay(self, ms: u64) -> Promise<T> {
        self.then(move |v| {
            std::thread::sleep(Duration::from_millis(ms));
            v
        })
    }
}

impl<T: Clone + Send + 'static> Promise<Vec<T>> {
    pub fn spread<U, F>(self, f: F) -> Promise<U>
    where
        U: Clone + Send + 'static,
        F: FnOnce(Vec<T>) -> U + Send + 'static,
    {
        self.then(f)
    }
}

struct AllShared<T: Send + 'static> {
    left: usize,
    settled: bool,
    out: Vec<Option<T>>,
    deferred: Option<Deferred<Vec<T>>>,
}

pub fn all<T: Clone + Send + 'static>(promises: Vec<Promise<T>>) -> Promise<Vec<T>> {
    let (d, p) = defer::<Vec<T>>();
    if promises.is_empty() {
        d.resolve(Vec::new());
        return p;
    }
    let n = promises.len();
    let shared = Arc::new(Mutex::new(AllShared {
        left: n,
        settled: false,
        out: (0..n).map(|_| None).collect(),
        deferred: Some(d),
    }));
    for (i, promise) in promises.into_iter().enumerate() {
        let shared = shared.clone();
        attach(
            &promise,
            Box::new(move |o: Outcome<T>| {
                let mut guard = shared.lock().unwrap();
                if guard.settled {
                    return;
                }
                match o {
                    Outcome::Ok(v) => {
                        guard.out[i] = Some(v);
                        guard.left -= 1;
                        if guard.left == 0 {
                            guard.settled = true;
                            let values: Vec<T> = std::mem::take(&mut guard.out)
                                .into_iter()
                                .map(|slot| slot.unwrap())
                                .collect();
                            if let Some(dd) = guard.deferred.take() {
                                dd.resolve(values);
                            }
                        }
                    }
                    Outcome::Err(e) => {
                        guard.settled = true;
                        if let Some(dd) = guard.deferred.take() {
                            dd.reject(e);
                        }
                    }
                }
            }),
        );
    }
    p
}

struct AnyShared<T: Send + 'static> {
    left: usize,
    settled: bool,
    deferred: Option<Deferred<T>>,
}

pub fn any<T: Clone + Send + 'static>(promises: Vec<Promise<T>>) -> Promise<T> {
    let (d, p) = defer::<T>();
    if promises.is_empty() {
        d.reject(
            "Q can't get fulfillment value from any promise, all promises were rejected."
                .to_string(),
        );
        return p;
    }
    let n = promises.len();
    let shared = Arc::new(Mutex::new(AnyShared {
        left: n,
        settled: false,
        deferred: Some(d),
    }));
    for promise in promises.into_iter() {
        let shared = shared.clone();
        attach(
            &promise,
            Box::new(move |o: Outcome<T>| {
                let mut guard = shared.lock().unwrap();
                if guard.settled {
                    return;
                }
                match o {
                    Outcome::Ok(v) => {
                        guard.settled = true;
                        if let Some(dd) = guard.deferred.take() {
                            dd.resolve(v);
                        }
                    }
                    Outcome::Err(e) => {
                        guard.left -= 1;
                        if guard.left == 0 {
                            guard.settled = true;
                            let msg = format!(
                                "Q can't get fulfillment value from any promise, all \
                                 promises were rejected. Last error: {e}"
                            );
                            if let Some(dd) = guard.deferred.take() {
                                dd.reject(msg);
                            }
                        }
                    }
                }
            }),
        );
    }
    p
}

#[derive(Clone, Debug, PartialEq)]
pub enum Settlement<T> {
    Fulfilled(T),
    Rejected(String),
}

pub fn all_settled<T: Clone + Send + 'static>(
    promises: Vec<Promise<T>>,
) -> Promise<Vec<Settlement<T>>> {
    if promises.is_empty() {
        let (d, p) = defer::<Vec<Settlement<T>>>();
        d.resolve(Vec::new());
        return p;
    }
    let wrapped: Vec<Promise<Settlement<T>>> = promises
        .into_iter()
        .map(|pr| pr.then_full(Settlement::Fulfilled, Settlement::Rejected))
        .collect();
    all(wrapped)
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

pub fn fcall<F, U>(f: F) -> Promise<U>
where
    F: FnOnce() -> U + Send + 'static,
    U: Clone + Send + 'static,
{
    let (d, p) = defer::<U>();
    std::thread::spawn(move || {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        match outcome {
            Ok(v) => d.resolve(v),
            Err(payload) => d.reject(panic_message(payload)),
        }
    });
    p
}

pub fn fbind<F, U>(f: F) -> impl Fn() -> Promise<U>
where
    F: FnOnce() -> U + Clone + Send + 'static,
    U: Clone + Send + 'static,
{
    move || fcall(f.clone())
}

pub fn denodeify<A, B, F>(f: F) -> impl Fn(A) -> Promise<B>
where
    A: Send + 'static,
    B: Clone + Send + 'static,
    F: Fn(A, Box<dyn FnOnce(Result<B, String>) + Send>) + Clone + Send + 'static,
{
    move |arg: A| {
        let (d, p) = defer::<B>();
        let g = f.clone();
        g(
            arg,
            Box::new(move |r| match r {
                Ok(v) => d.resolve(v),
                Err(e) => d.reject(e),
            }),
        );
        p
    }
}

pub fn nfbind<A, B, F>(f: F) -> impl Fn(A) -> Promise<B>
where
    A: Send + 'static,
    B: Clone + Send + 'static,
    F: Fn(A, Box<dyn FnOnce(Result<B, String>) + Send>) + Clone + Send + 'static,
{
    denodeify(f)
}

pub fn nfcall<A, B, F>(f: F, arg: A) -> Promise<B>
where
    A: Send + 'static,
    B: Clone + Send + 'static,
    F: Fn(A, Box<dyn FnOnce(Result<B, String>) + Send>) + Clone + Send + 'static,
{
    denodeify(f)(arg)
}
