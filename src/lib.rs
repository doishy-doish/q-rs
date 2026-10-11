use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Clone)]
enum Outcome<T> {
    Ok(T),
    Err(String),
}

enum State<T> {
    Pending(Vec<Box<dyn FnOnce(Outcome<T>) + Send>>),
    Done(Outcome<T>),
}

struct Inner<T> {
    state: State<T>,
}

#[derive(Clone)]
pub struct Promise<T: Clone + Send + 'static> {
    inner: Arc<Mutex<Inner<T>>>,
}

pub struct Deferred<T: Clone + Send + 'static> {
    inner: Arc<Mutex<Inner<T>>>,
}

pub fn defer<T: Clone + Send + 'static>() -> (Deferred<T>, Promise<T>) {
    let inner = Arc::new(Mutex::new(Inner {
        state: State::Pending(Vec::new()),
    }));
    (
        Deferred {
            inner: inner.clone(),
        },
        Promise { inner },
    )
}

fn settle<T: Clone + Send + 'static>(inner: &Arc<Mutex<Inner<T>>>, outcome: Outcome<T>) {
    let callbacks = {
        let mut guard = inner.lock().unwrap();
        match &mut guard.state {
            State::Pending(cbs) => {
                let taken = std::mem::replace(cbs, Vec::new());
                guard.state = State::Done(outcome.clone());
                taken
            }
            State::Done(_) => return,
        }
    };
    thread::spawn(move || {
        for cb in callbacks {
            cb(outcome.clone());
        }
    });
}

fn attach<T: Clone + Send + 'static>(promise: &Promise<T>, run: Box<dyn FnOnce(Outcome<T>) + Send>) {
    let done = {
        let guard = promise.inner.lock().unwrap();
        match &guard.state {
            State::Done(o) => Some(o.clone()),
            State::Pending(_) => None,
        }
    };
    if let Some(o) = done {
        thread::spawn(move || run(o));
        return;
    }
    let mut guard = promise.inner.lock().unwrap();
    match &mut guard.state {
        State::Pending(cbs) => cbs.push(run),
        State::Done(o) => {
            let o = o.clone();
            drop(guard);
            thread::spawn(move || run(o));
        }
    }
}

impl<T: Clone + Send + 'static> Deferred<T> {
    pub fn resolve(self, value: T) {
        settle(&self.inner, Outcome::Ok(value));
    }

    pub fn reject(self, reason: String) {
        settle(&self.inner, Outcome::Err(reason));
    }
}

impl<T: Clone + Send + 'static> Promise<T> {
    pub fn then<U, F>(self, on_ok: F) -> Promise<U>
    where
        U: Clone + Send + 'static,
        F: FnOnce(T) -> U + Send + 'static,
    {
        let (d, p) = defer::<U>();
        attach(
            &self,
            Box::new(move |o: Outcome<T>| match o {
                Outcome::Ok(v) => d.resolve(on_ok(v)),
                Outcome::Err(e) => d.reject(e),
            }),
        );
        p
    }

    pub fn catch<F>(self, on_err: F) -> Promise<T>
    where
        F: FnOnce(String) -> T + Send + 'static,
    {
        let (d, p) = defer::<T>();
        attach(
            &self,
            Box::new(move |o: Outcome<T>| match o {
                Outcome::Ok(v) => d.resolve(v),
                Outcome::Err(e) => d.resolve(on_err(e)),
            }),
        );
        p
    }
}

pub fn resolve<T: Clone + Send + 'static>(value: T) -> Promise<T> {
    let (d, p) = defer();
    d.resolve(value);
    p
}

pub fn reject<T: Clone + Send + 'static>(reason: &str) -> Promise<T> {
    let (d, p) = defer();
    d.reject(reason.to_string());
    p
}

pub fn when<T: Clone + Send + 'static>(promise: Promise<T>) -> Promise<T> {
    promise
}
