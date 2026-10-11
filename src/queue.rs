use crate::{attach, defer, resolve, Deferred, Outcome, Promise};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Node<T: Clone + Send + 'static> {
    head: Promise<T>,
    tail: Promise<Node<T>>,
}

struct Ends<T: Clone + Send + 'static> {
    promise: Promise<Node<T>>,
    deferred: Option<Deferred<Node<T>>>,
}

pub struct Queue<T: Clone + Send + 'static> {
    ends: Mutex<Ends<T>>,
    closed_slot: Arc<Mutex<Option<Deferred<String>>>>,
    closed_promise: Promise<String>,
}

impl<T: Clone + Send + 'static> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send + 'static> Queue<T> {
    pub fn new() -> Self {
        let (ends_d, ends_p) = defer::<Node<T>>();
        let (closed_d, closed_p) = defer::<String>();
        Queue {
            ends: Mutex::new(Ends {
                promise: ends_p,
                deferred: Some(ends_d),
            }),
            closed_slot: Arc::new(Mutex::new(Some(closed_d))),
            closed_promise: closed_p,
        }
    }

    pub fn put(&self, value: T) {
        let (next_d, next_p) = defer::<Node<T>>();
        let node = Node {
            head: resolve(value),
            tail: next_p,
        };
        let mut ends = self.ends.lock().unwrap();
        if let Some(d) = ends.deferred.take() {
            d.resolve(node);
            ends.deferred = Some(next_d);
        }
    }

    pub fn get(&self) -> Promise<T> {
        let head;
        {
            let mut ends = self.ends.lock().unwrap();
            let current = ends.promise.clone();
            ends.promise = current.clone().and_then(|node| node.tail);
            head = current.and_then(|node| node.head);
        }
        let slot = self.closed_slot.clone();
        let (d, p) = defer::<T>();
        attach(
            &head,
            Box::new(move |o| match o {
                Outcome::Ok(v) => d.resolve(v),
                Outcome::Err(e) => {
                    if let Some(cd) = slot.lock().unwrap().take() {
                        cd.resolve(e.clone());
                    }
                    d.reject(e);
                }
            }),
        );
        p
    }

    pub fn closed(&self) -> Promise<String> {
        self.closed_promise.clone()
    }

    pub fn close(&self, error: Option<String>) -> Promise<String> {
        let msg = error.unwrap_or_else(|| "Can't get value from closed queue".to_string());
        let (loop_d, loop_p) = defer::<Node<T>>();
        let end = Node {
            head: crate::reject::<T>(&msg),
            tail: loop_p,
        };
        loop_d.resolve(end.clone());
        let mut ends = self.ends.lock().unwrap();
        if let Some(d) = ends.deferred.take() {
            d.resolve(end);
        }
        self.closed_promise.clone()
    }
}
