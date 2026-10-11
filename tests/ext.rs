use q_rs::combinators::all;
use q_rs::ext::{delay_value, later, set_onerror, PromiseState};
use q_rs::{defer, reject, resolve};
use std::sync::{mpsc::channel, Arc};
use std::time::Duration;

fn recv<T>(rx: std::sync::mpsc::Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(5)).unwrap()
}

#[test]
fn resolve_promise_adopts_fulfillment() {
    let (d, p) = defer::<i32>();
    d.resolve_promise(resolve(10));
    assert_eq!(p.inspect(), PromiseState::Fulfilled(10));
}

#[test]
fn resolve_promise_adopts_rejection() {
    let (d, p) = defer::<i32>();
    d.resolve_promise(reject::<i32>("nope"));
    assert_eq!(p.inspect(), PromiseState::Rejected("nope".to_string()));
}

#[test]
fn resolve_promise_stays_pending_then_forwards() {
    let (a, pa) = defer::<i32>();
    let (b, pb) = defer::<i32>();
    a.resolve_promise(pb);
    assert!(pa.is_pending());
    let (tx, rx) = channel();
    pa.then(move |v| tx.send(v).unwrap());
    b.resolve(10);
    assert_eq!(recv(rx), 10);
}

#[test]
fn and_then_chains_promise_returning_closure() {
    let (tx, rx) = channel();
    resolve(5)
        .and_then(|v| resolve(v * 2))
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), 10);
}

#[test]
fn and_then_inner_rejection_wins() {
    let (tx, rx) = channel();
    resolve(5)
        .and_then(|_| reject::<i32>("inner bad"))
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(rx), "inner bad".to_string());
}

#[test]
fn and_then_outer_rejection_skips_closure() {
    let (tx, rx) = channel();
    reject::<i32>("outer bad")
        .and_then(|v| resolve(v * 2))
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(rx), "outer bad".to_string());
}

#[test]
fn inspect_three_states() {
    assert_eq!(resolve(1).inspect(), PromiseState::Fulfilled(1));
    assert_eq!(
        reject::<i32>("x").inspect(),
        PromiseState::Rejected("x".to_string())
    );
    let (_d, p) = defer::<i32>();
    assert_eq!(p.inspect(), PromiseState::Pending);
    assert!(resolve(1).is_fulfilled());
    assert!(reject::<i32>("x").is_rejected());
    assert!(p.is_pending());
}

#[test]
fn done_fulfilled_is_noop() {
    resolve(1).done();
    std::thread::sleep(Duration::from_millis(50));
}

#[test]
fn done_rejected_routes_to_hook() {
    let (tx, rx) = channel();
    set_onerror(Some(Arc::new(move |e: String| tx.send(e).unwrap())));
    reject::<i32>("unhandled reason").done();
    assert_eq!(recv(rx), "unhandled reason".to_string());
    set_onerror(None);
}

#[test]
fn fin_async_waits_for_callback_promise() {
    let (tx, rx) = channel();
    resolve("foo")
        .fin_async(|| later(100))
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), "foo");
}

#[test]
fn fin_async_new_rejection_replaces_outcome() {
    let (tx, rx) = channel();
    resolve("foo")
        .fin_async(|| reject::<()>("boo"))
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(rx), "boo".to_string());
}

#[test]
fn fin_async_forwards_original_rejection() {
    let (tx, rx) = channel();
    reject::<&str>("evil")
        .fin_async(|| later(20))
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(rx), "evil".to_string());
}

#[test]
fn tap_async_observes_value_and_waits() {
    let (tx, rx) = channel();
    resolve("foo")
        .tap_async(|v| {
            assert_eq!(*v, "foo");
            later(50)
        })
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), "foo");
}

#[test]
fn tap_async_skips_callback_on_rejection() {
    let (tx, rx) = channel();
    reject::<i32>("e")
        .tap_async(|_| {
            panic!("must not run");
            #[allow(unreachable_code)]
            later(1)
        })
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(rx), "e".to_string());
}

#[test]
fn later_and_delay_value() {
    let start = std::time::Instant::now();
    let (tx, rx) = channel();
    delay_value("what", 50).then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), "what");
    assert!(start.elapsed() >= Duration::from_millis(50));
    let (tx2, rx2) = channel();
    later(20).then(move |_| tx2.send(true).unwrap());
    assert!(recv(rx2));
}

#[test]
fn all_same_promise_twice_resolves_once_each() {
    let (d, p) = defer::<i32>();
    let (tx, rx) = channel();
    all(vec![p.clone(), p])
        .then(move |_| tx.send(true).unwrap());
    d.resolve(1);
    let _ = recv(rx);
}
