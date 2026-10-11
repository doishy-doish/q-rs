use q_rs::combinators::{all, all_settled, any, denodeify, fcall, nfcall, Settlement};
use q_rs::{defer, reject, resolve};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::channel,
    Arc,
};
use std::time::{Duration, Instant};

fn recv<T>(rx: std::sync::mpsc::Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(5)).unwrap()
}

#[test]
fn all_keeps_input_order() {
    let (d0, p0) = defer::<i32>();
    let (d1, p1) = defer::<i32>();
    let (d2, p2) = defer::<i32>();
    let (tx, rx) = channel();
    all(vec![p0, p1, p2]).then(move |v| tx.send(v).unwrap());
    d2.resolve(30);
    d0.resolve(10);
    d1.resolve(20);
    assert_eq!(recv(rx), vec![10, 20, 30]);
}

#[test]
fn all_rejects_on_first_error() {
    let (d0, p0) = defer::<i32>();
    let (d1, p1) = defer::<i32>();
    let (tx, rx) = channel();
    all(vec![p0, p1]).then_full(
        move |v| format!("{v:?}"),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    d1.resolve(1);
    d0.reject("bad".to_string());
    assert_eq!(recv(rx), "bad".to_string());
}

#[test]
fn any_first_fulfillment_wins() {
    let (d0, p0) = defer::<i32>();
    let p1 = resolve(100);
    let (tx, rx) = channel();
    any(vec![p0, p1]).then(move |v| tx.send(v).unwrap());
    std::thread::sleep(Duration::from_millis(50));
    d0.resolve(1);
    assert_eq!(recv(rx), 100);
}

#[test]
fn any_all_rejected_reports() {
    let (tx, rx) = channel();
    any(vec![reject::<i32>("first"), reject::<i32>("second")]).then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    let err = recv(rx);
    assert!(err.contains("all promises were rejected"), "{err}");
    assert!(err.contains("second"), "{err}");
}

#[test]
fn all_settled_mixed() {
    let (tx, rx) = channel();
    all_settled(vec![resolve(1), reject::<i32>("x")]).then(move |v| tx.send(v).unwrap());
    assert_eq!(
        recv(rx),
        vec![
            Settlement::Fulfilled(1),
            Settlement::Rejected("x".to_string())
        ]
    );
}

#[test]
fn timeout_rejects_slow_promise() {
    let (_d, p) = defer::<i32>();
    let (tx, rx) = channel();
    p.timeout(50, None).then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    let err = recv(rx);
    assert!(err.contains("Timed out after 50 ms"), "{err}");
}

#[test]
fn timeout_passes_fast_promise() {
    let (tx, rx) = channel();
    resolve(5)
        .timeout(5000, None)
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), 5);
}

#[test]
fn delay_holds_value_past_deadline() {
    let (tx, rx) = channel();
    let start = Instant::now();
    resolve(9).delay(100).then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), 9);
    assert!(start.elapsed() >= Duration::from_millis(100));
}

fn add_one(arg: i32, cb: Box<dyn FnOnce(Result<i32, String>) + Send>) {
    cb(Ok(arg + 1));
}

fn always_fails(arg: i32, cb: Box<dyn FnOnce(Result<i32, String>) + Send>) {
    let _ = arg;
    cb(Err("nope".to_string()));
}

#[test]
fn nfcall_wraps_node_style_ok() {
    let (tx, rx) = channel();
    nfcall(add_one, 41).then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), 42);
}

#[test]
fn denodeify_forwards_node_error() {
    let (tx, rx) = channel();
    denodeify(always_fails)(0).then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    assert_eq!(recv(rx), "nope".to_string());
}

#[test]
fn fcall_panic_becomes_rejection() {
    let (tx, rx) = channel();
    fcall(|| -> i32 { panic!("kaboom") }).then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    let err = recv(rx);
    assert!(err.contains("kaboom"), "{err}");
}

#[test]
fn fin_observes_and_forwards_rejection() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let (tx, rx) = channel();
    reject::<i32>("e")
        .fin(move || flag.store(true, Ordering::SeqCst))
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(rx), "e".to_string());
    assert!(ran.load(Ordering::SeqCst));
}

#[test]
fn spread_applies_promised_vec() {
    let (tx, rx) = channel();
    resolve(vec![1, 2, 3])
        .spread(|v| v.iter().sum::<i32>())
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), 6);
}
