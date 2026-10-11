use q_rs::{defer, reject, resolve, when};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

fn recv<T>(rx: std::sync::mpsc::Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(5)).unwrap()
}

#[test]
fn resolve_chain() {
    let (tx, rx) = std::sync::mpsc::channel();
    resolve(21)
        .then(|x| x + 1)
        .then(move |x| tx.send(x).unwrap());
    assert_eq!(recv(rx), 22);
}

#[test]
fn rejection_skips_then() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    reject::<i32>("boom")
        .then(move |x| {
            flag.store(true, Ordering::SeqCst);
            x
        })
        .catch(move |e| {
            tx.send(e).unwrap();
            0
        });
    assert_eq!(recv(rx), "boom".to_string());
    assert!(!ran.load(Ordering::SeqCst));
}

#[test]
fn catch_recovers_into_chain() {
    let (tx, rx) = std::sync::mpsc::channel();
    reject::<i32>("nope")
        .catch(|_| 41)
        .then(move |x| tx.send(x + 1).unwrap());
    assert_eq!(recv(rx), 42);
}

#[test]
fn defer_across_threads() {
    let (d, p) = defer::<String>();
    let (tx, rx) = std::sync::mpsc::channel();
    p.then(move |v| tx.send(v).unwrap());
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        d.resolve("late".to_string());
    });
    assert_eq!(recv(rx), "late".to_string());
}

#[test]
fn when_is_identity() {
    let (tx, rx) = std::sync::mpsc::channel();
    when(resolve(7)).then(move |x| tx.send(x).unwrap());
    assert_eq!(recv(rx), 7);
}
