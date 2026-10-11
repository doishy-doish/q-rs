use q_rs::queue::Queue;
use std::sync::mpsc::channel;
use std::time::Duration;

fn recv<T>(rx: std::sync::mpsc::Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(5)).unwrap()
}

#[test]
fn queue_fifo_order() {
    let q = Queue::new();
    q.put(1);
    q.put(2);
    q.put(3);
    let (tx, rx) = channel();
    q.get().then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), 1);
    let (tx2, rx2) = channel();
    q.get().then(move |v| tx2.send(v).unwrap());
    assert_eq!(recv(rx2), 2);
    let (tx3, rx3) = channel();
    q.get().then(move |v| tx3.send(v).unwrap());
    assert_eq!(recv(rx3), 3);
}

#[test]
fn queue_get_pends_until_put() {
    let q = Queue::new();
    let p = q.get();
    std::thread::sleep(Duration::from_millis(50));
    assert!(p.is_pending());
    let (tx, rx) = channel();
    p.then(move |v| tx.send(v).unwrap());
    q.put(42);
    assert_eq!(recv(rx), 42);
}

#[test]
fn queue_interleaved_put_get() {
    let q = Queue::new();
    q.put("a".to_string());
    let (tx, rx) = channel();
    q.get().then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), "a".to_string());
    q.put("b".to_string());
    q.put("c".to_string());
    let (tx2, rx2) = channel();
    q.get().then(move |v| tx2.send(v).unwrap());
    let (tx3, rx3) = channel();
    q.get().then(move |v| tx3.send(v).unwrap());
    assert_eq!(recv(rx2), "b".to_string());
    assert_eq!(recv(rx3), "c".to_string());
}

#[test]
fn queue_close_default_error() {
    let q = Queue::<i32>::new();
    q.close(None);
    let (tx, rx) = channel();
    q.get().then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    assert_eq!(recv(rx), "Can't get value from closed queue".to_string());
}

#[test]
fn queue_close_custom_error_and_closed_promise() {
    let q = Queue::<i32>::new();
    let closed_p = q.closed();
    std::thread::sleep(Duration::from_millis(20));
    assert!(closed_p.is_pending());
    q.close(Some("shut down".to_string()));
    let (tx, rx) = channel();
    q.get().then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    assert_eq!(recv(rx), "shut down".to_string());
    let (tx2, rx2) = channel();
    closed_p.then(move |v| tx2.send(v).unwrap());
    assert_eq!(recv(rx2), "shut down".to_string());
}

#[test]
fn queue_repeated_get_after_close_same_error() {
    let q = Queue::<i32>::new();
    q.put(1);
    q.close(Some("bye".to_string()));
    let (tx, rx) = channel();
    q.get().then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(rx), 1);
    for _ in 0..2 {
        let (txe, rxe) = channel();
        q.get().then_full(
        move |v| v.to_string(),
        move |e| {
            txe.send(e).unwrap();
            String::new()
        },
        );
        assert_eq!(recv(rxe), "bye".to_string());
    }
}

#[test]
fn queue_put_after_close_dropped() {
    let q = Queue::new();
    q.close(Some("gone".to_string()));
    q.put(99);
    let (tx, rx) = channel();
    q.get().then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    assert_eq!(recv(rx), "gone".to_string());
}
