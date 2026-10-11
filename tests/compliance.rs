use q_rs::combinators::{all, all_settled, any, denodeify};
use q_rs::ext::{delay_value, later, set_onerror};
use q_rs::{defer, reject, resolve};
use std::sync::{mpsc::channel, Arc};
use std::time::{Duration, Instant};

fn recv<T>(rx: &std::sync::mpsc::Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(5)).unwrap()
}

#[test]
fn spec_sum_chain_does_not_blow_stack() {
    let mut p = resolve(0);
    for i in 1..=1000 {
        p = p.then(move |acc| acc + i);
    }
    let (tx, rx) = channel();
    p.then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), 1000 * 1001 / 2);
}

#[test]
fn spec_callbacks_run_async_never_sync() {
    let (d, p) = defer::<i32>();
    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = ran.clone();
    p.then(move |v| {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        v
    });
    assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
    d.resolve(1);
    std::thread::sleep(Duration::from_millis(100));
    assert!(ran.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn spec_multiple_callbacks_fire_in_attach_order() {
    let (d, p) = defer::<i32>();
    let (tx, rx) = channel();
    for i in 0..5 {
        let tx = tx.clone();
        p.clone().then(move |_| tx.send(i).unwrap());
    }
    d.resolve(0);
    let mut got = Vec::new();
    for _ in 0..5 {
        got.push(recv(&rx));
    }
    assert_eq!(got, vec![0, 1, 2, 3, 4]);
}

#[test]
fn spec_value_propagates_through_modifying_chain() {
    let (tx, rx) = channel();
    resolve(10)
        .then(|ten| ten + 10)
        .then(move |twen| tx.send(twen).unwrap());
    assert_eq!(recv(&rx), 20);
}

#[test]
fn spec_errback_recovers_to_value() {
    let (tx, rx) = channel();
    reject::<i32>("bah")
        .then_full(|v| v, |_| 10)
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), 10);
}

#[test]
fn spec_rejection_flows_past_value_callbacks() {
    let (tx, rx) = channel();
    reject::<i32>("foolish")
        .then(|v| v + 1)
        .then(|v| v + 1)
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(&rx), "foolish".to_string());
}

#[test]
fn spec_resolution_forwarded_through_assimilated_deferred() {
    let (a, pa) = defer::<i32>();
    let (b, _pb) = defer::<i32>();
    let (tx, rx) = channel();
    pa.then(move |v| tx.send(v).unwrap());
    a.resolve_promise(_pb);
    b.resolve(10);
    assert_eq!(recv(&rx), 10);
}

#[test]
fn spec_all_empty_resolves_empty() {
    let (tx, rx) = channel();
    all(Vec::<q_rs::Promise<i32>>::new()).then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), Vec::<i32>::new());
}

#[test]
fn spec_all_mixed_values_and_promises() {
    let (d, p) = defer::<i32>();
    let (tx, rx) = channel();
    all(vec![resolve(1), p, resolve(3)]).then(move |v| tx.send(v).unwrap());
    d.resolve(2);
    assert_eq!(recv(&rx), vec![1, 2, 3]);
}

#[test]
fn spec_any_empty_rejects_documented_divergence() {
    let (tx, rx) = channel();
    any(Vec::<q_rs::Promise<i32>>::new()).then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    assert!(recv(&rx).contains("all promises were rejected"));
}

#[test]
fn spec_any_first_staggered_fulfillment_wins() {
    let (d0, p0) = defer::<String>();
    let (d1, p1) = defer::<String>();
    let (tx, rx) = channel();
    any(vec![p0, p1]).then(move |v| tx.send(v).unwrap());
    let t0 = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(60));
        d0.resolve("slow".to_string());
    });
    let t1 = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        d1.resolve("fast".to_string());
    });
    assert_eq!(recv(&rx), "fast".to_string());
    t0.join().unwrap();
    t1.join().unwrap();
}

#[test]
fn spec_any_rejection_message_matches_q() {
    let (tx, rx) = channel();
    any(vec![reject::<i32>("first"), reject::<i32>("second")]).then_full(
        move |v| v.to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    assert_eq!(
        recv(&rx),
        "Q can't get fulfillment value from any promise, all promises were rejected. Last error message: second"
    );
}

#[test]
fn spec_all_settled_empty() {
    let (tx, rx) = channel();
    all_settled(Vec::<q_rs::Promise<i32>>::new()).then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), Vec::new());
}

#[test]
fn spec_all_settled_mixed_snapshot_shapes() {
    use q_rs::combinators::Settlement;
    let (tx, rx) = channel();
    all_settled(vec![resolve(2), reject::<i32>("nope")]).then(move |v| tx.send(v).unwrap());
    assert_eq!(
        recv(&rx),
        vec![
            Settlement::Fulfilled(2),
            Settlement::Rejected("nope".to_string())
        ]
    );
}

#[test]
fn spec_spread_destructures_and_forwards_rejection() {
    let (tx, rx) = channel();
    resolve(vec![1, 2, 3])
        .spread(|v| v[0] + v[1] + v[2])
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), 6);
    let (tx2, rx2) = channel();
    all(vec![resolve(10), reject::<i32>("spread-err")]).then_full(
        move |v| format!("{v:?}"),
        move |e| {
            tx2.send(e).unwrap();
            String::new()
        },
    );
    assert_eq!(recv(&rx2), "spread-err".to_string());
}

#[test]
fn spec_tap_forwards_value_and_rejection() {
    let (tx, rx) = channel();
    resolve("foo")
        .tap(|v| assert_eq!(*v, "foo"))
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), "foo");
    let (tx2, rx2) = channel();
    reject::<i32>("tap-err")
        .tap(|_| panic!("tap must not run on rejection"))
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx2.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(&rx2), "tap-err".to_string());
}

#[test]
fn spec_done_string_reason_reaches_hook() {
    let (tx, rx) = channel();
    set_onerror(Some(Arc::new(move |e: String| tx.send(e).unwrap())));
    reject::<i32>("this is not an error, but it might show up").done();
    assert_eq!(
        recv(&rx),
        "this is not an error, but it might show up".to_string()
    );
    set_onerror(None);
}

#[test]
fn spec_done_empty_reason_reaches_hook_verbatim() {
    let (tx, rx) = channel();
    set_onerror(Some(Arc::new(move |e: String| tx.send(e).unwrap())));
    reject::<i32>("").done();
    assert_eq!(recv(&rx), String::new());
    set_onerror(None);
}

#[test]
fn spec_timeout_fast_fulfill_and_fast_reject_pass_through() {
    let (tx, rx) = channel();
    later(10)
        .timeout(2000, None)
        .then(move |_| tx.send(1).unwrap());
    assert_eq!(recv(&rx), 1);
    let (tx2, rx2) = channel();
    later(10)
        .and_then(|_| reject::<i32>("haha"))
        .timeout(2000, None)
        .then_full(
            move |v| v.to_string(),
            move |e| {
                tx2.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(&rx2), "haha".to_string());
}

#[test]
fn spec_timeout_slow_rejects_with_time_message() {
    let (tx, rx) = channel();
    later(500).timeout(10, None).then_full(
        move |_| "fulfilled".to_string(),
        move |e| {
            tx.send(e).unwrap();
            String::new()
        },
    );
    let err = recv(&rx);
    assert!(err.contains("Timed out after 10 ms"), "{err}");
}

#[test]
fn spec_timeout_custom_message_passes_through() {
    let (tx, rx) = channel();
    later(300)
        .timeout(10, Some("custom".to_string()))
        .then_full(
            move |_| "fulfilled".to_string(),
            move |e| {
                tx.send(e).unwrap();
                String::new()
            },
        );
    assert_eq!(recv(&rx), "custom".to_string());
}

#[test]
fn spec_delay_holds_fulfillment_releases_rejection_fast() {
    let start = Instant::now();
    let p = resolve(5).delay(100);
    std::thread::sleep(Duration::from_millis(40));
    assert!(p.is_pending());
    let (tx, rx) = channel();
    p.then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), 5);
    assert!(start.elapsed() >= Duration::from_millis(100));
    let pr = reject::<i32>("fast-err").delay(100);
    std::thread::sleep(Duration::from_millis(30));
    assert!(pr.is_rejected());
}

#[test]
fn spec_delay_value_and_delay_after_resolution() {
    let (tx, rx) = channel();
    delay_value("what", 30).then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), "what");
    let p1 = delay_value("what", 30);
    let p2 = p1.delay(30);
    std::thread::sleep(Duration::from_millis(40));
    assert!(p2.is_pending());
    let (tx2, rx2) = channel();
    p2.then(move |v| tx2.send(v).unwrap());
    assert_eq!(recv(&rx2), "what");
}

#[test]
fn spec_then_resolve_then_reject_analogues() {
    let (tx, rx) = channel();
    later(20).then(|_| "foo").then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), "foo");
    let (tx2, rx2) = channel();
    reject::<&str>("boo").then(|_: &str| "foo").then_full(
        move |v| v.to_string(),
        move |e| {
            tx2.send(e).unwrap();
            String::new()
        },
    );
    assert_eq!(recv(&rx2), "boo".to_string());
    let (tx3, rx3) = channel();
    later(5)
        .then_full(|_| "unreached".to_string(), |_| "foo".to_string())
        .then(move |v| tx3.send(v).unwrap());
    assert_eq!(recv(&rx3), "unreached".to_string());
}

#[test]
fn spec_assimilation_chain_collapses() {
    let (tx, rx) = channel();
    resolve(1)
        .and_then(|v| resolve(v + 1))
        .and_then(|v| resolve(v * 10))
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), 20);
}

#[test]
fn spec_nodeify_without_callback_stays_usable() {
    let (tx, rx) = channel();
    resolve(3)
        .nodeify(None::<fn(Result<i32, String>)>)
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), 3);
}

#[test]
fn spec_denodeify_wrapper_called_twice() {
    fn double(arg: i32, cb: Box<dyn FnOnce(Result<i32, String>) + Send>) {
        cb(Ok(arg * 2));
    }
    let f = denodeify(double);
    let (tx, rx) = channel();
    f(21).then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), 42);
    let (tx2, rx2) = channel();
    f(10).then(move |v| tx2.send(v).unwrap());
    assert_eq!(recv(&rx2), 20);
}

#[test]
fn spec_two_observers_each_fire_exactly_once() {
    let (d, p) = defer::<i32>();
    let (tx, rx) = channel();
    let t1 = tx.clone();
    p.clone().then(move |_| t1.send("a").unwrap());
    p.then(move |_| tx.send("b").unwrap());
    d.resolve(1);
    let mut got = vec![recv(&rx), recv(&rx)];
    got.sort_unstable();
    assert_eq!(got, vec!["a", "b"]);
}

#[test]
fn spec_fin_async_delay_case() {
    let start = Instant::now();
    let (tx, rx) = channel();
    resolve("foo")
        .fin_async(|| later(250))
        .then(move |v| tx.send(v).unwrap());
    assert_eq!(recv(&rx), "foo");
    assert!(start.elapsed() >= Duration::from_millis(250));
}
