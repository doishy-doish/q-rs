# q-rs — Rust port of kriskowal/q

Faithful Rust port of the [Q promise library](https://github.com/kriskowal/q) (archived, MIT, 15k stars).
Same API names where Rust's type system allows; every documented difference below is deliberate, not drift.

## Layout

- `src/lib.rs` — Leaf 1 core: `defer` / `resolve` / `reject` / `then` / `catch` / `when`.
  Settlement is first-wins; callbacks run on spawned threads (documents, not mimics, Q's `asap` queue).
- `src/combinators.rs` — Leaf 2: `all` (order-preserving, first-reject wins), `any`
  (first-fulfill wins, Q-exact all-rejected message), `all_settled` via `all`+`then_full`,
  `spread`, `then_full`, `fin`, `tap`, `nodeify`, `timeout`, `delay`, `fcall` (panic→rejection),
  `fbind`, `denodeify` + `nfbind`/`nfcall` aliases.
- `src/ext.rs` — Leaf 3 spec-gap API: `resolve_promise` assimilation, `and_then` chains,
  `inspect` / `PromiseState` / `is_*`, `done` + `set_onerror` hook, `fin_async` / `tap_async`
  (panics become rejections), `later` / `delay_value`.

## Tests — 63, all green on CI (fmt + clippy `-D warnings` + test)

- `tests/core.rs` (5): chains, rejection-skips-then, catch recovery, cross-thread defer, `when`.
- `tests/combinators.rs` (13): ordering, first-error/first-win, mixed settlement, timeout, delay, node-style wrap, panic→rejection, fin/finally, spread.
- `tests/ext.rs` (16): assimilation (fulfilled/rejected/pending), `and_then` chains, inspect states,
  `done`→hook routing, async fin/tap incl. replacement-rejection, delay helpers.
- `tests/compliance.rs` (29): ported from Q's `q-spec.js` (197 cases) — propagation, all/any/allSettled,
  spread, tap, done (gh-73/gh-90 analogues), timeout, delay, thenResolve/thenReject analogues,
  assimilation chains, A+ 2.1/2.2 vectors (state irreversibility, attach-order callbacks, async delivery).

## Deliberate divergences from Q

1. Rejections are `String`, not `Error` objects (no `.stack`, no `.code` field; timeout custom errors are message-only).
2. `any([])` rejects (Q fulfills with `undefined` — unrepresentable without a value for `T`).
3. No `progress`/`notify` (out of core scope).
4. No foreign-thenable assimilation (no dynamic thenables in Rust); `resolve_promise`/`and_then` cover the native analogue.
5. No `get`/`set`/`del`/`post`/`send`/`keys`, `nbind`/`npost`/`ninvoke`, `makeNodeResolver` (JS object-model dispatch).
6. No unhandled-rejection tracker / long stack traces / domains (use `done()` + `set_onerror`).
7. `fin`/`tap` sync observers must not panic (thread panic, not rejection); use `fin_async`/`tap_async` for fallible observers.
8. Callbacks on already-settled promises run on a new thread each — registration order is guaranteed only for callbacks attached while pending.

## License

MIT — original library copyright Krish Kowalski; see LICENSE.

## Benchmarks

JS original (q@1.5.1, this browser): 20,000 resolve+then in 1228.9ms (~61 us/op). Rust port (playground release): 20,000 in 1538.5ms (~77 us/op, 0.8x). Honest loss on trivial chains: threads + channels cost more than a JS promise hop. q-rs wins on real multi-core fan-out, not micro-chains.
