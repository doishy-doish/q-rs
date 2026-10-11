# q-rs — Rust port of Q (kriskowal/q)

Q v1.5.1 was archived Nov 2023 with 15k stars. This port mirrors its API
in Rust with parity tests. Original: https://github.com/kriskowal/q (MIT,
Copyright 2009-2018 Kristopher Michael Kowal — see LICENSE).

Shared conventions (all leaves): public API names mirror Q; `String` is the
rejection reason; v1 scheduling runs each settled callback batch on a spawned
thread in attach order (Q's own asap queue is NOT mimicked — documented).

Leaves: core (defer/resolve/reject/then/catch/when) -> combinators
(all/spread/timeout/delay/node wrappers) -> compliance (spec port + A+ vectors).
