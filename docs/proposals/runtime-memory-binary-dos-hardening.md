# Proposal: Runtime Memory, Binary Size & DoS Hardening

**Date:** 2026-05-28
**Scope:** `src/lib.rs` (`Event`/`EventKind`), `src/runtime/limits.rs`,
`src/runtime/replay_engine.rs`, `src/runtime/dispatchers/orchestration.rs`,
`src/client/mod.rs`, `Cargo.toml`.
**Goal:** Reduce the in-memory footprint of history, shrink the dependency/binary
surface, and close unbounded-input vectors that let untrusted callers exhaust
memory or CPU.

> **Relationship to other proposals.** This proposal is the *resource-safety*
> companion to [replay-engine-perf-optimizations.md](replay-engine-perf-optimizations.md).
> That doc targets per-turn CPU/allocation scaling (`O(M·H)` / `O(H²)`); this doc
> targets (a) the per-event memory *layout*, (b) the *binary/dependency* surface,
> and (c) *input-driven* denial-of-service. Where they overlap (per-turn history
> clone, quadratic completion matching) this doc references the perf proposal
> rather than re-specifying the fix.

---

## TL;DR

The runtime is functionally solid but has three resource-safety gaps:

1. **Memory layout.** Every `Event` denormalizes `instance_id: String` and
   `duroxide_version: String`, so both are duplicated once per event in an
   instance's history. Payloads (`input`/`result`/`data`) are owned `String`s
   cloned at every WorkItem→Event→working-history hop.
2. **Binary size.** `tokio` is pulled with `features = ["full"]` and
   `tracing-subscriber` is a **mandatory** dependency (with `env-filter` → `regex`
   and `json`), even though a library should not force a subscriber on consumers.
3. **DoS via bad inputs.** Existing limits in
   [limits.rs](../../src/runtime/limits.rs) cover custom status, tags, and KV —
   but the **primary payload sizes**, **fan-out / history length**, and the
   **external-event mailbox depth** are all unbounded, and the limits that do
   exist are enforced *after* the turn has already allocated the oversized value.

---

## Severity legend

| Severity | Meaning |
|----------|---------|
| 🔴 High | Untrusted-input memory/CPU exhaustion, or dominant memory cost. |
| 🟡 Medium | Significant constant-factor memory/binary cost or hardening gap. |
| 🟢 Low | Minor / defense-in-depth. |

---

## 1. Memory Efficiency

### 1.1 🟡 Per-event denormalization (`instance_id`, `duroxide_version`)

[`Event`](../../src/lib.rs) carries two denormalized owned strings on **every**
event:

```rust
pub struct Event {
    pub event_id: u64,
    pub source_event_id: Option<u64>,
    pub instance_id: String,        // duplicated for every event in the instance
    pub execution_id: u64,
    pub timestamp_ms: u64,
    pub duroxide_version: String,   // e.g. "0.1.29" duplicated for every event
    #[serde(flatten)]
    pub kind: EventKind,
}
```

For an instance with `N` events, `instance_id` is allocated `N` times and
`duroxide_version` `N` times. A 36-char UUID instance ID plus a `String` header
(24 bytes) plus a heap allocation is ~60+ bytes/event of pure redundancy, before
the version string. For long-lived instances (instance-actor pattern, large
fan-in) this is the single biggest avoidable in-memory cost, and it is paid
again on every replay because the whole history is materialized.

**Proposal**

- Represent `instance_id` as `Arc<str>` internally so the entire history shares
  one allocation; cloning an event clones an `Arc` (refcount bump), not the bytes.
- Replace `duroxide_version: String` with a compact form — `Arc<str>`, a packed
  `u32` semver, or strip it from the in-memory struct and only materialize it at
  the serde boundary. Within one execution nearly all events share the same
  version.

**Compatibility note (rolling upgrades).** This touches the serde representation
boundary. The on-wire JSON must remain unchanged: serialize `Arc<str>` exactly as
the current `String` (serde does this transparently), and keep `instance_id` /
`duroxide_version` as the same JSON fields so mixed-version clusters and existing
persisted histories deserialize identically. No event variant is added or removed.

### 1.2 🔴 Full history deep-cloned every turn

[`run_turn`](../../src/runtime/replay_engine.rs) materializes the combined
history every turn:

```rust
let mut working_history = self.baseline_history.clone();   // deep clone of all events + payloads
working_history.extend_from_slice(&self.history_delta);    // second copy
```

This clones every payload string on every turn; combined with full replay, total
work over an instance's lifetime is `O(history_bytes × turns)`.

**Proposal:** iterate `baseline_history.iter().chain(history_delta.iter())` with
an index/`replay_boundary` instead of building `working_history`. The run loop
only needs `&Event` — it never mutates the combined vector.

> Already specified in detail as **Tier 1** of
> [replay-engine-perf-optimizations.md](replay-engine-perf-optimizations.md).
> Listed here because it is also the highest-leverage *memory* fix and it
> amplifies the DoS impact of §3.1/§3.2 (each oversized payload is re-cloned per
> turn). Implement once; it satisfies both proposals.

### 1.3 🟡 Redundant snapshots in the commit path

[`process_orchestration_item`](../../src/runtime/dispatchers/orchestration.rs)
does `history_delta_snapshot = history_mgr.delta().to_vec()`, then
`validate_limits` and `compute_execution_metadata` each re-scan it, and
`item.kv_snapshot.clone()` is passed by value. Prefer borrowing the delta slice
and the KV snapshot; clone only where an owned value must outlive the borrow.
(Overlaps with Tier 1/2 of the perf proposal.)

### 1.4 🟡 Payloads are `String` end-to-end

`input` / `result` / `data` flow `WorkItem → Event → working_history`
([`prep_completions`](../../src/runtime/replay_engine.rs)) with a clone at each
hop. For large payloads each completion is copied several times.

**Proposal:** move payloads behind `Arc<str>` (or `bytes::Bytes`) so hand-offs
are refcount bumps. Compounds with §1.1 and §1.2. Same serde-boundary
compatibility constraint as §1.1.

---

## 2. Binary Size

### 2.1 🟢 `tokio = { features = ["full"] }`

`full` pulls `net`, `process`, `signal`, `fs`, `io-std`, etc. The runtime uses
roughly `rt-multi-thread`, `rt`, `macros`, `time`, `sync`. Single-threaded / pgrx
embedders need even less.

**Proposal:** narrow the feature list to what is actually used (audit with
`cargo tree -e features`). Also improves compile time.

### 2.2 🟡 `tracing-subscriber` is a mandatory dependency

In `Cargo.toml` it is a non-optional dependency with `fmt + env-filter + json`.
`env-filter` pulls `regex` (large); `json` pulls extra serde machinery. A library
should expose the `tracing` *facade* (already a dep, tiny) for instrumentation
but **not force a subscriber implementation** on every consumer.

**Proposal:** feature-gate `tracing-subscriber` (and `init_logging`) behind an
`observability` / `logging` feature; keep `tracing` always-on. Consumers with
their own subscriber (and pgrx embedders) then drop `regex`/`env-filter`/`json`
entirely. Low risk: [`init_logging`](../../src/runtime/observability.rs) already
uses `try_init()`, so it never panics on double-init.

### 2.3 🟢 Already reasonable

`semver`, `metrics` (zero-cost facade unless a recorder is installed),
`async-trait`, `serde_json` are fine. `sqlx` / `libsqlite3-sys` are already
correctly optional behind the `sqlite` feature.

---

## 3. DoS via Bad Inputs

[limits.rs](../../src/runtime/limits.rs) covers `MAX_CUSTOM_STATUS_BYTES`,
`MAX_TAG_NAME_BYTES`, `MAX_KV_VALUE_BYTES`, `MAX_KV_KEYS`, `MAX_WORKER_TAGS`, and
`MAX_CARRY_FORWARD_EVENTS`. The gaps below are the vectors **not** yet covered.

### 3.1 🔴 No limit on payload sizes

[`start_orchestration`](../../src/client/mod.rs),
[`raise_event`](../../src/client/mod.rs), and `enqueue_event` accept
`input` / `data` of any size with zero validation. Activity `result` strings have
the same problem on the worker side. An untrusted caller submits a multi-GB
payload → it is persisted into history → then re-cloned every turn (§1.2) and
replayed forever.

**Proposal:** add and enforce

```rust
pub const MAX_INPUT_BYTES: usize           = /* e.g. */ 1 * 1024 * 1024;
pub const MAX_EVENT_DATA_BYTES: usize      = 1 * 1024 * 1024;
pub const MAX_ACTIVITY_RESULT_BYTES: usize = 1 * 1024 * 1024;
```

Enforce at the **client/provider enqueue boundary** (reject before persistence,
returning `ClientError::InvalidInput`) **and** in
[`prep_completions`](../../src/runtime/replay_engine.rs) for results arriving from
workers (fail the orchestration with an Infrastructure error, mirroring
`validate_limits`).

### 3.2 🔴 No cap on fan-out / actions-per-turn / history length

A single turn can emit unbounded actions (e.g. a loop scheduling 1M activities,
possibly driven by attacker-controlled input). This grows `worker_items`,
`history_delta`, and the persisted history without bound in one turn — memory
blow-up plus a poison-pill instance that is expensive to replay thereafter.

**Proposal:** add

```rust
pub const MAX_ACTIONS_PER_TURN: usize  = /* e.g. */ 10_000;
pub const MAX_HISTORY_EVENTS: usize    = /* e.g. */ 100_000;
```

- `MAX_ACTIONS_PER_TURN`: fail the turn (Infrastructure error) if exceeded.
- `MAX_HISTORY_EVENTS` per execution: fail the instance when exceeded, nudging
  users toward `continue_as_new`. This is the missing companion to the existing
  `MAX_CARRY_FORWARD_EVENTS`.

### 3.3 🟡 Unbounded external-event / queue mailbox

External events and queue messages are
[materialized unconditionally](../../src/runtime/replay_engine.rs) and persist
until consumed. An attacker can `raise_event` / `enqueue_event` at an instance
that never consumes them, growing history/queue without bound.

**Proposal:** `MAX_PENDING_EVENTS` (and/or `MAX_QUEUE_DEPTH`) per instance;
reject or drop-with-warning past the cap.

### 3.4 🟡 Limits enforced post-execution, not at the boundary

[`validate_limits`](../../src/runtime/dispatchers/orchestration.rs) runs *after*
the turn executes, so an oversized KV value / custom status is fully allocated and
already lives in `history_delta` before being rejected.

**Proposal:** also check at action-emission time
(`set_kv_value` / `set_custom_status`) and at client enqueue, so the oversized
allocation is never produced. Keep `validate_limits` as defense-in-depth.

### 3.5 🟡 Quadratic completion matching (CPU DoS)

[`prep_completions`](../../src/runtime/replay_engine.rs) does, *per message*,
several full scans of `baseline_history` + `history_delta`
(`is_completion_already_in_history`, the `schedule_kind` closure, the
`already_in_delta` check) → `O(messages × history)`. Large batch + large history
triggers super-linear CPU.

**Proposal:** pre-build one-pass lookups before the loop — a
`HashSet<u64>` of completed `source_event_id`s and a `HashMap<u64, ActionKind>`
of scheduled IDs — making each message `O(1)`.

> Specified as **Tier 1** of
> [replay-engine-perf-optimizations.md](replay-engine-perf-optimizations.md);
> repeated here only for its DoS relevance.

### 3.6 🟢 Provider deserialization size bound

Provider-side `serde_json` deserialization has serde's default 128-level
recursion guard (protects against stack-overflow nesting) but **no byte/size
limit**. Pairs with §3.1 — enforce a max serialized payload / history-row size at
the provider read boundary.

---

## Priority Summary

| Priority | Item | Type |
|----------|------|------|
| 🔴 High | §3.1 Payload size limits at boundary | DoS |
| 🔴 High | §3.2 Fan-out / history-length caps | DoS |
| 🔴 High | §1.2 Stop deep-cloning history per turn | Memory/CPU |
| 🟡 Medium | §1.1 `Arc<str>` for `instance_id` / version | Memory |
| 🟡 Medium | §1.4 `Arc<str>` payloads | Memory |
| 🟡 Medium | §3.5 De-quadratic completion matching | CPU DoS |
| 🟡 Medium | §3.3 Mailbox depth cap | DoS |
| 🟡 Medium | §3.4 Move limit checks to boundary | DoS hardening |
| 🟡 Medium | §2.2 Feature-gate `tracing-subscriber` | Binary size |
| 🟢 Low | §2.1 Trim tokio features | Binary size |
| 🟢 Low | §3.6 Provider deserialization size cap | DoS |

---

## Suggested Sequencing

1. **Limit constants + boundary enforcement (§3.1, §3.2).** New constants in
   `limits.rs`, checks in `client/mod.rs` and `prep_completions`. No wire-format
   change; pure addition. Highest safety payoff.
2. **Per-turn clone removal (§1.2) + de-quadratic matching (§3.5).** Coordinate
   with [replay-engine-perf-optimizations.md](replay-engine-perf-optimizations.md)
   so the work is done once. Covered by existing replay tests.
3. **Mailbox cap (§3.3) and boundary-time limit checks (§3.4).**
4. **Binary-size cuts (§2.1, §2.2).** Feature-gating and feature trimming;
   mechanical and independently shippable.
5. **`Arc<str>` migration (§1.1, §1.4).** Largest blast radius (touches the serde
   boundary); do last, behind careful rolling-upgrade compatibility tests.

## Testing

- **Limits:** add validators under `src/provider_validation/` and cases in
  `tests/sqlite_provider_validations.rs` for each new constant (at/over boundary).
- **Memory layout (§1.1/§1.4):** round-trip serde tests proving the on-wire JSON
  is byte-identical before/after the `Arc<str>` change; mixed-version replay test
  reading a pre-change persisted history.
- **DoS (§3.1–§3.3):** tests asserting oversized input is rejected at enqueue and
  that an over-fan-out / over-length turn fails the instance with the expected
  `ErrorDetails` category rather than OOM-ing.
- Run `./run-tests.sh` (two-pass, with and without `--all-features`) before
  committing, per repo conventions.
