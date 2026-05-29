# Proposal: Replay Engine & Dispatcher Performance Optimizations

**Date:** 2026-05-28
**Scope:** `src/runtime/replay_engine.rs`, `src/runtime/dispatchers/orchestration.rs`,
`src/runtime/state_helpers.rs`, `src/runtime/mod.rs`, and the provider fetch path in
`src/providers/sqlite.rs`.
**Goal:** Reduce per-turn CPU/allocation cost in the orchestration execution hot path,
eliminating the `O(M·H)` and `O(H²)` scaling patterns that hurt long-lived instances.

---

## TL;DR

Every orchestration turn re-reads the full execution history, clones it multiple times,
and replays the orchestration from `event[0]`. Several inner loops scan the entire history
per message, producing `O(M·H)` work for a batch of `M` messages against a history of length
`H`, and `O(H²)` total work over an instance's lifetime.

This proposal groups fixes into three tiers:

- **Tier 1 — Local hot-path fixes** (no contract/behavior change, covered by existing replay
  tests): index history once instead of scanning per-message, stop redundant clones, cache
  `next_event_id`, and skip cancellation reconciliation when nothing was cancelled.
- **Tier 2 — Structural** (provider default-impl additions): avoid loading full history for
  terminal acks; single-pass metadata extraction.
- **Tier 3 — Deep design** (avoid full replay per turn): tracked separately in
  [orchestration-instance-caching.md](orchestration-instance-caching.md). This proposal
  references it rather than duplicating it.

---

## Background: the per-turn hot path

A single orchestration turn currently does the following:

1. **Provider fetch** — `fetch_orchestration_item` reads and deserializes the *entire*
   execution history from the DB ([sqlite.rs `read_history_in_tx`](../../src/providers/sqlite.rs)).
2. **Metadata extraction** — `HistoryManager::from_history` clones the whole history
   ([state_helpers.rs](../../src/runtime/state_helpers.rs)). A second `temp_history_mgr`
   is also built in `process_orchestration_item`.
3. **Completion prep** — `prep_completions` converts incoming messages to events,
   scanning baseline + delta *per message* for dedup and nondeterminism checks
   ([replay_engine.rs](../../src/runtime/replay_engine.rs)).
4. **Replay** — `execute_orchestration` clones `baseline_history`, extends it with the
   delta, and replays the orchestration function from the first event.
5. **Cancellation reconciliation** — `collect_cancelled_from_context` builds ~8 `HashSet`s
   by scanning persisted history, *even when no future was dropped*.

For history length `H` and a batch of `M` messages, step 3 is `O(M·H)` and steps 2/4/5 are
`O(H)` each. Over the full lifetime of a long-lived instance (e.g. the instance-actor
pattern), the cumulative cost is `O(H²)`.

---

## Severity legend

| Severity | Meaning |
|----------|---------|
| 🔴 High | Dominant scaling cost (`O(M·H)` / `O(H²)`); biggest wins. |
| 🟡 Medium | Per-turn constant-factor cost (redundant clones/scans). |
| 🔵 Low | Polish; small constant savings. |

---

## Tier 1 — Local hot-path fixes

These are pure local optimizations with no contract or behavior changes. They are fully
covered by the existing replay determinism test suite.

### 1.1 🔴 `prep_completions` is `O(M·H)` — index the history once

In `prep_completions`, for **each** incoming message the engine performs multiple full scans:

- `is_completion_already_in_history` → full scan of `baseline_history`.
- The `already_in_delta` check → full scan of `history_delta`.
- The `schedule_kind` closure → full scan of `baseline_history.chain(history_delta)`.
- (feature `replay-version-test`) `ExternalRaised2` subscription check → another full scan.

For a batch of `M` messages this is `O(M·H)`.

**Fix.** Build lookup structures once before the loop:

- `HashMap<u64, ScheduleKind>` mapping `event_id → schedule kind` for all
  `ActivityScheduled` / `TimerCreated` / `SubOrchestrationScheduled` events in baseline.
- `HashSet<u64>` of `source_event_id`s already completed in baseline (keyed/checked by kind).
- Track delta additions incrementally as events are pushed, rather than re-scanning the delta.

This turns each per-message check from `O(H)` into `O(1)`, making the function `O(M + H)`.

**Risk:** Low. The matching/dedup semantics are unchanged; only the data structure used to
answer "is this id already present / what kind is it" changes.

### 1.2 🟡 `HistoryManager::from_history` clones the full history (twice)

`from_history` does `history.to_vec()`. In `process_orchestration_item`, a `temp_history_mgr`
is built from `&item.history`, and then in the non-CAN path the same history is used again —
two full clones before replay even begins.

**Fix.**
- Add `from_history_owned(Vec<Event>)` that *moves* the vec instead of cloning.
- Restructure `process_orchestration_item` to build a single `HistoryManager`, taking
  ownership of `item.history` where possible.

**Risk:** Low–medium. Requires care around the CAN branch (which starts from empty history)
and the terminal-ack early return, but both can move the vec rather than clone it.

### 1.3 🟡 `HistoryManager::next_event_id` is an `O(H)` scan per call

`next_event_id` computes `iter().chain(delta).map(event_id).max() + 1` on every call, and it
is invoked repeatedly (e.g. in `validate_limits` / `fail_orchestration_for_limit`).

**Fix.** Cache `next_event_id` as a field, initialized once and bumped on each `append`.
Event IDs are monotonic, so this is just "last appended + 1" — the `ReplayEngine` already
tracks it this way; mirror that in `HistoryManager`.

**Risk:** Low.

### 1.4 🔴 `collect_cancelled_from_context` always builds 8 HashSets

`collect_cancelled_from_context` scans persisted history four times for `*CancelRequested`
breadcrumbs and four times for schedules — **even when the context reported zero
cancellations**, which is the overwhelmingly common case.

**Fix.** Early-out when all four `ctx.get_cancelled_*` vectors are empty *and* there are no
persisted `dropped_future` breadcrumbs to enforce against. Replace the unconditional 8-set
construction with:

1. A cheap check: are there any cancellations this turn? If not, do a single scan to confirm
   there are no persisted `dropped_future` breadcrumbs that would now be missing, then return.
2. Only build the per-type sets when there is something to reconcile.

**Risk:** Low, but must preserve the "removed drop" detection (case A in the function's
docs): when persisted history contains a `dropped_future` breadcrumb but the code no longer
drops, we must still flag nondeterminism. The single-scan guard above covers this.

### 1.5 🟡 `execute_orchestration` double-allocates working history

`execute_orchestration` builds `working_history = baseline.clone(); working_history.extend(delta)`,
and `final_history()` clones again later.

**Fix.** Iterate `baseline_history.iter().chain(history_delta.iter())` directly. The empty/
terminal/`OrchestrationStarted`-first checks and the main `for (event_index, event)` loop all
work over a chained iterator with an `enumerate()` index, removing one full `O(H)` allocation
per turn.

**Risk:** Low. Indexing semantics (`event_index >= replay_boundary`) are preserved by
`enumerate()` over the chain.

### 1.6 🔵 `validate_limits` recomputes the KV key set from scratch

The KV-key-count block clones every snapshot key into a `HashSet` then replays the delta,
even when the delta contains no KV mutations.

**Fix.** Pre-check the (small) delta for any `KeyValueSet` / `KeyValueCleared` /
`KeyValuesCleared` event; skip the whole key-count computation when there are none.

**Risk:** Low.

---

## Tier 2 — Structural improvements (provider default-impl)

### 2.1 🟡 Avoid loading full history for terminal acks

`fetch_orchestration_item` unconditionally deserializes the entire history. For terminal
instances, `process_orchestration_item` immediately acks the batch and discards the history
(the `is_completed || is_failed || (is_continued_as_new && !is_can_start)` branch). The DB
read + deserialize is wasted there.

**Fix (incremental, backward-compatible).** Expose a lightweight terminal indicator from the
provider (derived from the stored instance/execution status) so the runtime can short-circuit
terminal acks without materializing history. Gate behind a `Provider` trait default-impl so
existing providers remain compatible.

**Risk:** Medium — touches the provider contract. Mitigated by a default implementation that
falls back to the current behavior.

### 2.2 🔵 Single-pass metadata extraction

`HistoryManager::from_history`, `Runtime::compute_execution_metadata`, and the dispatcher's
inline `pinned_version` extraction each independently scan for `OrchestrationStarted`.

**Fix.** Consolidate into a single pass (produced once by `HistoryManager` or the provider)
and reuse the result. Constant-factor win, improves clarity.

**Risk:** Low.

---

## Tier 3 — Deep design: avoid full replay per turn

The dominant `O(H²)` cost is that **every** message delivery re-runs the orchestration from
`event[0]`, re-deriving `open_schedules`, token bindings, and KV state that the previous turn
already computed.

This is tracked in detail in
[orchestration-instance-caching.md](orchestration-instance-caching.md), which proposes keeping
hydrated instance state warm (LRU cache keyed by `(instance, execution_id, history_len)`) and
applying only new completion events incrementally. Instance-level locking already serializes
turns per instance, so a cache validated against persisted `history_len` is safe by
construction: a mismatch (another node advanced the instance, or `continue_as_new` reset the
history) falls back to full replay.

A natural extension is an incremental history fetch
(`read_history_since(instance, execution_id, after_event_id)`) so warm instances avoid the DB
`O(H)` read as well. See that proposal for the full design, lifecycle, and correctness
analysis.

---

## Recommended sequencing

1. **Tier 1 first.** 1.1, 1.4, and 1.5 deliver the biggest wins for the least risk; 1.2, 1.3,
   and 1.6 are easy follow-ons. All are covered by existing replay tests — validate with
   `cargo nt` and `./run-tests.sh`.
2. **Tier 2.1** next (real DB savings for terminal/idle churn), behind a provider default-impl.
3. **Tier 3** (instance caching) as the headline architectural improvement — see
   [orchestration-instance-caching.md](orchestration-instance-caching.md).

---

## Expected impact

| Change | Before | After |
|--------|--------|-------|
| `prep_completions` (1.1) | `O(M·H)` per batch | `O(M + H)` |
| History clones (1.2, 1.5) | 2–3 full `O(H)` clones/turn | 0–1 |
| `next_event_id` (1.3) | `O(H)` per call | `O(1)` |
| Cancellation reconcile (1.4) | 8 `HashSet` builds/turn | skipped when no drops |
| Terminal acks (2.1) | full history read/deserialize | metadata-only |
| Steady-state turn (Tier 3) | `O(H)` replay | `O(new events)` |
| Instance lifetime (Tier 3) | `O(H²)` | `O(H)` |

---

## Testing & validation

- **Correctness:** Tier 1 changes are behavior-preserving and validated by the existing
  replay determinism suite (`src/runtime/replay_engine_tests.rs`,
  `tests/scenarios/`). Run `./run-tests.sh` (two-pass: with and without `--all-features`)
  before committing.
- **Determinism:** Pay special attention to 1.4 — keep a regression test that asserts the
  "removed drop" nondeterminism detection still fires.
- **Benchmarking:** Use `sqlite-stress/` and the instance-actor scenario (long histories) to
  measure per-turn CPU and total lifetime cost before/after.
