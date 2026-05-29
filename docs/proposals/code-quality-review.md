# Duroxide Code Quality & Idiomatic-Rust Review

**Date:** 2026-05-28
**Scope:** Full source review of `src/` (production code + a lighter pass over the `provider_validation/` and `provider_stress_test/` suites).
**Goal:** Honest, guru-level assessment against idiomatic-Rust and software-quality standards, with concrete, actionable recommendations.

---

## TL;DR

Duroxide is **well-engineered, professional Rust**. The architecture is clean (two-queue runtime, provider-as-pure-storage, deterministic replay), the determinism discipline is genuinely excellent, error types are structured (`ProviderError`, `ErrorDetails`, `ClientError`), and documentation is exceptional. It is **not yet** "the most idiomatic Rust ever written" — there is a consistent set of recurring issues that a senior reviewer would flag:

1. **Panics on caller-provided data** in a few library entry points (most notably typed client event APIs).
2. **String-matching for error classification** in several places, instead of using the structured error kinds that already exist.
3. **Doc duplication and stale doc references** in the `Provider` trait (large, verbatim-duplicated sections; references to fields that don't exist).
4. **Oversized functions** in the orchestration dispatcher and execution paths.
5. **Code duplication** in the combinators (`PollAllJoin{,2,3}`, `Select2/3`) and client polling loops.
6. **API inconsistencies** (`&str` vs `impl Into<String>`; `OrchestrationContext` getters return owned `String` while `ActivityContext` returns `&str`).

None of these are correctness-critical for the happy path (the test suite is broad and passing), but addressing them would materially raise the codebase from "very good" to "exemplary."

> **Note on review method:** Findings were cross-checked against the source. Several initially-flagged "critical" items turned out to be **false positives** and were removed or corrected — see [Appendix: Rejected findings](#appendix-rejected-findings). Remaining findings were verified against the actual code.

---

## Severity legend

| Severity | Meaning |
|----------|---------|
| 🔴 High | Real defect or footgun; should be fixed. May panic, mislead, or mask failures. |
| 🟡 Medium | Maintainability / idiomatic / API-design issue worth scheduling. |
| 🔵 Low | Polish; nice-to-have. |

---

## 1. Cross-cutting themes

These patterns recur across multiple files and are worth a single, consistent decision rather than ad-hoc fixes.

### 1.1 🔴 Panic on caller-provided data in typed APIs

Typed client event methods panic if serialization of a caller's value fails, while the equivalent `start_orchestration` path correctly returns an error.

- [src/client/mod.rs](../../src/client/mod.rs#L405) — `raise_event_typed`: `Json::encode(data).expect("Serialization should not fail")`
- [src/client/mod.rs](../../src/client/mod.rs#L450) — `enqueue_event_typed`: same pattern
- Contrast with the correct pattern at [src/client/mod.rs](../../src/client/mod.rs#L296) and [src/client/mod.rs](../../src/client/mod.rs#L315), which use `.map_err(|e| ClientError::InvalidInput { .. })?`.

**Why it matters:** `serde` serialization *can* fail for caller types (e.g. a map with non-string keys, a custom `Serialize` that errors, non-finite floats under some configs). A library should not abort the process on user input.

**Recommendation:** Make `raise_event_typed`/`enqueue_event_typed` return the encode error as `ClientError::InvalidInput`, mirroring `start_orchestration`.

### 1.2 🟡 Error classification by substring matching

Several places infer error semantics from message text rather than from structured fields, even though structured error kinds already exist (`ProviderError`, `ErrorDetails`).

- [src/client/mod.rs](../../src/client/mod.rs#L1044) — `translate_delete_error` matches `msg.contains("not found")`, `"still running"`, etc.
- [src/providers/instrumented.rs](../../src/providers/instrumented.rs) — error-type metric label derived from `error.message.contains("deadlock"|"timeout"|...)`.
- [src/providers/sqlite.rs](../../src/providers/sqlite.rs#L42) — `sqlx_to_provider_error` classifies retryable via `contains("database is locked") || contains("SQLITE_BUSY")`.

**Why it matters:** Message text is not a stable API. It changes across SQLite/sqlx versions, is locale-sensitive, and silently mis-classifies when it drifts.

**Recommendation:**
- For sqlite: match on `sqlx::Error` variants and `SqliteError::code()` (e.g. extended result codes `SQLITE_BUSY`/`SQLITE_LOCKED`) instead of message substrings.
- For the client/delete path: add a `kind: ProviderErrorKind` discriminant to `ProviderError` (e.g. `NotFound`, `StillRunning`, `SubOrchestration`, `Conflict`) and match on it. This removes the brittle round-trip through `Display`.

### 1.3 🟡 `ProviderError` drops the error source chain

[src/providers/error.rs](../../src/providers/error.rs) stores `operation`, `message`, `retryable` but no `source`. Converting `sqlx::Error` to a string loses the underlying chain, and `std::error::Error::source()` returns `None`.

**Recommendation:** Add `source: Option<Box<dyn std::error::Error + Send + Sync>>` and implement `Error::source()`. Keep the flattened `message` for display, but preserve the cause for debugging. Reconsider the blanket `From<String>`/`From<&str>` conversions (they default `operation` to `"unknown"` and `retryable` to a fixed value, silently losing intent).

### 1.4 🟡 `#[must_use]` missing on fallible public APIs

Public `async fn`s returning `Result<_, ClientError>` / `Result<_, ProviderError>` are not annotated `#[must_use]`, so `client.cancel_instance(..).await;` compiles with no warning.

**Recommendation:** Add `#[must_use = "this returns a Result that should be handled"]` on the fallible public client methods (and trait methods where appropriate).

### 1.5 🟡 `&str` vs `impl Into<String>` inconsistency

The client mixes parameter conventions for the same logical argument (instance/orchestration name): some take `impl Into<String>` (e.g. `start_orchestration`, `cancel_instance`), others `&str` (e.g. `get_orchestration_status`, `wait_for_orchestration`). See [src/client/mod.rs](../../src/client/mod.rs).

**Recommendation:** Pick one convention. Prefer `&str` for read-only paths and reserve `impl Into<String>` for paths that store the value. Document the rule once.

---

## 2. `src/providers/` (excluding `mod.rs`, reviewed separately)

### `sqlite.rs` (≈4,560 lines)

- 🟡 **`expect` in the history hot path.** [sqlite.rs](../../src/providers/sqlite.rs#L685): `serde_json::to_string(&event).expect("Event serialization should never fail")`. Realistically safe, but it's in the durable-write path; prefer mapping to a `ProviderError` so a future non-serializable `EventKind` field can't panic a worker. The accompanying `event_type` discriminant `match` is also a manual mirror of `EventKind` that must be hand-maintained (easy to forget a variant — the `#[cfg(feature = "replay-version-test")]` arms already show the fragility).
- 🟡 **Rollback errors swallowed.** Multiple `tx.rollback().await.ok();` sites (e.g. around [sqlite.rs](../../src/providers/sqlite.rs#L819)). A failed rollback indicates a real storage problem and should at least be logged at `warn`.
- 🟡 **`new(database_url, _options)` ignores its options.** [sqlite.rs](../../src/providers/sqlite.rs#L140) accepts `SqliteOptions` but binds it to `_options`. Either implement it (pool size, timeouts, pragmas) or remove the parameter — accepting-and-ignoring config violates least surprise.
- 🟡 **Best-effort session updates silently ignored.** `…execute(&self.pool).await.ok();` for `last_activity_at` piggyback updates. Log on failure so a deleted session row doesn't silently stop activity tracking.
- 🔵 **Magic numbers in PRAGMAs.** `wal_autocheckpoint = 10000`, `cache_size = -64000`, `busy_timeout = 60000` — promote to named constants with units in the name.
- 🔵 **Style nit:** [sqlite.rs](../../src/providers/sqlite.rs#L1887) `let next_item = next_item.unwrap();` immediately follows an `is_none()` early-return. It is **safe** (no `await` between), but `let Some(next_item) = next_item else { return Ok(None) }` is more idiomatic and panic-free.
- 🔵 **Error-mapping boilerplate.** `.map_err(|e| Self::sqlx_to_provider_error("op_name", e))?` repeats ~200×. A small helper (extension trait `IntoProviderResult` with the op name) would DRY this and centralize classification.

### `error.rs`

- See [1.3](#13-providererror-drops-the-error-source-chain). Otherwise clean and idiomatic.

### `instrumented.rs`

- 🟡 **Inconsistent instrumentation coverage.** Some forwarded methods (e.g. `abandon_orchestration_item`, `renew_orchestration_item_lock`, `read_with_execution`, `append_with_execution`) record no timing/error metrics while their peers do — creating observability blind spots in exactly the failure paths you most want to see. Make coverage uniform (a macro or a single `instrument(op, fut)` wrapper helps).
- See [1.2](#12-error-classification-by-substring-matching) for the substring-based error label.

### `management.rs`

- 🔵 Mostly trait + value types; clean. Minor: option/filter structs (`InstanceFilter`, `PruneOptions`) are the right call — keep favoring them over positional args as these grow.

---

## 3. `src/providers/mod.rs` (the `Provider` trait + value types)

Reviewed in detail previously; summarizing for completeness.

- 🟡 **Massive doc duplication.** Entire sections ("Design Principles", "Multi-Execution Support", "Concurrency Model", "Peek-Lock Pattern", "Required vs Optional Methods", "Testing Your Provider") appear **twice**, nearly verbatim, in the trait doc comment. They will drift. De-duplicate.
- 🟡 **Stale doc references.** Docs mention `metadata.create_next_execution`, `metadata.next_execution_id`, and a `DeleteResult` type that don't match the actual structs (`ExecutionMetadata` has no such fields; the type is `DeleteInstanceResult`). Fix or remove.
- 🟡 **Panicking constructors.** `TagFilter::tags`/`default_and` `assert!` on caller input (too many / empty). Consider `try_*` returning `Result`, or keep panics but document them as debug-time contract violations (they are currently `# Panics`-documented, which is at least honest).
- 🟡 **7-arg `ack_orchestration_item` + `#[allow(clippy::too_many_arguments)]`.** Bundle the turn outputs into a struct (`AckRequest { execution_id, history_delta, worker_items, orchestrator_items, metadata, cancelled_activities }`). This also future-proofs the signature against churn across all implementors.
- 🟡 **Naming drift for the same concept:** `read`/`read_with_execution` (on `Provider`) vs `read_history`/`read_history_with_execution_id` (on `ProviderAdmin`). Unify the vocabulary.
- 🔵 `Provider: Any` supertrait enables downcasting for `as_management_capability`. Works, but it's a reflection hatch; fine to keep, worth a comment explaining why.

---

## 4. `src/lib.rs` (core types, `OrchestrationContext`, `ActivityContext`)

- 🟡 **Getter return-type inconsistency.** `OrchestrationContext::instance_id()/orchestration_name()/orchestration_version()` return owned `String` (cloning out of the `Mutex`), while `ActivityContext::instance_id()/orchestration_name()/…` return `&str`. The asymmetry is forced by `Arc<Mutex<CtxInner>>`, but it's a surprising public-API inconsistency. Consider documenting the rationale, or returning `Arc<str>` for the rarely-changing identity fields to avoid per-call allocation.
- 🟡 **`OrchestrationContext` = `Arc<Mutex<CtxInner>>` with pervasive `.lock().unwrap()`.** This is an intentional, documented poison-panic policy (`#![allow(clippy::unwrap_used)]`). Acceptable, but: (a) the orchestration body is effectively single-task during a turn, so the `Mutex` is largely there to satisfy `Send`; (b) a tiny `fn locked(&self) -> MutexGuard<CtxInner>` helper would centralize the unwrap and make the intent explicit at every call site rather than relying on a crate-level allow.
- 🟡 **Hand-maintained `EventKind` ⇄ string mirrors.** The discriminant string mapping (in `sqlite.rs`) and similar `match`es over `EventKind` must be kept in lockstep with the enum. A `#[derive(strum::IntoStaticStr)]` (or a single `impl EventKind { fn type_name(&self) -> &'static str }` in `lib.rs`) would make this a single source of truth.
- 🔵 `CtxInner::new` takes two `_`-prefixed "kept for API compatibility, no longer used" parameters (`_history`, `_worker_id`). Dead parameters on an internal constructor; remove them and update call sites.
- ✅ **Strengths:** `Either2/3`, `ScheduleKind`, `ErrorDetails` (with `category()`/`is_retryable()`), and `RetryPolicy`/`BackoffStrategy` builders are clean and idiomatic. Determinism guarantees (`new_guid`, `utc_now`, `ContinueAsNewFuture` that is permanently `Pending`) are elegant.

---

## 5. `src/runtime/`

### `dispatchers/orchestration.rs` (≈1,400 lines)

- 🔴 **`process_orchestration_item` is too large and does too much.** It interleaves poison-message checks, history-deserialization-error handling, terminal-state detection, handler resolution, execution, metadata computation, limit validation, metrics, cancellation tracking, and lock renew/ack. Single-responsibility is lost; unit-testing one concern requires standing up all state.
  **Recommendation:** Extract `validate_not_terminal`, `resolve_handler`, `execute_step`, `compute_and_validate_metadata`, `finalize_and_ack`.
- 🟡 **Duplicated failure path in `fail_orchestration_as_poison`.** The corrupted-history branch and the normal branch both build an error event, append, and ack with near-identical logic. Extract a shared `fail_with_error(...)`.
- 🟡 **Lock-renewal task gives up on first error.** The renewal loop `break`s on any `Err`. Correct when the lock was legitimately lost (instance terminated), but a transient storage blip also ends renewal. Distinguish `is_retryable()` and back off on transient errors; only break on permanent ones.

### `execution.rs` (≈1,125 lines)

- 🟡 **`run_single_execution_atomic` has ~9 parameters** (plus a `&mut HistoryManager`). Bundle the stable inputs into an `ExecutionContext { instance, orchestration_name, orchestration_version, execution_id, worker_id, kv_snapshot }`.

### `replay_engine.rs` (≈1,934 lines)

- 🟡 **Per-turn full-history clone.** `let mut working_history = self.baseline_history.clone(); working_history.extend_from_slice(&self.history_delta);` allocates and copies the entire baseline each turn. For long-lived instances this is O(history) per turn. Prefer `baseline_history.iter().chain(history_delta.iter())` where a borrow suffices, or keep a single growing buffer. (`HistoryManager::full_history()` in `state_helpers.rs` has the same allocate-on-call shape and is called from multiple sites despite a doc note preferring an iterator form.)
- 🟡 **`prep_completions` does repeated linear scans of `history_delta`** (membership checks + per-message `schedule_kind`). Build a `HashSet<u64>` of delta event-ids once per batch for O(1) lookups.
- 🔵 **Nondeterminism error messages** could include orchestration name + execution_id + attempt for production triage. (Note: nondeterminism detection lives here in the `ReplayEngine`, not in `execution.rs`.)
- ✅ **Strength:** the determinism enforcement and completion-kind mismatch detection are rigorous and well-tested.

### `registry.rs`

- 🟡 **Non-monotonic version registration panics.** `register_versioned*` `panic!`s if a version isn't strictly greater than the latest. This is a setup-time *user* error and should be a returned `Result`, not a process abort. (If panicking is a deliberate "fail fast at boot" choice, document it as such.)
- 🔵 **Repeated `.lock().expect("poisoned")`** — same poison policy as elsewhere; a small helper would DRY it.
- 🔵 `debug_dump` clones+stringifies the whole registry on a lookup miss; cheap to make it lazier, but it only runs on the error path.

### `mod.rs` (runtime)

- 🟡 **`start_with_options` panics on a config invariant** (`session_idle_timeout <= worker_renewal_interval`). Prefer returning `Result` so embedders can surface a clean error instead of a panic.
- 🟡 **`RuntimeOptions` (20+ fields) has no builder.** A `RuntimeOptionsBuilder` (you already use the builder pattern for the registry) would make partial customization safe and readable.
- 🔵 **`current_execution_ids: Mutex<HashMap<String, u64>>`** has no eviction; verify it's bounded/cleared on instance completion to avoid slow growth in long-lived processes.

### `observability.rs`

- 🔵 **`active_orchestrations_atomic: AtomicI64`** for a value that should never be negative — `AtomicU64` better expresses intent (cast at the gauge boundary if the metrics API needs `i64`).
- 🔵 **`gauge_poll_interval: Duration`** unvalidated; guard against `Duration::ZERO` (busy-loop) at construction.

### `dispatchers/worker.rs`

- 🟡 **Session-capacity check-then-increment race** is handled correctly (check, take guard, re-check, abandon if over) but is subtle; the RAII `SessionGuard` is a nice pattern — add a one-line doc on the intended race resolution.
- 🔵 Hardcoded 5s shutdown ticker in the session manager delays shutdown responsiveness; make it a shorter constant or configurable.

### `limits.rs`

- 🔵 Pure constants; consider co-locating small `validate_*` helpers so every limit has one canonical enforcement site (several limits are currently checked ad hoc by callers).

---

## 6. `src/combinators.rs`

- 🟡 **Heavy duplication across arities.** `PollAllJoin`, `PollAllJoin2`, `PollAllJoin3`, `Select2`, `Select3` are near-identical hand-rolled futures. A `macro_rules!` generator (or const-generic array of pinned futures) would cut ~70% of the boilerplate and guarantee consistent poll semantics across arities.
- 🔵 **`.expect()` on internal invariants** ("future slot should be occupied", "completed future should have output"). These are genuine invariants, but `debug_assert!` + structured panic messages (including the index) would aid debugging if they ever fire.
- 🔵 **No doc comments** on these `pub(crate)` combinators; a sentence each on the deterministic poll order would help maintainers.
- ✅ **Strength:** correct deterministic semantics (fixed poll order, no `tokio::select!`) — exactly what replay requires.

---

## 7. `src/client/mod.rs`

- See [1.1](#11-panic-on-caller-provided-data-in-typed-apis), [1.2](#12-error-classification-by-substring-matching), [1.4](#14-must_use-missing-on-fallible-public-apis), [1.5](#15-str-vs-impl-intostring-inconsistency).
- 🟡 **Polling backoff duplicated 3×.** `wait_for_orchestration`, `wait_for_kv_value`, `wait_for_status_change` repeat the deadline + exponential-backoff loop. Extract `async fn poll_with_backoff<F, T>(deadline, f)`.
- 🟡 **`wait_for_orchestration` reads the full history every poll iteration** via `get_orchestration_status` → `store.read(instance)`. For large histories under a tight poll cadence this is O(history × polls). Consider a lightweight status read (last terminal event only) for the polling path. (`get_custom_status` already demonstrates the "lightweight change check" pattern.)
- 🟡 **`get_orchestration_status` swallows all `get_custom_status` errors to `(None, 0)`.** This conflates "no custom status" with "storage read failed." Only swallow not-found; propagate transient errors.
- 🔵 Deprecated `raise_event_persistent` still present — fine to keep for one release cycle; track removal.
- ✅ **Strength:** `ClientError` is a clean typed enum; capability discovery via `as_management_capability()` is elegant; method docs are excellent.

---

## 8. Test / validation / stress suites (lighter pass)

`provider_validation/*`, `provider_stress_test/*`, and the in-file `#[cfg(test)]` modules are broad and well-organized (capability filtering, sessions, KV store, poison messages, lock expiration, multi-execution, atomicity). Liberal `.unwrap()`/`.expect()` here is **idiomatic and appropriate** for test code. Two minor suggestions:

- 🔵 Some validation files exceed 1,500–2,400 lines (`kv_store.rs`, `sessions.rs`). Consider splitting by scenario for navigability.
- 🔵 The combinators lack direct unit tests for their internal invariants (poll-once, ordered outputs); add a few targeted tests if the macro refactor in §6 lands.

---

## 9. Prioritized action plan

**Tier 1 — correctness / footguns (do first):**
1. Replace `expect("Serialization should not fail")` in `raise_event_typed`/`enqueue_event_typed` with `ClientError::InvalidInput`. ([§1.1](#11-panic-on-caller-provided-data-in-typed-apis))
2. Replace SQLite retryable classification with `sqlx`/`SqliteError` code matching. ([§1.2](#12-error-classification-by-substring-matching))
3. Make `registry::register_versioned*` and `runtime::start_with_options` return `Result` instead of panicking on user/config errors. ([§5](#5-srcruntime))
4. Log (don't swallow) rollback and best-effort session-update failures in `sqlite.rs`. ([§2](#sqliters-4560-lines))

**Tier 2 — API & maintainability:**
5. Add `ProviderErrorKind` discriminant + `source` chain to `ProviderError`; replace client substring matching. ([§1.2](#12-error-classification-by-substring-matching), [§1.3](#13-providererror-drops-the-error-source-chain))
6. De-duplicate the `Provider` trait doc comment and fix stale field references. ([§3](#3-srcprovidersmodrs-the-provider-trait--value-types))
7. Bundle `ack_orchestration_item` args into a struct; remove the `too_many_arguments` allow. ([§3](#3-srcprovidersmodrs-the-provider-trait--value-types))
8. Extract `process_orchestration_item` into focused helpers. ([§5](#dispatchersorchestrationrs-1400-lines))
9. Add `#[must_use]` to fallible public client APIs; standardize `&str` vs `impl Into<String>`. ([§1.4](#14-must_use-missing-on-fallible-public-apis), [§1.5](#15-str-vs-impl-intostring-inconsistency))
10. Extract `poll_with_backoff` in the client; macro-ize the combinators. ([§7](#7-srcclientmodrs), [§6](#6-srccombinatorsrs))

**Tier 3 — performance & polish:**
11. Avoid per-turn full-history clones (`replay_engine.rs`, `state_helpers.rs`); use iterator chaining. ([§5](#replay_enginers-1934-lines))
12. Build delta-event-id `HashSet` once in `prep_completions`. ([§5](#replay_enginers-1934-lines))
13. Single source of truth for `EventKind` → string. ([§4](#4-srclibrs-core-types-orchestrationcontext-activitycontext))
14. Uniform metrics coverage in `instrumented.rs`; named constants for SQLite PRAGMAs; `AtomicU64` for the gauge. ([§2](#instrumentedrs), [§5](#observabilityrs))

---

## Appendix: Rejected findings

These were raised during review but are **not** valid; documented to prevent re-litigation:

- ❌ *"`MAX(current_execution_id, ?)` / `MAX(0, attempt_count - 1)` is invalid because SQLite has no `MAX()` in `UPDATE`."* — **False.** SQLite's `max(X, Y, …)` with two or more arguments is a **scalar** function and is valid anywhere an expression is allowed, including `UPDATE … SET`. See [sqlite.rs](../../src/providers/sqlite.rs#L1260) and [sqlite.rs](../../src/providers/sqlite.rs#L2137).
- ❌ *"`next_item.unwrap()` at sqlite.rs:1887 can race because of await points between the null-check and the unwrap."* — **False.** There is no `await` between `if next_item.is_none() { return … }` and `.unwrap()`. It's safe; only a style nit (prefer `let … else`).
- ❌ *"`format!`-built SQL is a SQL-injection vector."* — **Not a vulnerability.** The interpolated portions are `?`-placeholder counts and a fixed tag clause; all values are bound via `.bind(...)`. It's a readability/maintainability note, not a security issue. (Still worth a comment marking the SQL as internally-constructed.)
- ❌ *"Nondeterminism handling in `execution.rs` continues instead of returning (CRITICAL)."* — **Mislocated.** Nondeterminism detection lives in `replay_engine.rs`, not `execution.rs`; the real, lower-severity note is the error-message richness in §5.

---

*Reviewed against the source tree as of 2026-05-28. Findings that touch behavior should be validated with `cargo nt` (nextest) and `./run-tests.sh` before/after any change.*
