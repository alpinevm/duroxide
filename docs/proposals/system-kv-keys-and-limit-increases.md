# Proposal: System KV Keys & Limit Increases

**Status:** Draft  
**Author:** AI-assisted  
**Date:** 2026-03-30

## Summary

Two related changes:

1. **Increase three existing limits** to give orchestrations more headroom.
2. **Introduce "system KV keys"** — a reserved keyspace under a well-known prefix where the runtime writes introspection stats (history length, history size, event queue depth, KV metrics) as a single JSON blob, readable by orchestration code but not writable.

---

## 1. Limit Increases

| Constant | Current | Proposed | File |
|---|---|---|---|
| `MAX_CARRY_FORWARD_EVENTS` | 20 | **100** | `src/runtime/limits.rs` |
| `MAX_KV_KEYS` | 100 | **150** | `src/runtime/limits.rs` |
| `MAX_KV_VALUE_BYTES` | 16 KiB | **64 KiB** | `src/runtime/limits.rs` |

### Rationale

- **Carry-forward (20 → 100):** Orchestrations using `continue_as_new` with fan-in queues regularly hit the 20-event cap, silently dropping messages. 100 is generous while still preventing unbounded growth.
- **KV keys (100 → 150):** The new system key will consume one slot, and orchestrations using KV for per-task state need more room. The new limit of 150 applies to **user keys only** — system keys are excluded from the count (see §2.4).
- **KV value size (16 KiB → 64 KiB):** The system stats blob is small, but users storing structured data (e.g., serialized tool results, LLM context) frequently need more than 16 KiB.

### Impact

- All three constants are enforced at the runtime level (`validate_limits()` in the orchestration dispatcher and `MAX_CARRY_FORWARD` in the `continue_as_new` path). No provider-level schema changes needed.
- Existing orchestrations are unaffected — limits are only loosened, never tightened.

---

## 2. System KV Keys

### 2.1 Reserved Prefix

```rust
/// Prefix for runtime-managed system KV keys.
/// Keys starting with this prefix cannot be written or cleared by orchestration code.
pub const SYSTEM_KV_PREFIX: &str = "__duroxide.";
```

This follows the existing `__duroxide_syscall:` convention for system activities.

### 2.2 Well-Known Key: `__duroxide.stats`

```rust
/// Well-known system KV key containing runtime introspection stats.
/// Updated by the runtime after each orchestration turn completes.
pub const SYSTEM_KV_STATS: &str = "__duroxide.stats";
```

### 2.3 Stats Blob Schema

The value is a JSON object:

```json
{
  "history_event_count": 847,
  "history_size_bytes": 52301,
  "queue_pending_count": 3,
  "kv_user_key_count": 42,
  "kv_total_value_bytes": 8192
}
```

| Field | Type | Description |
|---|---|---|
| `history_event_count` | `u64` | Total events in history for the current execution (persisted + delta) |
| `history_size_bytes` | `u64` | Approximate serialized size of the full history in bytes |
| `queue_pending_count` | `u64` | Number of unmatched queue arrivals (enqueued but not yet dequeued) |
| `kv_user_key_count` | `u64` | Number of user KV keys (excluding system keys) |
| `kv_total_value_bytes` | `u64` | Sum of all user KV value sizes in bytes |

A typed accessor struct will be provided:

```rust
/// Runtime introspection stats, available via [`OrchestrationContext::get_system_stats`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SystemStats {
    pub history_event_count: u64,
    pub history_size_bytes: u64,
    pub queue_pending_count: u64,
    pub kv_user_key_count: u64,
    pub kv_total_value_bytes: u64,
}
```

### 2.4 Enforcement: Reserved Keyspace

**Write protection:** `set_kv_value()`, `clear_kv_value()`, and `clear_all_kv_values()` will reject keys that start with `SYSTEM_KV_PREFIX`:

```rust
// In set_kv_value:
pub fn set_kv_value(&self, key: impl Into<String>, value: impl Into<String>) {
    let key: String = key.into();
    assert!(
        !key.starts_with(SYSTEM_KV_PREFIX),
        "Cannot write to system KV key '{}' — keys starting with '{}' are reserved",
        key, SYSTEM_KV_PREFIX,
    );
    // ... existing logic
}
```

Same guard in `clear_kv_value()`. `clear_all_kv_values()` will skip system keys.

**Read access:** `get_kv_value()`, `get_kv_all_values()`, `get_kv_all_keys()` continue to work normally — system keys are readable.

**Limit exclusion:** System keys are **excluded** from both the `MAX_KV_KEYS` count and the `MAX_KV_VALUE_BYTES` check. They do not occupy space in the user's KV budget. In `validate_limits()`:

```rust
// Only count user keys toward the limit
let user_key_count = effective_keys.iter()
    .filter(|k| !k.starts_with(SYSTEM_KV_PREFIX))
    .count();
if user_key_count > MAX_KV_KEYS { ... }
```

### 2.5 When Stats Are Written

The stats key is written during **every** `ack_orchestration_item` — including terminal acks (Completed, Failed, Terminated). This means stats persist through the orchestration's entire lifecycle and remain readable via `Client::get_kv_value()` until the instance is deleted.

Implementation location: `src/runtime/dispatchers/orchestration.rs`, in the post-turn processing block, after the turn completes but before `validate_limits()` runs. The stats are injected as a `KeyValueSet` event appended to the history delta.

**Data sources:**

- `history_event_count`: `history_mgr.full_history_len()` (persisted + delta)
- `history_size_bytes`: **estimate** — `prev_history_size_bytes` (from persisted history at fetch time) + sum of `serde_json::to_string(&event).len()` for each event in the history delta. Cheap O(delta) per turn, monotonically increasing within an execution.
- `queue_pending_count`: count unmatched arrivals from the context's `queue_arrivals` minus resolved subscriptions
- `kv_user_key_count`: `ctx.kv_state` keys not starting with `SYSTEM_KV_PREFIX`
- `kv_total_value_bytes`: sum of value lengths for user keys in `ctx.kv_state`

### 2.6 Convenience Accessor

```rust
impl OrchestrationContext {
    /// Read the runtime introspection stats for this orchestration.
    ///
    /// Returns `None` on the very first turn of a brand-new orchestration
    /// (stats haven't been written yet). Available from the second poll onward.
    pub fn get_system_stats(&self) -> Option<SystemStats> {
        self.get_kv_value(SYSTEM_KV_STATS)
            .and_then(|s| serde_json::from_str(&s).ok())
    }
}
```

Note: On the first turn of a new orchestration, `get_system_stats()` returns `None` because the stats key hasn't been acked yet. From the second turn onward (after the first `ack_orchestration_item`), it reflects the state at the end of the previous turn. Stats persist through terminal state and remain readable via `Client::get_kv_value()` until instance deletion.

---

## 3. Files Changed

| File | Changes |
|---|---|
| `src/runtime/limits.rs` | Update three constants, add `SYSTEM_KV_PREFIX`, `SYSTEM_KV_STATS`, `SystemStats` |
| `src/lib.rs` | Guard `set_kv_value`/`clear_kv_value`/`clear_all_kv_values` against system prefix; re-export constants and `SystemStats`; add `get_system_stats()` |
| `src/runtime/dispatchers/orchestration.rs` | Write stats blob post-turn; exclude system keys in `validate_limits()` |
| `src/runtime/execution.rs` | Update `MAX_CARRY_FORWARD` assertion test (`== 100`) |
| Tests (various) | Update limit-related tests, add new tests for system keys |

---

## 4. Test Plan

### 4.1 Limit Increase Tests

- Verify carry-forward of >20 (up to 100) events across `continue_as_new()`.
- Verify carry-forward drops events beyond 100 with warning.
- Verify KV store accepts up to 150 user keys without error.
- Verify KV store rejects >150 user keys.
- Verify KV value up to 64 KiB is accepted.
- Verify KV value >64 KiB is rejected.

### 4.2 System Key Protection Tests

- `set_kv_value("__duroxide.foo", ...)` panics with descriptive message.
- `clear_kv_value("__duroxide.foo")` panics.
- `clear_all_kv_values()` clears user keys but preserves system keys.
- `get_kv_value("__duroxide.stats")` returns the stats blob (on 2nd+ turn).
- `get_kv_all_keys()` includes system keys.
- `get_kv_length()` includes system keys (total count, not limit-governed count).

### 4.3 System Stats Accuracy Tests

- After a turn with N activities scheduled, `history_event_count` ≥ N.
- After enqueuing M messages and dequeuing K, `queue_pending_count` == M - K.
- After setting J user KV keys, `kv_user_key_count` == J.
- `kv_total_value_bytes` matches sum of user value sizes.
- System key is excluded from `kv_user_key_count`.

### 4.4 Existing Tests

- Update `max_carry_forward_constant_is_20` → `max_carry_forward_constant_is_100`.
- Update any tests that assert specific limit values.
- Ensure all existing KV tests pass with the new limits.

---

## 5. Design Decisions (Resolved)

1. **Panic for system key writes.** Writing a system key is a programming error, same as exceeding `MAX_WORKER_TAGS`. `assert!` is the right enforcement.

2. **History size is an estimate (incremental).** We do NOT scan the full history each turn. Instead:
   - The provider records `prev_history_size_bytes` from the persisted history (loaded at fetch time, stored in the history manager or execution metadata).
   - On each turn, the runtime sums the serialized size of the **history delta events only** and adds it to `prev_history_size_bytes`.
   - This yields an estimate that is cheap (O(delta) per turn) and monotonically increasing within an execution. It may drift slightly from the true serialized size due to compaction or encoding differences, but that's acceptable for introspection.

3. **Stats available after first ack, persisted through terminal state.** The `__duroxide.stats` key is written during every `ack_orchestration_item` call — including the terminal ack (Completed/Failed/Terminated). This means:
   - First turn: `get_system_stats()` returns `None` (stats haven't been acked yet).
   - Second turn onward: returns stats from the end of the previous turn.
   - After terminal state: stats remain in the KV store and are readable via `Client::get_kv_value()` until the instance is deleted.
   - This is important for post-mortem diagnostics — you can inspect the final history size and queue state of a completed orchestration.

4. **Future system keys.** The `__duroxide.` prefix reserves the entire namespace for future system keys (e.g., `__duroxide.parent`, `__duroxide.replay_version`).
