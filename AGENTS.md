# AGENTS.md — JRTC Realtime Control-Plane Performance Upgrade

## 0. Mission

This repository is the generic, application-independent Janus/JRTC control-plane library.

Implement the changes in this file so that JRTC remains:

- natively asynchronous;
- safe under high concurrent signaling load;
- fast on the Janus WebSocket receive path;
- resistant to broker/event-publisher backpressure;
- explicitly capable of batched ICE trickling;
- deterministic about session-loss recovery;
- bounded in memory and work queues;
- observable without logging sensitive or enormous SDP/ICE payloads;
- reusable by Synq and other applications without embedding Django, Socket.IO, Broka, or application-domain assumptions.

Do **not** move Synq-specific participant, meeting, database, permission, or Socket.IO logic into this package.

The highest-priority goal is to keep the Janus command/response connection independent from slow or overloaded external event consumers.

---

## 1. Repository ownership boundary

JRTC owns:

1. Janus transport/WebSocket lifecycle.
2. Janus request/response transaction correlation.
3. Janus session management and health.
4. Generic plugin attachment/detachment.
5. Generic plugin event dispatch.
6. Generic event publication hooks/queues.
7. Generic typed Janus protocol models.
8. Generic ICE trickle request construction.
9. Session-loss signaling and recovery primitives.

JRTC does **not** own:

- Django ORM state;
- meeting participants;
- Socket.IO payloads;
- frontend ICE batching policy;
- VideoRoom-specific management-handle policy beyond generic plugin support;
- application persistence;
- Broka-specific domain event schemas;
- user-facing meeting sounds.

Keep these boundaries strict.

---

## 2. Known hot-path files

Audit and modify, where appropriate:

- `src/jrtc/transport/websocket.py`
- `src/jrtc/messaging/dispatcher.py`
- `src/jrtc/messaging/publisher.py`
- `src/jrtc/lib/plugins/base.py`
- `src/jrtc/models/request.py`
- the module implementing `JanusSession`
- the module implementing `JanusSessionManager`
- existing transport/dispatcher/session tests
- existing plugin/request-model tests

If exact module names have moved, locate the current implementation rather than creating parallel replacements.

---

## 3. Non-negotiable invariants

### 3.1 Janus receive loop must stay hot

The WebSocket receive loop MUST NOT wait on:

- Redis;
- RabbitMQ;
- Kafka;
- Broka;
- Django;
- databases;
- filesystem persistence;
- analytics;
- webhooks;
- application callbacks;
- queues that may block waiting for capacity;
- expensive event serialization.

The intended receive-loop work is:

```text
receive frame
    -> parse Janus envelope
    -> resolve transaction immediately
    -> route plugin-local event immediately
    -> attempt non-blocking global event admission
    -> continue receiving
```

External publication happens later in background workers.

### 3.2 Transaction resolution has priority over global event fan-out

A slow event sink must never delay the Future waiting for a Janus response.

For a response carrying a transaction ID:

1. resolve or advance the transaction state first;
2. route the event to its attached plugin without blocking the reader;
3. only then attempt best-effort/non-blocking admission to global event publication.

Preserve the existing rule that an `ack` must not complete a transaction that is waiting for the final plugin `event`.

### 3.3 All queues are bounded

Never introduce an unbounded `asyncio.Queue`, task set, list, or retry buffer on a realtime hot path.

Every queue must have:

- a capacity;
- an overflow policy;
- metrics;
- deterministic shutdown/drain behavior.

### 3.4 JRTC remains async-native

Do not add `asyncio.run()`, `run_coroutine_threadsafe()`, `sync_to_async()`, thread executors, or synchronous wrappers inside JRTC merely to make application integration easier.

Applications may provide compatibility bridges outside JRTC.

### 3.5 Plugin event dispatch remains non-blocking from transport

The existing `Plugin._dispatch_event()` model is directionally correct:

- bounded queue;
- `put_nowait`;
- per-handle worker;
- oldest-event drop under overflow;
- `dropped_events` accounting.

Preserve this architecture unless a measured improvement replaces it with an equally bounded/non-blocking design.

---

# PART I — NON-BLOCKING GLOBAL EVENT INGRESS

## 4. Add a non-blocking publisher admission API

The current global event publisher admission path must not be capable of stalling the Janus reader while it waits for queue capacity.

Add an API with semantics equivalent to:

```python
def try_admit(self, event: EventEnvelope) -> bool:
    ...
```

or another clearly named synchronous/non-awaiting equivalent.

Required properties:

- returns immediately;
- never waits for queue capacity;
- returns `True` only when the event has been admitted;
- returns `False` when overflow policy rejects/coalesces/spools elsewhere;
- updates metrics;
- does not serialize a full broker payload before admission;
- is safe to call from the transport/dispatcher loop.

Do not silently replace the existing awaited `admit()` API if external callers rely on it. It is acceptable to keep:

```python
await publisher.admit(...)
```

for non-hot-path callers while introducing:

```python
publisher.try_admit(...)
```

for transport ingress.

Document the semantic difference.

---

## 5. Refactor dispatcher behavior

The dispatcher must distinguish three concerns:

1. transaction completion;
2. plugin-local event routing;
3. global/application event publication.

These must not share one awaited critical path.

Target structure:

```python
async def dispatch(message):
    resolve_transaction_if_applicable(message)
    dispatch_to_plugin_queue_if_applicable(message)
    publisher.try_admit(lightweight_envelope(message))
```

The exact functions may differ, but the ordering and non-blocking behavior are required.

If transaction resolution itself currently performs unrelated publication work, split it.

---

## 6. Move serialization out of the receive path

Do not perform expensive operations such as:

- repeated `model_dump(mode="json")`;
- deep dictionary normalization;
- broker-schema construction;
- large immutable copies;
- JSON encoding;
- event enrichment requiring external data;

inside the WebSocket reader.

Instead, enqueue a lightweight internal envelope containing only references/typed data required by a background publisher worker.

Example conceptual structure:

```python
@dataclass(slots=True)
class EventIngressEnvelope:
    received_monotonic: float
    session_id: int | None
    handle_id: int | None
    janus_type: str
    event: JanusResponse
    priority: EventPriority
```

The background worker may then serialize to the configured external publisher format.

Avoid retaining unnecessary large duplicate payloads.

---

# PART II — EVENT PRIORITY, OVERFLOW, AND DELIVERY POLICY

## 7. Classify event importance

Introduce a generic policy mechanism rather than hard-coding Synq-specific event names.

At minimum support conceptual classes equivalent to:

### Protected / high importance

Examples:

- session destroyed/lost;
- handle detached;
- fatal transport/session errors;
- plugin lifecycle state that consumers require for correctness.

### Normal

Examples:

- ordinary plugin state notifications.

### Coalescible / telemetry

Examples:

- repeated slow-link state;
- frequent media statistics;
- routine high-frequency observations where latest-state-wins is acceptable.

The actual classifier may use Janus types, plugin metadata, caller-supplied policy, or a small extensible registry.

Do not introduce VideoRoom-specific participant business logic in core JRTC.

---

## 8. Overflow policy must be explicit

Supported policy options may include:

- reject/drop newest;
- drop oldest;
- coalesce by key;
- reserve protected capacity;
- enqueue into a separate durable spool handled outside the reader;
- priority queues.

Requirements:

- no overflow policy may block the reader;
- protected-event behavior must be documented;
- metrics must expose every loss/coalescing decision;
- tests must cover full queues.

If durable delivery is required, durability happens in a background component. Never synchronously publish to a broker from the reader as a durability shortcut.

---

## 9. Do not create an unbounded task-per-event model

Avoid:

```python
asyncio.create_task(publish(event))
```

for every Janus event with no semaphore/queue/ownership.

Use a bounded worker pool fed by a bounded queue.

Workers must:

- survive individual publication errors;
- use bounded retries;
- surface metrics;
- have deterministic shutdown;
- not block session/transport teardown forever.

---

# PART III — ICE TRICKLE CONTRACT

## 10. Preserve sequence-capable ICE trickling

JRTC already has the right basic API shape:

```python
Plugin.trickle(
    TrickleCandidate | Sequence[TrickleCandidate]
)
```

A single candidate must produce the singular Janus field:

```json
{
  "janus": "trickle",
  "candidate": { ... }
}
```

A sequence must produce:

```json
{
  "janus": "trickle",
  "candidates": [
    { ... },
    { ... }
  ]
}
```

Do not regress this into a loop that submits one Janus transaction per candidate.

The sequence must be preserved in order.

---

## 11. Preserve explicit completion semantics

`complete_trickle()` must continue to use the canonical singular completion marker:

```json
{
  "janus": "trickle",
  "candidate": {
    "completed": true
  }
}
```

Do not silently reinterpret an empty candidate list as completion.

Do not require applications to put `{completed: true}` inside the `candidates` array.

Synq will implement final-batch semantics as:

```text
trickle([remaining candidates])
then
complete_trickle()
```

when its frontend message contains both remaining candidates and `completed=true`.

---

## 12. Keep request-model safety

The current `TrickleRequest` defensive hard limit (currently 256 candidates) should remain unless there is protocol evidence and benchmark justification to change it.

JRTC's maximum is a protocol safety ceiling, not the recommended application batch size.

Synq frontend/backend will use much smaller operational batch sizes.

Required model behavior:

- reject an empty `candidates` array;
- reject a payload containing both singular `candidate` and plural `candidates`;
- reject a payload containing neither;
- validate `sdpMid`/`sdpMLineIndex`;
- validate the completion marker shape.

---

## 13. ICE must remain a no-final-plugin-event command

Trickle requests should continue to avoid waiting for a plugin-specific final event where Janus only needs the transport-level response.

Preserve the low-latency semantics equivalent to:

```python
wait_for_event=False
```

for `trickle` and `complete_trickle`.

---

# PART IV — SESSION LOSS AND RECOVERY

## 14. Make recovery notification-driven

Current periodic health monitoring is useful but should be the safety net, not the primary detector.

Target:

```text
transport connection lost
    -> session immediately transitions to LOST/unhealthy
    -> session manager is notified
    -> session manager schedules recovery/replacement
```

Periodic monitoring remains to catch missed edge cases.

---

## 15. Define a single recovery owner

Avoid having both:

- transport reconnect logic, and
- `JanusSessionManager`

independently trying to recreate/replace the same session.

Preferred rule:

> The transport detects and reports loss. The session/session manager owns recovery policy.

If the existing transport has reconnect primitives, make them subordinate to the session manager or clearly define a single state machine.

Add tests for simultaneous:

- socket close;
- keepalive failure;
- request timeout;
- manager health sweep.

There must be only one replacement/recovery attempt for the same generation.

---

## 16. Session reclaim is a later resilience phase

JRTC supports Janus session claim/reclaim semantics.

Do not make handle adoption/reclaim the first performance optimization.

If implemented later:

- model transport generation explicitly;
- prove plugin-handle ownership after reclaim;
- prevent old and new transports from both considering themselves authoritative;
- add extensive failure-injection tests.

A clean replacement is preferable to unsafe handle resurrection.

---

# PART V — SESSION POOL POLICY

## 17. Do not increase pool size as a substitute for hot-path fixes

One Janus WebSocket can have many outstanding transaction IDs.

Therefore:

1. fix reader blocking;
2. fix publication backpressure;
3. fix unnecessary application sync bridges;
4. fix ICE request amplification;
5. only then benchmark JRTC session pool sizes.

Benchmark at least:

- 1;
- 2;
- 4

sessions under the same workload.

Do not change defaults solely because a larger number appears faster in a microbenchmark.

Measure:

- transaction RTT p50/p95/p99;
- event-loop lag;
- CPU;
- memory;
- Janus session/handle count;
- reconnect/recovery behavior.

---

# PART VI — LOGGING AND SERIALIZATION

## 18. Logging rules

Production logs MUST NOT routinely contain:

- full SDP;
- full ICE candidate strings;
- full Janus payloads;
- credentials/tokens;
- giant plugin bodies.

Prefer structured metadata:

- Janus session ID;
- handle ID;
- transaction ID;
- Janus message type;
- plugin identifier;
- queue depth;
- duration milliseconds;
- result/status;
- recovery generation;
- drop/coalesce counts.

Full payload logging may exist only behind explicit diagnostic/debug controls and should be sampled where practical.

---

## 19. Profile before rewriting serializers

After structural fixes, profile:

- Pydantic validation cost;
- `model_dump`;
- JSON encoding/decoding;
- repeated model conversions;
- dictionary copying.

Possible later optimizations:

- precompiled `TypeAdapter`s;
- direct `validate_json`;
- fewer model->dict->model conversions;
- pydantic-core primitives.

Do not trade validation correctness for speculative micro-optimization.

---

# PART VII — TEST REQUIREMENTS

## 20. Transport/dispatcher tests

Add tests proving:

1. a full global publisher queue does not prevent transaction completion;
2. a full publisher queue does not delay a subsequent incoming Janus response;
3. plugin-local dispatch still occurs when global admission fails;
4. `ack` does not complete `wait_for_event=True` plugin commands;
5. final plugin event does complete the correct transaction;
6. out-of-order concurrent transactions resolve correctly;
7. reader loop continues while broker workers are artificially stalled;
8. external publisher exceptions never terminate the reader.

Use deterministic synchronization primitives rather than timing-only sleeps where possible.

---

## 21. Queue overload tests

Test:

- normal capacity;
- exact capacity;
- overflow;
- protected-event admission;
- coalescing;
- shutdown while full;
- worker cancellation;
- retry exhaustion.

Assert metrics/counters, not just absence of exceptions.

---

## 22. ICE tests

At package level prove:

1. one candidate creates the singular `candidate` field;
2. a sequence of 16 candidates creates one request with one `candidates` array of length 16;
3. ordering is unchanged;
4. empty sequence is rejected;
5. completion uses singular `{completed: true}`;
6. candidate sequence requests do not wait for a plugin final event;
7. the hard maximum is enforced;
8. publisher/plugin subclass behavior does not change sequence semantics.

---

## 23. Recovery tests

Inject:

- transport disconnect;
- keepalive failure;
- simultaneous manager sweep;
- session replacement during outstanding requests;
- shutdown during recovery.

Prove:

- one recovery owner;
- no duplicate active sessions for one generation;
- pending requests terminate predictably;
- stale plugin handles are invalidated;
- shutdown completes.

---

# PART VIII — BENCHMARKS AND OBSERVABILITY

## 24. Add/retain metrics suitable for load testing

Expose or make observable:

- frames received/sec;
- receive->transaction-resolution latency;
- dispatcher duration;
- global ingress queue depth;
- global ingress accepted/rejected/coalesced/spooled counts;
- publisher worker latency;
- publisher error/retry counts;
- plugin event queue drops;
- outstanding transaction count;
- request RTT p50/p95/p99;
- event-loop lag;
- session recovery count/duration.

Metrics implementation must itself be low overhead.

---

## 25. Benchmark scenarios

At minimum:

### Scenario A — transaction storm

Many concurrent plugin commands with no external event sink pressure.

### Scenario B — broker stall

Artificially block the external publisher while continuing Janus command responses.

Expected result: command RTT should remain stable within a small bounded delta.

### Scenario C — event storm

Generate high-rate Janus/plugin events and prove memory remains bounded.

### Scenario D — batched ICE

Compare:

- 1 candidate/request;
- batches of 4;
- batches of 8;
- batches of 16;
- batches of 32.

JRTC must show request-count reduction without introducing reader stalls.

---

# PART IX — PUBLIC API AND COMPATIBILITY

## 26. Preserve compatibility where reasonable

Avoid breaking:

- plugin subclass APIs;
- request models;
- session APIs;
- event publisher integrations;
- `Plugin.trickle()` sequence support.

When adding `try_admit`, prefer an additive API.

Any unavoidable breaking change must:

- be documented;
- include migration notes;
- update tests;
- update type hints;
- avoid application-specific workarounds in JRTC core.

---

# PART X — IMPLEMENTATION ORDER

Implement in this order:

1. Add tests reproducing receive-loop blockage under publisher backpressure.
2. Add non-blocking global event ingress.
3. Split dispatcher transaction/plugin/global-publication paths.
4. Move serialization to publisher workers.
5. Add explicit overflow/priority policy and metrics.
6. Add notification-driven session-loss path with one recovery owner.
7. Strengthen ICE batch/request tests; preserve current API.
8. Benchmark session pool sizes only after the above.
9. Profile JSON/Pydantic/logging.
10. Optimize serializers only when measurements justify it.

---

# PART XI — DEFINITION OF DONE

This JRTC upgrade is complete only when:

- the Janus receive loop never awaits external publisher capacity;
- a stalled broker does not materially delay unrelated Janus transaction resolution;
- all queues remain bounded;
- plugin event routing remains non-blocking from transport;
- sequence ICE trickling remains one Janus transaction per batch;
- completion semantics remain explicit;
- session recovery has one owner;
- tests cover overload and concurrency;
- production logging avoids full SDP/ICE payloads;
- benchmarks demonstrate the structural changes before session-pool defaults are reconsidered.

Do not mark the task complete merely because unit tests pass. Include concurrency/overload evidence in the implementation notes.
