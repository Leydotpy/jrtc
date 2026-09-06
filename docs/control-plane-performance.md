# Control-plane performance upgrade

This implementation follows the repository-wide `AGENTS.md` contract. No nested
`AGENTS.md` files exist under `src`, so the root policy applies to every source
module.

## Implemented invariants

- WebSocket and HTTP dispatch resolve transaction state before local event routing
  and global publication.
- Transport ingress uses synchronous `JanusEventPublisher.try_admit()`. It neither
  waits for capacity nor serializes a broker payload.
- A fixed worker pool owns Pydantic normalization, broker calls, finite publish
  deadlines, and bounded retries.
- Publisher, local-listener, plugin-event, transport-listener, transaction, and
  HTTP session/poller work all have explicit bounds and deterministic teardown.
- Overflow is generic and observable: normal events reject newest, media/slow-link
  state can coalesce, and protected lifecycle events may evict the oldest queued
  non-protected event.
- Transport/session loss synchronously fences the session generation and every
  plugin handle. `JanusSessionManager` is the sole recovery owner and deduplicates
  simultaneous close, keepalive, and health-sweep signals by slot generation.
  Pending requests for that session fail immediately without aborting unrelated
  sessions multiplexed by the same built-in transport.
- ICE sequences remain one ordered plural Janus request, with a 256-candidate
  safety ceiling. Completion remains a separate singular `{ "completed": true }`
  request and never follows from an empty sequence.
- Routine logs contain metadata rather than full Janus, SDP, or ICE payloads.

## Reproducible local evidence

The following architecture-regression run completed on Python 3.12.12 / Windows
11. It uses real JRTC request models, parser, dispatcher, transaction table, event
publisher, and plugin trickle methods with bounded in-process socket/broker doubles:

```powershell
uv run python benchmarks/control_plane.py `
  --transactions 128 `
  --stalled-transactions 128 `
  --events 2000 `
  --ice-candidates 128 `
  --queue-capacity 32
```

Observed structural results:

| Check | Result |
| --- | ---: |
| Transactions completed while broker remained blocked | 128 / 128 |
| Outstanding transactions after broker-stall run | 0 |
| Healthy-to-stalled average receive-to-resolution delta | +3.239 µs |
| Stalled-run publisher depth / configured capacity | 32 / 32 |
| Event-storm accepted / rejected-or-coalesced | 32 / 1,968 |
| Event-storm peak traced ingress memory | 47,260 bytes |
| ICE requests for 128 candidates, batches 1 / 4 / 8 / 16 / 32 | 128 / 32 / 16 / 8 / 4 |

The same transaction storm was run with session-pool sizes 1, 2, and 4. This local
in-process sample produced roughly 2,490, 2,994, and 3,016 frames/second,
respectively, but it is not deployment evidence for changing the default. The
default remains one session; live Janus runs must also measure CPU, RSS, handle
count, recovery, and network behavior before changing pool policy.

A `cProfile` pass showed admission/metric accounting above Pydantic validation in
the measured hot path; publisher response serialization was worker-only and was
not a leading cumulative cost. No speculative serializer rewrite was made.

## Required deployment evidence

The included benchmark proves boundedness and control-flow independence, not
backend durability or production capacity. Before deployment, repeat broker-stall,
event-storm, rolling-restart, and recovery injection against the exact Janus and
broker versions. Record transaction p50/p95/p99, event-loop lag, CPU/RSS, Janus
session/handle counts, broker redelivery, and recovery duration.
