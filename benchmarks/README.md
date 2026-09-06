# JRTC control-plane benchmarks

Run the deterministic structural suite after unit tests:

```console
uv run python benchmarks/control_plane.py
```

It covers the four scenarios required by the repository policy:

1. a transaction storm over 1, 2, and 4 simulated WebSocket sessions;
2. transaction completion while the external broker worker is stalled;
3. event ingress overload with a configured memory bound;
4. ICE batches of 1, 4, 8, 16, and 32 candidates.

The JSON result includes request RTT p50/p95/p99, frames per second, event-loop
lag, CPU time, peak traced memory, ingress depth/capacity, and ICE request
counts. This is an architecture regression benchmark, not a substitute for a
deployment test against the production Janus and broker versions. For capacity
decisions, repeat the same workload against live services and record Janus
session/handle counts, process RSS, CPU, reconnects, and recovery duration.

See [`docs/control-plane-performance.md`](../docs/control-plane-performance.md)
for the implementation invariants and one reproducible local evidence snapshot.
