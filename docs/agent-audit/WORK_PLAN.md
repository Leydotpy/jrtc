# JRTC core — concern-by-concern work plan

The audit/instruction change is ready for review. The initial audit left the tasks pending. Current implementation evidence is in [IMPLEMENTATION.md](IMPLEMENTATION.md). Existing code is credited in requirements.csv; tasks describe the remaining corrections or verification, not a request to repeat completed features.

Task IDs are unique across the four repositories: S=Synq, F=frontend, C=core, V=VideoRoom. A dependency in another repository refers to that repository's WORK_PLAN.md. Dependencies gate integration/release; isolated test preparation may proceed earlier. No task requires automatic delegation to other agents.

| Task | Priority | Dependencies | State |
| --- | --- | --- | --- |
| C-T01 — Re-run the implemented core invariants | P1 | None | ready_for_review |
| C-T02 — Define an application loss-observer contract | P1 | None | ready_for_review |
| C-T03 — Collect load and release compatibility evidence | P2 | C-T01 | ready_for_review |

## C-T01 — Re-run the implemented core invariants

**Scope:** existing core tests; installed dependency matrix.

**Acceptance:** Run the full Python suite, type/lint/build gates and focused ingress/ICE/recovery tests with real pinned Broka/Dispio. Record commit and versions. Do not rebuild already-present ingress or recovery abstractions. Confirm true means an exact queue slot; false includes coalescing, and neither guarantees durable delivery.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## C-T02 — Define an application loss-observer contract

**Scope:** manager/session public lifecycle API and consumer integration.

**Acceptance:** Document the existing single recovery-owner rule. If Synq requires proactive application observation beyond current health checks, add a separate bounded observer interface with tests; do not repurpose set_loss_handler. Synchronous handle fencing remains immediate, observers cannot replace sessions or await persistence on the reader, and shutdown unsubscribes them. Mark the API proposed until implemented and released.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## C-T03 — Collect load and release compatibility evidence

**Scope:** benchmark harness; external Janus/brokers; package release.

**Acceptance:** Run matched healthy/stalled/event-storm/ICE workloads and 1/2/4 pools, recording p50/p95/p99, loop lag, CPU/RSS, queue high-water marks, drops, recovery duration and versions. Account for receive frame size times outstanding capacity and mutable payload ownership. Profile worst-case protected eviction/coalescing before optimizing. Verify supported Python versions and published wheels; retain default pool size until evidence supports a change.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## Updating the tracker

When starting, set only the selected task to in_progress and record its branch. Record blocked reasons when a real dependency prevents progress. On completion, add implementation_commit and verification_evidence with commands, runtime/version, observed results and evidence paths; update the relevant requirement rows. Preserve this audit baseline, add a dated assessment for a new head, and never overwrite historical evidence as if it were obtained at the new commit. For a docs-only change, leave application task states unchanged.
