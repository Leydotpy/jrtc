# JRTC core — AGENTS implementation audit

Audit date: 2026-10-07. **The principal structural implementation is present: non-waiting ingress, worker serialization, bounded overflow/retries, batch ICE and notification-driven recovery. Full completion is not certified: Python suites were not executable here and integrated deployment measurements remain open.**

This PR improves instructions and adds an evidence/tracking pack. It does **not** change runtime application code, fix the listed defects, or declare the remaining implementation tasks complete. Review and merge the instruction update independently; select implementation concerns from WORK_PLAN.md afterward.

## Audited baselines

| Repository scope | Requested branch | Audited commit |
| --- | --- | --- |
| Synq backend | `v4` | `2d8682701300f27ab3771e9ea72413b3f7631a49` |
| Synq browser client | `codex/batched-ice-meeting-sounds` | `da471c92989ccf28a26de14d6ec4d1356c647bed` |
| JRTC core | `main` | `de64c1e02b550049c8253fdef7d47a87b765c9e8` |
| jrtc-video | `main` | `bec03a533ab3af33c1deef822e8249f123a007dc` |

The branch heads were rechecked and unchanged before preparing this update. All code evidence links are pinned to the audited commits. The attached standalone AGENTS.md duplicates AGENTS(5).md and is not an additional scope. Uploaded package metadata was treated as supporting context; repository source at the requested refs is authoritative for implementation status. Secret material was excluded from retrieval and commits.

The four repository trees were inspected, and 302 relevant source, test, configuration and documentation files were retrieved initially (plus the frontend provider/layout follow-up). Generated/vendor snapshots and unrelated plugins are outside the implementation scope. This is a requirement-focused code audit, not a claim that every line of every repository received a security review.

## Meaning of the statuses

- **present**: relevant implementation found; verification is reported separately.
- **partial**: some implementation exists, but a defect, omitted behavior or acceptance evidence remains.
- **missing**: required implementation was not found or the current path still implements the superseded behavior.
- **policy**: ownership/rollout constraint, not a standalone feature completion.
- **deferred**: the original brief explicitly postpones the work.

source_review means code/tests were inspected. runtime_subset_passed covers only the named executed subset. reproduced_gap records a controlled observation of a defect. external_not_verified means required live/deployment evidence is absent. No completion percentage is manufactured from these categories.

## Requirement coverage

Every numbered section in the supplied file is mapped in [requirements.csv](requirements.csv), with stable section IDs, separate implementation/verification status, findings and pinned evidence links. Rows share a group assessment where the same code serves several requirements; the CSV does not imply that every sub-bullet has an individual passing test. Unnumbered mission/order/definition-of-done text is assessed by this verdict and the work plan.

| Group | Original sections | Concern | Implementation | Verification |
| --- | --- | --- | --- | --- |
| C01 | 0, 1, 2 | Generic ownership boundaries | policy | source_review |
| C02 | 3, 4, 5, 6, 20 | Non-waiting response and publication ingress | present | source_review |
| C03 | 7, 8, 9, 21 | Bounded overload, priority and retry behavior | present | source_review |
| C04 | 10, 11, 12, 13, 22 | Generic ICE request contract | present | source_review |
| C05 | 14, 15, 23 | Single-owner recovery and generation fencing | present | source_review |
| C06 | 16 | Session reclaim | deferred | source_review |
| C07 | 17, 24, 25 | Metrics, benchmark scenarios and pool evidence | partial | external_not_verified |
| C08 | 18, 19 | Safe logging and measured serialization changes | present | source_review |
| C09 | 26 | Additive API compatibility | present | source_review |

## Principal findings

No missing implementation of the main non-waiting publisher/ICE/recovery architecture was found in the inspected paths. The limitations are verification and integration rather than grounds to rewrite those components. `docs/control-plane-performance.md` contains prior in-process results (including 128 stalled-broker transactions); they were read, not rerun. The original brief's Broka wording is clarified so a future agent does not remove the user's intended messaging integration.

### C01 — Generic ownership boundaries

No Synq/Django business logic belongs in core. Generic Broka integration is intentionally present; the original mission sentence forbidding Broka assumptions must not be interpreted as removing that integration.

Evidence: [pyproject.toml](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/pyproject.toml); [factory.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/messaging/factory.py).

### C02 — Non-waiting response and publication ingress

Dispatcher resolves transactions then queues listeners and calls try_admit; serialization is deferred to publisher workers. Source tests cover full ingress, stalled brokers, ACK/final events, ordering and publisher exceptions.

Evidence: [dispatcher.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/messaging/dispatcher.py#L197); [publisher.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/messaging/publisher.py#L399); [listeners.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/messaging/listeners.py#L140); [test_realtime_ingress.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/tests/test_realtime_ingress.py).

### C03 — Bounded overload, priority and retry behavior

Global slots include queued/in-flight work, worker queues are bounded, telemetry coalesces, protected events can evict normal work, and publish retries/deadlines are finite. Protected events can still be lost when all slots are protected/in flight.

Evidence: [publisher.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/messaging/publisher.py); [test_realtime_ingress.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/tests/test_realtime_ingress.py#L268).

### C04 — Generic ICE request contract

Single candidate versus ordered sequence, explicit singular completion, exactly-one container validation, strict fields, 256 ceiling and wait_for_event=False are present with focused tests.

Evidence: [request.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/models/request.py#L83); [request.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/models/request.py#L126); [base.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/lib/plugins/base.py#L517); [test_ice_contract.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/tests/test_ice_contract.py).

### C05 — Single-owner recovery and generation fencing

Session invalidates plugin handles synchronously; one manager callback schedules generation-fenced replacement. Tests cover concurrent loss signals, shutdown and scoped transaction abort.

Evidence: [base.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/session/base.py#L395); [manager.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/manager.py#L176); [test_session_recovery.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/tests/test_session_recovery.py).

### C06 — Session reclaim

The brief explicitly defers reclaim/adoption as a later resilience phase. Do not count safe replacement instead of reclaim as missing work.

Evidence: [base.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/session/base.py); [manager.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/manager.py).

### C07 — Metrics, benchmark scenarios and pool evidence

Metrics and an in-process benchmark exist. Repository notes record a prior Python/Windows run and explicitly reserve live Janus/broker measurements. Those results were not rerun here and do not certify Synq or production capacity.

Evidence: [websocket.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/transport/websocket.py#L143); [manager.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/manager.py#L113); [control_plane.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/benchmarks/control_plane.py); [control-plane-performance.md](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/docs/control-plane-performance.md).

### C08 — Safe logging and measured serialization changes

Hot-path diagnostics expose metadata, worker serialization is explicit, and existing notes report profiling rather than a speculative serializer rewrite. Preserve bounded metric cardinality and nonblocking callback contracts.

Evidence: [websocket.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/transport/websocket.py#L357); [publisher.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/messaging/publisher.py); [test_realtime_ingress.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/tests/test_realtime_ingress.py#L446); [control-plane-performance.md](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/docs/control-plane-performance.md).

### C09 — Additive API compatibility

Awaited admit remains alongside try_admit; sequence trickle and subclass contracts remain. Package compatibility still needs tests against built artifacts and consumers.

Evidence: [publisher.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/messaging/publisher.py#L337); [base.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/src/jrtc/lib/plugins/base.py); [test_ice_contract.py](https://github.com/Leydotpy/jrtc/blob/de64c1e02b550049c8253fdef7d47a87b765c9e8/tests/test_ice_contract.py).

## Verification actually performed

Full Python tests and benchmarks were not run. An offline dependency attempt failed because broka==0.0.2 was absent from cache; pytest, dispio and logvista were also unavailable in the base runtime. Source tests and prior benchmark notes were inspected, not relabeled as fresh passes.

The GitHub check-runs endpoint returned zero checks for each audited SHA at inspection time. This is not evidence that tests failed or passed; no CI result is claimed. No live Janus, external broker, production database or browser session was exercised. Source assertions and prior repository benchmark prose are not substitutes for those checks.

## Improvements implemented in the instructions

1. A concise root policy replaces ambiguous baseline-as-current wording and records exact repository ownership, branch and precedence.
2. The original supplied requirements are preserved, hashed in progress.json and mapped section-by-section; Synq's earlier migration safety requirements are retained separately.
3. Shared ICE, event identity, lifecycle and release ordering are explicit, including the backend/frontend compatibility blocker.
4. Cancellation, stale async continuations, failure outcomes, queue/byte budgets, notification persistence and real package API checks are turned into concrete acceptance work.
5. Concern IDs, dependencies and review states let the user inspect one job at a time without incorrectly marking documentation or code inspection as implementation success.
6. Verification commands, limits and reproducible observations make claims reviewable; dependency/runtime blockers remain visible.

Read [WORK_PLAN.md](WORK_PLAN.md) for the implementation order and [progress.json](progress.json) for the current task states. The source brief remains in [source-requirements.md](source-requirements.md); proposed stronger behavior is governed by the root decisions and [shared contract](CROSS_REPO_CONTRACT.md).
