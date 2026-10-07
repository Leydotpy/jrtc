# AGENTS.md — JRTC core

## Scope and reading order

This file governs work in this repository. The current task is an evidence-backed refinement of the supplied requirements. These instructions do not assert that the requested application work is already complete.

1. Read [the audit](docs/agent-audit/README.md) and its pinned source baseline.
2. Select the relevant concern in [WORK_PLAN.md](docs/agent-audit/WORK_PLAN.md); inspect the current code before editing.
3. Read the affected numbered sections of [source-requirements.md](docs/agent-audit/source-requirements.md) and the [shared contract](docs/agent-audit/CROSS_REPO_CONTRACT.md). The original brief is preserved for traceability. Non-conflicting requirements remain applicable; the explicit decisions below supersede obsolete or ambiguous wording.
4. Update [progress.json](docs/agent-audit/progress.json) and the matching [requirements.csv](docs/agent-audit/requirements.csv) with evidence, not estimates.

Audited target: `Leydotpy/jrtc` at `main`, commit `de64c1e02b550049c8253fdef7d47a87b765c9e8`. Work from the requested branch/current authorized successor; never silently switch to the default branch. Evidence for an older commit must be rechecked before marking a current task complete.

## Current decisions and stronger acceptance rules

- Broka is the supported generic messaging abstraction used here; retain it. The older phrase about avoiding Broka assumptions means avoiding application/domain schemas or coupling transaction progress to a backend, not removing the existing Broka integration.
- The performance implementation already exists. Inspect and preserve try_admit, deferred worker serialization, bounded listener queues, plugin dispatch and one-owner recovery; do not create parallel replacements to satisfy historical imperative wording.
- Preserve try_admit compatibility: True means the exact response reference owns one queue slot; False includes rejected, invalid, stopped and coalesced input. Use counters to distinguish outcomes. Protected admission and successful enqueue do not promise durable delivery.
- Treat admitted response objects as immutable until all readers/serialization finish. Any ownership change must avoid deep copies on the reader and include a mutation/race test. Capacity accounting includes in-flight work; frame-size and total memory budgets need deployment measurements.
- Keep constant-time/nonblocking requirements explicit for synchronous lifecycle callbacks and telemetry sinks. A callback being synchronous is not evidence that it is cheap; application network/database work belongs in bounded workers. Never silently change callback timing or ordering without compatibility tests.
- set_loss_handler belongs exclusively to the manager recovery owner. A future application observer API must be distinct, bounded and independently removable; its existence must not be assumed by Synq before release.
- Reclaim remains deferred; do not resurrect handles or change the pool default as part of instruction cleanup. Benchmark with the exact supported package/broker versions before tuning.
- Existing local benchmark figures are prior repository evidence, not fresh test results and not deployment capacity. Keep raw outputs, runtime versions and artifact/commit identity with every new claim.

## Reviewable execution

- Work on a separate branch. Keep one concern and its necessary regression checks reviewable in each commit/PR; preserve unrelated changes.
- Follow the user-authorized scope. This audit does not pre-authorize future runtime changes. For later implementation, the user can select one concern, several concerns or the whole plan; do not add redundant permission gates for work already authorized. Do not merge/deploy unless authorized.
- If the user selects one concern, report its result and pause before starting another. Hand back changed paths, behavioral effect, exact verification commands/results, known limitations, dependency effects and rollback steps.
- Use task states `not_started`, `in_progress`, `blocked`, `ready_for_review`, `accepted`. Only the user/maintainer accepts work. A passing unit test or merged documentation is not implementation completion.
- Keep implementation state (`present`, `partial`, `missing`, `policy`, `deferred`) separate from verification state (`source_review`, `runtime_subset_passed`, `reproduced_gap`, `external_not_verified`). New instructions stay pending until code and the relevant acceptance evidence exist.
- Preserve source section IDs; add improvement IDs instead of silently deleting requirements. Record blockers explicitly. Unavailable infrastructure is a verification limitation, not a pass or a code failure.
- Use deterministic barriers/fake clocks for lifecycle races. Do not replace meaningful assertions with source-string matching or fake external dependencies merely to make a suite pass.
- Never include tokens, credentials, auth payloads, full SDP or raw ICE addresses in audit artifacts, commits or routine logs.

## Verification commands

Run from the repository root with the declared runtime/dependencies and required services:

```sh
uv sync --all-extras --group dev
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src/jrtc
uv run python benchmarks/control_plane.py
uv build
```

These are required follow-up gates, not claims that they passed in this audit. See the audit for what actually ran. A final completion report must include cross-repository compatibility and any live checks required by the selected concern.
