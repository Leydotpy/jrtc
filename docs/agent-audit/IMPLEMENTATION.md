# Implementation evidence — 2026-10-08

Runtime change: bounded application loss observers, separate from the existing
single manager recovery owner. Local shutdown releases registrations. Callback
failures are isolated and counted. Version 3.2.0 is prepared for maintainer release;
no package has been published.

## Verification

Python 3.12.14, Broka 0.0.2, Dispio 0.0.2, LogVista 0.1.0, Pydantic 2.13.5.
With declared test dependencies installed:

- `PYTHONPATH=src python -m pytest`: 110 passed, 3 skipped. The three optional
  jsrv-checkout checks require that separate repository; standalone core checks run.
- `ruff check src tests` and `ruff format --check src tests`: passed.
- `PYTHONPATH=src mypy src/jrtc`: passed, 42 source files.
- `uv build`: wheel and source distribution built; the final wheel includes the license/type markers. The
  frozen Synq consumer built and imported the pinned Git artifact successfully.
- `PYTHONPATH=src python benchmarks/control_plane.py`: synthetic benchmark ran;
  raw output is in `evidence/implementation-core-benchmark.json`. Stalled-broker
  transactions completed and queue admission stayed bounded. These latency/CPU
  figures describe an in-process harness, not live Janus deployment capacity.

## Integration and rollback

Synq must use this API without replacing set_loss_handler and must unsubscribe on
shutdown. Application callbacks may only fence local state/enqueue bounded work.
External Janus, actual broker, TURN-only media, published-wheel and deployment
matrix checks remain pending. C-T03 is blocked on that environment/release.
Revert the implementation commits and retain the previous tested dependency lock
to roll back. No deployment, merge, pool-size change or registry replacement ran.

Code commit: `b0998af03c0702302855cc7c52c3dd53ac7a32af`. Base: `66fceda7530e5d98df01fc7c3b871d30038769f9`.

## Consumer verification completed — 2026-10-08

Synq's frozen consumer install built the exact reviewed Git commits and imported
JRTC 3.2.0 and jrtc-video 3.0.3 from site-packages on Python 3.14.8/Django 6.0.7
with Broka 0.0.2 and Dispio 0.0.2. It passed the required contracts/new regressions
and the real ORM/registry/service integration checks; see Synq's IMPLEMENTATION.md
for the full suite's seven pre-existing missing-startup-file failures.

The installed VideoRoom model/service suite also passed all 33 unittest tests in
that consumer environment. Synq tests prove service reuse, replacement with a
recycled integer ID, and cleanup-before-manager ordering despite repeated caller
cancellation. Core observer callbacks remain separate from manager recovery.

Local wheel SHA-256 values (build provenance, not published registry hashes):

- jrtc-3.2.0: `3e0b82c7edba0c8de20f98fa53af7de9a5cb29c0bef880717d6a6c44faf2d7e8`
- jrtc_video-3.0.3: `041689387e20cf72901aef472c5b874444058cf6504650ba41113353584f2a13`

Review PRs: [Synq #2](https://github.com/Leydotpy/synq/pull/2), [synq.js #7](https://github.com/Leydotpy/synq.js/pull/7), [JRTC #2](https://github.com/Leydotpy/jrtc/pull/2), [VideoRoom #2](https://github.com/Leydotpy/jrtc-plugins/pull/2).
No published-release or live media acceptance is claimed.
