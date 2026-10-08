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
- `uv build`: wheel and source distribution built; final artifact validation follows
  the completed source checkout and consumer installation.
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
