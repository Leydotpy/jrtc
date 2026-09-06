"""Process-local, self-healing Janus session pool.

The pre-3.0 Redis leader/RPC implementation could not preserve plugin events or
fence stale leaders and has intentionally been removed from the production
path.  Each application worker should own its Janus control connections; use a
separate, authenticated broker when cross-process handle ownership is truly
required.
"""

from __future__ import annotations

import asyncio
import itertools
import math
import time
from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING, Any

from logvista import get_logger

from jrtc.auth import JanusCredentials
from jrtc.conf import settings
from jrtc.core.exceptions import JanusConnectionClosed
from jrtc.models.common import JanusId
from jrtc.session.base import SessionLoss, SessionLossHandler, SessionState
from jrtc.session.websocket import WebsocketSession

if TYPE_CHECKING:
    from jrtc.messaging import JanusEventPublisher

logger = get_logger(__name__)

SessionFactory = Callable[[], WebsocketSession]


class JanusSessionManager:
    """Own a bounded pool of independent Janus sessions for one process."""

    def __init__(
        self,
        *,
        pool_size: int | None = None,
        session_factory: SessionFactory | None = None,
        monitor_interval: float = 2.0,
        restart_backoff: float = 1.0,
        fail_fast: bool = True,
        event_publisher: JanusEventPublisher | None = None,
    ) -> None:
        self._pool_size = int(
            pool_size if pool_size is not None else getattr(settings, "JANUS_SESSION_POOL_SIZE", 1)
        )
        if self._pool_size < 1:
            raise ValueError("pool_size must be at least one")
        if any(
            not math.isfinite(value) or value <= 0 for value in (monitor_interval, restart_backoff)
        ):
            raise ValueError("manager timing values must be finite and greater than zero")
        self._factory = session_factory or self._default_session_factory
        self._monitor_interval = monitor_interval
        self._restart_backoff = restart_backoff
        self._fail_fast = fail_fast
        self._event_publisher = event_publisher
        self._sessions: list[WebsocketSession] = []
        self._round_robin = itertools.count()
        self._lock = asyncio.Lock()
        self._monitor_task: asyncio.Task[None] | None = None
        self._replacement_tasks: dict[int, asyncio.Task[None]] = {}
        self._replacement_generations: dict[int, int] = {}
        self._slot_generations: list[int] = []
        self._loss_handlers: dict[WebsocketSession, SessionLossHandler] = {}
        # Exactly one stale-session cleanup may exist for each bounded pool
        # slot; a replacement does not finish until its slot cleanup does.
        self._cleanup_tasks: dict[int, asyncio.Task[None]] = {}
        self._stopping = asyncio.Event()
        self._metrics: dict[str, int | float] = {
            "loss_notifications": 0,
            "recovery_signals_deduplicated": 0,
            "recoveries_started": 0,
            "recovery_attempts": 0,
            "recoveries_completed": 0,
            "recovery_failures": 0,
            "recovery_duration_ms_total": 0.0,
            "last_recovery_duration_ms": 0.0,
            "event_loop_lag_samples": 0,
            "event_loop_lag_ms_total": 0.0,
            "event_loop_lag_ms_last": 0.0,
            "event_loop_lag_ms_max": 0.0,
        }

    def _default_session_factory(self) -> WebsocketSession:
        token = settings.JANUS_TOKEN
        api_secret = settings.JANUS_API_SECRET
        credentials = (
            JanusCredentials(token=token, api_secret=api_secret)
            if token is not None or api_secret is not None
            else None
        )
        return WebsocketSession(
            credentials=credentials,
            keepalive_interval=settings.JANUS_KEEPALIVE_INTERVAL,
            keepalive_failures=settings.JANUS_KEEPALIVE_FAILURES,
            shutdown_timeout=settings.JANUS_SHUTDOWN_TIMEOUT,
            detach_concurrency=settings.JANUS_DETACH_CONCURRENCY,
            event_publisher=self._event_publisher,
        )

    @property
    def sessions(self) -> tuple[WebsocketSession, ...]:
        return tuple(self._sessions)

    @property
    def metrics(self) -> dict[str, int | float]:
        """Return a low-overhead snapshot of pool recovery metrics."""

        snapshot = dict(self._metrics)
        samples = int(snapshot["event_loop_lag_samples"])
        snapshot["event_loop_lag_ms_average"] = (
            float(snapshot["event_loop_lag_ms_total"]) / samples if samples else 0.0
        )
        snapshot["recoveries_in_progress"] = len(self._replacement_tasks)
        return snapshot

    @property
    def ready(self) -> bool:
        return len(self._sessions) == self._pool_size and all(item.ready for item in self._sessions)

    def get_session(self, key: str | int | None = None) -> WebsocketSession | None:
        active = tuple(session for session in self._sessions if session.ready)
        if not active:
            return None
        if key is not None:
            return active[hash(str(key)) % len(active)]
        return active[next(self._round_robin) % len(active)]

    def _bind_session(
        self,
        index: int,
        session: WebsocketSession,
        slot_generation: int,
    ) -> None:
        """Give this manager sole recovery ownership for one pool slot."""

        handler = partial(
            self._session_lost,
            index,
            session,
            slot_generation,
        )
        session.set_loss_handler(handler)
        self._loss_handlers[session] = handler

    def _unbind_session(self, session: WebsocketSession) -> None:
        handler = self._loss_handlers.pop(session, None)
        if handler is not None:
            session.remove_loss_handler(handler)

    def _session_lost(
        self,
        index: int,
        session: WebsocketSession,
        slot_generation: int,
        loss: SessionLoss,
    ) -> None:
        """Synchronously turn a transport/session notification into one task."""

        self._metrics["loss_notifications"] += 1
        self._request_recovery(
            index,
            session,
            slot_generation,
            session_generation=loss.generation,
            started_monotonic=loss.occurred_monotonic,
        )

    def _request_recovery(
        self,
        index: int,
        stale: WebsocketSession,
        slot_generation: int,
        *,
        session_generation: int | None = None,
        started_monotonic: float | None = None,
    ) -> bool:
        """Schedule at most one recovery task for an exact slot generation."""

        valid_owner = (
            not self._stopping.is_set()
            and self._monitor_task is not None
            and index < len(self._sessions)
            and index < len(self._slot_generations)
            and self._sessions[index] is stale
            and self._slot_generations[index] == slot_generation
            and (session_generation is None or session_generation == stale.generation)
        )
        current = self._replacement_tasks.get(index)
        if not valid_owner or current is not None:
            self._metrics["recovery_signals_deduplicated"] += 1
            return False

        self._metrics["recoveries_started"] += 1
        task = asyncio.create_task(
            self._replace(
                index,
                stale,
                slot_generation,
                started_monotonic=(
                    time.monotonic() if started_monotonic is None else started_monotonic
                ),
            ),
            name=f"janus-session-replacement-{index}-{slot_generation}",
        )
        self._replacement_tasks[index] = task
        self._replacement_generations[index] = slot_generation
        task.add_done_callback(partial(self._replacement_done, index, slot_generation))
        return True

    async def start(self) -> None:
        async with self._lock:
            if self._monitor_task is not None:
                return
            self._stopping.clear()
            created: list[WebsocketSession] = []
            try:
                for index in range(self._pool_size):
                    session = self._factory()
                    self._bind_session(index, session, 1)
                    created.append(session)
                results = await asyncio.gather(
                    *(session.create() for session in created),
                    return_exceptions=True,
                )
            except BaseException:
                for session in created:
                    self._unbind_session(session)
                await asyncio.gather(
                    *(session.destroy() for session in created),
                    return_exceptions=True,
                )
                raise
            failures = [result for result in results if isinstance(result, BaseException)]
            failed_sessions = [
                session
                for session, result in zip(created, results, strict=True)
                if isinstance(result, BaseException)
            ]
            if failed_sessions:
                await asyncio.gather(
                    *(session.destroy() for session in failed_sessions),
                    return_exceptions=True,
                )
            if failures and self._fail_fast:
                for session in created:
                    self._unbind_session(session)
                await asyncio.gather(
                    *(session.destroy() for session in created), return_exceptions=True
                )
                raise RuntimeError(
                    f"Could not start {len(failures)} of {self._pool_size} Janus sessions"
                ) from failures[0]
            if failures:
                logger.error(
                    "Janus session pool degraded",
                    "Some initial Janus sessions could not be activated",
                    context={
                        "failed_sessions": len(failures),
                        "pool_size": self._pool_size,
                    },
                )
            self._sessions = created
            self._slot_generations = [1] * len(created)
            self._monitor_task = asyncio.create_task(
                self._monitor(), name="janus-session-pool-monitor"
            )
            # Degraded-start failures are handed to the same single recovery
            # owner immediately; the monitor remains only a safety net.
            for index, session in enumerate(created):
                if not session.ready:
                    self._request_recovery(index, session, 1)

    async def stop(self) -> None:
        async with self._lock:
            self._stopping.set()
            monitor, self._monitor_task = self._monitor_task, None
            if monitor is not None and monitor is not asyncio.current_task():
                monitor.cancel()
            replacements, self._replacement_tasks = (
                tuple(self._replacement_tasks.values()),
                {},
            )
            for replacement in replacements:
                replacement.cancel()
            sessions, self._sessions = tuple(self._sessions), []
            self._replacement_generations.clear()
            self._slot_generations.clear()
            for session in tuple(self._loss_handlers):
                self._unbind_session(session)
        # Never wait for a task while holding the manager lock: replacement
        # tasks acquire it to commit or relinquish ownership.
        tasks = tuple(
            task
            for task in (monitor, *replacements)
            if task is not None and task is not asyncio.current_task()
        )
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        cleanup_tasks = tuple(self._cleanup_tasks.values())
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            self._cleanup_tasks.clear()
        if sessions:
            await asyncio.gather(
                *(session.destroy() for session in sessions), return_exceptions=True
            )

    async def _monitor(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._monitor_interval
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(max(0.0, deadline - loop.time()))
                observed = loop.time()
                lag_ms = max(0.0, (observed - deadline) * 1000)
                self._metrics["event_loop_lag_samples"] += 1
                self._metrics["event_loop_lag_ms_total"] += lag_ms
                self._metrics["event_loop_lag_ms_last"] = lag_ms
                self._metrics["event_loop_lag_ms_max"] = max(
                    float(self._metrics["event_loop_lag_ms_max"]), lag_ms
                )
                deadline = observed + self._monitor_interval
                for index, session in enumerate(tuple(self._sessions)):
                    if session.ready:
                        continue
                    if session.state not in {SessionState.LOST, SessionState.CLOSED}:
                        session._invalidate("manager health sweep detected an unready session")
                    if index < len(self._slot_generations):
                        self._request_recovery(
                            index,
                            session,
                            self._slot_generations[index],
                        )
        except asyncio.CancelledError:
            raise

    def _replacement_done(
        self,
        index: int,
        slot_generation: int,
        task: asyncio.Task[None],
    ) -> None:
        if self._replacement_tasks.get(index) is task:
            self._replacement_tasks.pop(index, None)
        if self._replacement_generations.get(index) == slot_generation:
            self._replacement_generations.pop(index, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "Janus recovery task failed",
                "A session replacement task terminated unexpectedly",
                context={
                    "error_type": type(error).__name__,
                    "pool_index": index,
                    "recovery_generation": slot_generation,
                },
                exc_info=error,
            )
        # A freshly committed replacement can itself be lost while the prior
        # generation is still cleaning up. Re-check here so that deduplicating
        # its notification cannot strand the slot until the next health sweep.
        if (
            not self._stopping.is_set()
            and self._monitor_task is not None
            and index < len(self._sessions)
            and index < len(self._slot_generations)
        ):
            current = self._sessions[index]
            if not current.ready:
                self._request_recovery(
                    index,
                    current,
                    self._slot_generations[index],
                )

    def _track_cleanup(
        self,
        index: int,
        session: WebsocketSession,
        *,
        name: str,
    ) -> asyncio.Task[None]:
        existing = self._cleanup_tasks.get(index)
        if existing is not None and not existing.done():
            raise RuntimeError(f"cleanup already active for Janus pool slot {index}")

        async def cleanup() -> None:
            await session.destroy()

        task = asyncio.create_task(cleanup(), name=name)
        self._cleanup_tasks[index] = task
        task.add_done_callback(partial(self._cleanup_done, index))
        return task

    def _cleanup_done(self, index: int, task: asyncio.Task[None]) -> None:
        if self._cleanup_tasks.get(index) is task:
            self._cleanup_tasks.pop(index, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.warning(
                "Janus session cleanup failed",
                "Could not fully clean up a replaced Janus session",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _replace(
        self,
        index: int,
        stale: WebsocketSession,
        slot_generation: int,
        *,
        started_monotonic: float,
    ) -> None:
        delay = self._restart_backoff
        while not self._stopping.is_set():
            replacement: WebsocketSession | None = None
            committed = False
            try:
                if (
                    index >= len(self._sessions)
                    or index >= len(self._slot_generations)
                    or self._sessions[index] is not stale
                    or self._slot_generations[index] != slot_generation
                ):
                    return
                self._metrics["recovery_attempts"] += 1
                replacement = self._factory()
                next_generation = slot_generation + 1
                self._bind_session(index, replacement, next_generation)
                await replacement.create()
                if not replacement.ready:
                    raise JanusConnectionClosed(
                        "replacement session became unavailable during activation"
                    )
                async with self._lock:
                    if (
                        not self._stopping.is_set()
                        and index < len(self._sessions)
                        and index < len(self._slot_generations)
                        and self._sessions[index] is stale
                        and self._slot_generations[index] == slot_generation
                        and replacement.ready
                    ):
                        self._unbind_session(stale)
                        self._sessions[index] = replacement
                        self._slot_generations[index] = next_generation
                        committed = True
                if not committed:
                    return

                duration_ms = max(
                    0.0,
                    (time.monotonic() - started_monotonic) * 1000,
                )
                self._metrics["recoveries_completed"] += 1
                self._metrics["recovery_duration_ms_total"] += duration_ms
                self._metrics["last_recovery_duration_ms"] = duration_ms

                stale_cleanup = self._track_cleanup(
                    index,
                    stale,
                    name=(f"janus-stale-session-cleanup-{index}-{slot_generation}"),
                )
                # Keep cleanup alive if this replacement task is cancelled
                # concurrently with manager shutdown.
                await asyncio.gather(asyncio.shield(stale_cleanup), return_exceptions=True)
                logger.info(
                    "Janus session recovered",
                    "Replaced one unavailable session pool slot",
                    context={
                        "duration_ms": round(duration_ms, 3),
                        "pool_index": index,
                        "recovery_generation": slot_generation,
                    },
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._metrics["recovery_failures"] += 1
                logger.error(
                    "Janus recovery attempt failed",
                    "Could not activate a replacement session; retrying",
                    context={
                        "backoff_seconds": delay,
                        "error_type": type(exc).__name__,
                        "pool_index": index,
                        "recovery_generation": slot_generation,
                    },
                    exc_info=exc,
                )
            finally:
                if replacement is not None and not committed:
                    self._unbind_session(replacement)
                    cleanup = asyncio.create_task(replacement.destroy())
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        await asyncio.gather(cleanup, return_exceptions=True)
                        raise
                    except Exception:
                        logger.warning(
                            "Janus replacement cleanup failed",
                            "Could not clean up an uncommitted Janus replacement",
                            exc_info=True,
                        )

            if self._stopping.is_set():
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)

    async def __aenter__(self) -> JanusSessionManager:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.stop()


def get_session(session_id: JanusId | None = None) -> WebsocketSession:
    """Compatibility constructor; unlike 2.x it always returns a new instance."""

    return WebsocketSession(session_id=session_id)
