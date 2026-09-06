"""Reusable, deterministic local listeners for typed Janus responses."""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from threading import RLock

from broka import MetricsProvider
from logvista import VisualLogger, get_logger

from jrtc.messaging.constants import (
    DISPATCHABLE_JANUS_TYPES,
    LISTENER_DROPPED_TOTAL,
    LISTENER_FAILURES_TOTAL,
    LISTENER_QUEUE_DEPTH,
    LISTENER_QUEUE_LATENCY_SECONDS,
)
from jrtc.messaging.metrics import LogVistaMetrics
from jrtc.models import JanusResponse

type ResponseCallback = Callable[[JanusResponse], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class _QueuedNotification:
    event: str
    response: JanusResponse
    callbacks: tuple[ResponseCallback, ...]
    admitted_at: float


class LocalListenerRegistry:
    """Maintain ordered per-event and wildcard callbacks.

    Listener failures are isolated so one local integration cannot prevent
    transaction resolution or external publication. Cancellation is never
    swallowed.
    """

    def __init__(
        self,
        *,
        queue_capacity: int = 1024,
        metrics: MetricsProvider | None = None,
        logger: VisualLogger | None = None,
    ) -> None:
        if (
            isinstance(queue_capacity, bool)
            or not isinstance(queue_capacity, int)
            or queue_capacity < 1
        ):
            raise ValueError("queue_capacity must be a positive integer")
        self.logger = logger or get_logger("jrtc.messaging.listeners")
        self.metrics = metrics or LogVistaMetrics(self.logger)
        self._listeners: dict[str, list[ResponseCallback]] = {}
        self._lock = RLock()
        self._queue: asyncio.Queue[_QueuedNotification] = asyncio.Queue(maxsize=queue_capacity)
        self._worker: asyncio.Task[None] | None = None
        self._closed = False
        self._dropped = 0
        with suppress(Exception):
            self.metrics.set_gauge(LISTENER_QUEUE_DEPTH, 0)

    @property
    def queue_capacity(self) -> int:
        return self._queue.maxsize

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def dropped(self) -> int:
        return self._dropped

    def add(self, event: str, callback: ResponseCallback) -> None:
        """Register ``callback`` once for an exact event or ``"*"``."""

        event = self._validate_event(event)
        if not callable(callback):
            raise TypeError("listener must be callable")
        with self._lock:
            listeners = self._listeners.setdefault(event, [])
            if callback not in listeners:
                listeners.append(callback)

    def remove(self, event: str, callback: ResponseCallback) -> bool:
        """Remove a callback, returning whether it was registered."""

        event = self._validate_event(event)
        with self._lock:
            listeners = self._listeners.get(event)
            if not listeners:
                return False
            try:
                listeners.remove(callback)
            except ValueError:
                return False
            if not listeners:
                self._listeners.pop(event, None)
            return True

    def clear(self, event: str | None = None) -> None:
        """Remove listeners for one event, or all listeners when omitted."""

        with self._lock:
            if event is None:
                self._listeners.clear()
            else:
                self._listeners.pop(self._validate_event(event), None)

    def snapshot(self, event: str) -> tuple[ResponseCallback, ...]:
        """Return the exact deterministic invocation order for ``event``."""

        event = self._validate_event(event)
        with self._lock:
            callbacks = [*self._listeners.get(event, ()), *self._listeners.get("*", ())]
        unique: list[ResponseCallback] = []
        for callback in callbacks:
            if callback not in unique:
                unique.append(callback)
        return tuple(unique)

    async def notify(self, event: str, response: JanusResponse) -> None:
        """Invoke a stable listener snapshot sequentially.

        This awaited compatibility API is intended for non-hot-path callers.
        Transport dispatch uses :meth:`try_notify` instead.
        """

        event = self._validate_event(event)
        await self._invoke(event, response, self.snapshot(event))

    def try_notify(self, event: str, response: JanusResponse) -> bool:
        """Admit one callback notification without waiting for application code."""

        event = self._validate_event(event)
        callbacks = self.snapshot(event)
        if not callbacks:
            return True
        if self._closed:
            self._record_drop(event, "closed")
            return False
        notification = _QueuedNotification(
            event=event,
            response=response,
            callbacks=callbacks,
            admitted_at=time.perf_counter(),
        )
        try:
            self._queue.put_nowait(notification)
        except asyncio.QueueFull:
            # Preserve already admitted ordering and lifecycle notifications;
            # callback-only overflow rejects newest and never affects plugin or
            # broker routing of the response itself.
            self._record_drop(event, "queue-full")
            return False
        with suppress(Exception):
            self.metrics.set_gauge(LISTENER_QUEUE_DEPTH, self._queue.qsize())
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(
                self._drain(),
                name="janus-local-listener-worker",
            )
        return True

    async def _drain(self) -> None:
        while not self._closed or not self._queue.empty():
            try:
                notification = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                with suppress(Exception):
                    self.metrics.set_gauge(LISTENER_QUEUE_DEPTH, self._queue.qsize())
                    self.metrics.observe(
                        LISTENER_QUEUE_LATENCY_SECONDS,
                        max(0.0, time.perf_counter() - notification.admitted_at),
                        labels={"janus_type": self._safe_event(notification.event)},
                    )
                await self._invoke(
                    notification.event,
                    notification.response,
                    notification.callbacks,
                )
            finally:
                self._queue.task_done()

    async def _invoke(
        self,
        event: str,
        response: JanusResponse,
        callbacks: tuple[ResponseCallback, ...],
    ) -> None:
        safe_event = self._safe_event(event)
        for callback in callbacks:
            try:
                result = callback(response)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                task = asyncio.current_task()
                if task is not None and task.cancelling():
                    raise
                with suppress(Exception):
                    self.metrics.increment(
                        LISTENER_FAILURES_TOTAL,
                        labels={"janus_type": safe_event},
                    )
                self.logger.warning(
                    "Local listener cancelled",
                    "A Janus response listener cancelled its own awaitable",
                    context={"janus_type": safe_event},
                )
            except Exception as exc:
                with suppress(Exception):
                    self.metrics.increment(
                        LISTENER_FAILURES_TOTAL,
                        labels={"janus_type": safe_event},
                    )
                self.logger.error(
                    "Local listener failed",
                    "A Janus response listener raised and was isolated",
                    context={
                        "error_type": type(exc).__name__,
                        "janus_type": safe_event,
                    },
                    exc_info=exc,
                )

    async def aclose(
        self,
        *,
        drain: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Stop callback admission and deterministically settle queued work."""

        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be finite and greater than zero")
        self._closed = True
        worker = self._worker
        if drain and not self._queue.empty() and (worker is None or worker.done()):
            worker = asyncio.create_task(
                self._drain(),
                name="janus-local-listener-drain",
            )
            self._worker = worker
        if worker is not None and not worker.done():
            try:
                if drain:
                    if timeout is None:
                        await worker
                    else:
                        async with asyncio.timeout(timeout):
                            await worker
                else:
                    worker.cancel()
                    await asyncio.gather(worker, return_exceptions=True)
            except TimeoutError:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            except asyncio.CancelledError:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
                self._discard_queued("shutdown-cancelled")
                self._worker = None
                raise
        self._discard_queued("shutdown")
        self._worker = None

    def _discard_queued(self, reason: str) -> None:
        while True:
            try:
                notification = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._record_drop(notification.event, reason)
            self._queue.task_done()
        with suppress(Exception):
            self.metrics.set_gauge(LISTENER_QUEUE_DEPTH, 0)

    @staticmethod
    def _safe_event(event: str) -> str:
        return event if event in DISPATCHABLE_JANUS_TYPES else "unknown"

    def _record_drop(self, event: str, reason: str) -> None:
        self._dropped += 1
        with suppress(Exception):
            self.metrics.increment(
                LISTENER_DROPPED_TOTAL,
                labels={"janus_type": self._safe_event(event), "result": reason},
            )

    @staticmethod
    def _validate_event(event: str) -> str:
        if not isinstance(event, str) or not event.strip():
            raise ValueError("listener event must be a non-empty string")
        return event.strip()


__all__ = ["LocalListenerRegistry", "ResponseCallback"]
