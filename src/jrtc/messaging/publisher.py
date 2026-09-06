"""Bounded, ordered asynchronous publication of Janus WebRTC events."""

from __future__ import annotations

import asyncio
import enum
import hashlib
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, cast

from broka import Broker, DeliveryMode, Destination, MetricsProvider, PublishOptions
from logvista import VisualLogger, get_logger

from jrtc.messaging.constants import (
    ADMISSION_TOTAL,
    COALESCED_TOTAL,
    DEFAULT_PHYSICAL_ROUTE,
    DROPPED_TOTAL,
    JANUS_EVENT_ROUTES,
    JANUS_LOGICAL_PATTERN,
    PUBLISH_FAILURES_TOTAL,
    PUBLISH_LATENCY_SECONDS,
    PUBLISH_RETRIES_TOTAL,
    PUBLISHED_TOTAL,
    QUEUE_DEPTH,
    QUEUE_LATENCY_SECONDS,
)
from jrtc.messaging.metrics import LogVistaMetrics
from jrtc.models import JanusResponse
from jrtc.models.common import JanusId, validate_janus_id

type JanusIdentifier = JanusId
type EventClassifier = Callable[[JanusResponse], "EventPriority"]


class EventPriority(enum.StrEnum):
    """Generic delivery importance used by bounded ingress overflow policy."""

    PROTECTED = "protected"
    NORMAL = "normal"
    COALESCIBLE = "coalescible"


_PROTECTED_TYPES = frozenset({"detached", "hangup", "timeout"})
_COALESCIBLE_TYPES = frozenset({"media", "slowlink"})


def default_event_classifier(response: JanusResponse) -> EventPriority:
    """Classify Janus events without embedding plugin/application semantics."""

    if response.janus in _PROTECTED_TYPES:
        return EventPriority.PROTECTED
    if response.janus in _COALESCIBLE_TYPES:
        return EventPriority.COALESCIBLE
    return EventPriority.NORMAL


class _AdmissionSlots(asyncio.BoundedSemaphore):
    """Bounded semaphore with an event-loop-local non-waiting acquisition."""

    def acquire_nowait(self) -> bool:
        # asyncio intentionally has no public non-waiting semaphore method.
        # This subclass owns the counter and is only used from one event loop;
        # ``locked`` also respects already-waiting acquirers on supported
        # Python versions, so the synchronous path cannot jump the wait queue.
        if self.locked():
            return False
        self._value -= 1
        return True


@dataclass(slots=True)
class EventIngressEnvelope:
    """Lightweight event reference admitted before broker serialization."""

    route: str
    event: JanusResponse
    ordering_key: str
    admitted_at: float
    session_id: JanusIdentifier | None
    handle_id: JanusIdentifier | None
    janus_type: str
    priority: EventPriority
    coalesce_key: tuple[object, ...] | None
    sequence: int


_QueuedEvent = EventIngressEnvelope


_STOP = object()


class JanusEventPublisher:
    """Decouple transport receive loops from a Broka backend.

    :meth:`try_admit` is the transport-safe ingress API: it never waits and it
    retains only a lightweight response reference. :meth:`admit` keeps the
    historical finite capacity wait for non-hot-path callers. Accepted items
    consume one global bounded slot and are assigned to a deterministic worker
    shard by session/sender, preserving order for that key. Serialization and
    broker work happen only in the fixed worker pool.

    When full, coalescible events replace an already queued event with the same
    generic key (latest state wins). Protected events may evict the globally
    oldest queued non-protected event; they are rejected only when all capacity
    is in flight or protected. Normal events reject newest. No policy blocks
    the caller and every decision is observable through metrics/statistics.
    """

    def __init__(
        self,
        broker: Broker,
        *,
        physical_route: str | None = None,
        workers: int = 4,
        queue_capacity: int = 1024,
        admission_timeout: float = 0.05,
        publish_timeout: float = 5.0,
        max_publish_retries: int = 0,
        retry_backoff: float = 0.05,
        delivery_mode: DeliveryMode | str | None = None,
        owns_broker: bool = True,
        classifier: EventClassifier | None = None,
        metrics: MetricsProvider | None = None,
        logger: VisualLogger | None = None,
    ) -> None:
        if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
            raise ValueError("workers must be a positive integer")
        if (
            isinstance(queue_capacity, bool)
            or not isinstance(queue_capacity, int)
            or queue_capacity < 1
        ):
            raise ValueError("queue_capacity must be a positive integer")
        if not math.isfinite(admission_timeout) or admission_timeout <= 0:
            raise ValueError("admission_timeout must be finite and greater than zero")
        if not math.isfinite(publish_timeout) or publish_timeout <= 0:
            raise ValueError("publish_timeout must be finite and greater than zero")
        if (
            isinstance(max_publish_retries, bool)
            or not isinstance(max_publish_retries, int)
            or max_publish_retries < 0
        ):
            raise ValueError("max_publish_retries must be a non-negative integer")
        if not math.isfinite(retry_backoff) or retry_backoff <= 0:
            raise ValueError("retry_backoff must be finite and greater than zero")
        if physical_route is not None and (
            not isinstance(physical_route, str) or not physical_route.strip()
        ):
            raise ValueError("physical_route must be a non-empty string or None")

        self.broker = broker
        requested_route = None if physical_route is None else physical_route.strip()
        self.worker_count = workers
        self.queue_capacity = queue_capacity
        self.admission_timeout = float(admission_timeout)
        self.publish_timeout = float(publish_timeout)
        self.max_publish_retries = max_publish_retries
        self.retry_backoff = float(retry_backoff)
        self.owns_broker = bool(owns_broker)
        self.classifier = classifier or default_event_classifier
        self.logger = logger or get_logger("jrtc.messaging.publisher")
        broker_metrics = getattr(broker, "metrics", None)
        self.metrics = metrics or broker_metrics or LogVistaMetrics(self.logger)
        self.physical_route = self._ensure_route_mapping(requested_route)
        self.delivery_mode = (
            DeliveryMode(delivery_mode)
            if delivery_mode is not None
            else self._default_delivery_mode()
        )

        self._queues: tuple[asyncio.Queue[_QueuedEvent | object], ...] = tuple(
            asyncio.Queue(maxsize=queue_capacity) for _ in range(workers)
        )
        self._slots = _AdmissionSlots(queue_capacity)
        self._tasks: tuple[asyncio.Task[None], ...] = ()
        self._lifecycle_lock = asyncio.Lock()
        self._admission_lock = asyncio.Lock()
        self._accepting = False
        self._started = False
        self._depth = 0
        self._sequence = 0
        self._coalescible_pending: dict[tuple[object, ...], _QueuedEvent] = {}
        self._statistics = {
            "accepted": 0,
            "rejected": 0,
            "coalesced": 0,
            # No durable spool is configured by core. Keeping the zero-valued
            # counter explicit makes that delivery policy observable.
            "spooled": 0,
            "protected_evictions": 0,
            "published": 0,
            "publish_errors": 0,
            "publish_retries": 0,
        }
        with suppress(Exception):
            self.metrics.set_gauge(QUEUE_DEPTH, 0)

    @property
    def running(self) -> bool:
        """Return whether the publisher currently accepts events."""

        return self._started and self._accepting

    @property
    def queue_depth(self) -> int:
        """Return the number of admitted items not yet completed."""

        return self._depth

    @property
    def statistics(self) -> dict[str, int]:
        """Return low-overhead ingress/worker counters for load tests."""

        return dict(self._statistics)

    async def start(self) -> None:
        """Start the owned broker, then the ordered publisher workers."""

        async with self._lifecycle_lock:
            if self._started:
                return
            broker_start_attempted = False
            tasks: list[asyncio.Task[None]] = []
            try:
                if self.owns_broker:
                    broker_start_attempted = True
                    await self.broker.startup()
                for index, queue in enumerate(self._queues):
                    tasks.append(
                        asyncio.create_task(
                            self._worker(index, queue),
                            name=f"janus-event-publisher-{index}",
                        )
                    )
                self._tasks = tuple(tasks)
                self._started = True
                async with self._admission_lock:
                    self._accepting = True
            except BaseException:
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                self._tasks = ()
                self._started = False
                self._accepting = False
                if broker_start_attempted:
                    with suppress(Exception):
                        await self.broker.shutdown()
                raise
            self.logger.debug(
                "Publisher lifecycle",
                "Janus event publisher started",
                context={
                    "delivery_mode": self.delivery_mode.value,
                    "queue_capacity": self.queue_capacity,
                    "workers": self.worker_count,
                },
            )

    async def stop(
        self,
        *,
        drain: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Stop admission, optionally drain, and shut down an owned broker.

        A finite timeout cancels remaining worker activity and accounts queued
        items as dropped. Shutdown remains best-effort and idempotent.
        """

        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be finite and greater than zero")
        async with self._lifecycle_lock:
            if not self._started:
                return
            # Serialize the admission boundary: an item is either fully queued
            # before draining starts or observes the stopped state and is
            # rejected. No item can be placed behind a worker stop marker.
            async with self._admission_lock:
                self._accepting = False
            deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
            timed_out = False
            try:
                if drain:
                    await self._wait_for_queues(deadline)
                else:
                    self._discard_queued("shutdown")
                for queue in self._queues:
                    queue.put_nowait(_STOP)
                await self._wait_for_tasks(deadline)
            except TimeoutError:
                timed_out = True
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
                self._discard_queued("shutdown-timeout")
            except BaseException:
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
                self._discard_queued("shutdown-cancelled")
                raise
            finally:
                self._tasks = ()
                self._started = False
                self._accepting = False
                if self.owns_broker:
                    try:
                        await self.broker.shutdown()
                    except Exception as exc:
                        with suppress(Exception):
                            self.metrics.increment(
                                PUBLISH_FAILURES_TOTAL,
                                labels={"result": "shutdown"},
                            )
                        self.logger.error(
                            "Broker shutdown failed",
                            "Broka failed during Janus publisher cleanup",
                            context={"error_type": type(exc).__name__},
                            exc_info=exc,
                        )
                self.logger.debug(
                    "Publisher lifecycle",
                    "Janus event publisher stopped",
                    context={"drained": bool(drain and not timed_out), "timed_out": timed_out},
                )

    async def admit(
        self,
        response: JanusResponse,
        *,
        session_id: JanusIdentifier | None = None,
        sender: JanusIdentifier | None = None,
    ) -> bool:
        """Wait briefly for capacity and admit an event outside hot paths.

        This compatibility API may wait for at most ``admission_timeout``.
        Transport readers must use :meth:`try_admit`, which never awaits.
        Serialization remains deferred to a publisher worker in both cases.
        """

        try:
            item = self._make_envelope(response, session_id=session_id, sender=sender)
        except Exception as exc:
            self._record_invalid(response, exc)
            return False
        if not self._accepting:
            self._record_drop("not-running", item.janus_type, item.priority)
            return False
        acquired = False
        stopped = False
        try:
            async with asyncio.timeout(self.admission_timeout):
                await self._slots.acquire()
                acquired = True
                async with self._admission_lock:
                    if not self._accepting:
                        stopped = True
                    else:
                        self._enqueue_reserved(item)
                        acquired = False
        except TimeoutError:
            if acquired:
                self._slots.release()
            if self._try_coalesce(item):
                return False
            if item.priority is EventPriority.PROTECTED and self._evict_for_protected(item):
                self._record_accepted(item)
                return True
            self._record_admission(item, "timeout")
            self._record_drop("admission-timeout", item.janus_type, item.priority)
            return False
        except asyncio.CancelledError:
            if acquired:
                self._slots.release()
            raise
        except Exception as exc:
            if acquired:
                self._slots.release()
            self._record_invalid(response, exc)
            return False

        if stopped:
            self._slots.release()
            self._record_drop("stopping", item.janus_type, item.priority)
            return False
        self._record_accepted(item)
        return True

    def try_admit(
        self,
        response: JanusResponse,
        *,
        session_id: JanusIdentifier | None = None,
        sender: JanusIdentifier | None = None,
    ) -> bool:
        """Attempt immediate, non-serializing event ingress.

        ``True`` means the exact response reference owns one bounded queue
        slot. ``False`` covers unsupported/stopped/invalid input, newest-drop,
        or latest-state coalescing. The method contains no await point and is
        safe on the publisher's owning event loop.
        """

        try:
            item = self._make_envelope(response, session_id=session_id, sender=sender)
        except Exception as exc:
            self._record_invalid(response, exc)
            return False
        if not self._accepting:
            self._record_drop("not-running", item.janus_type, item.priority)
            return False
        acquire_nowait = getattr(self._slots, "acquire_nowait", None)
        acquired = bool(acquire_nowait()) if callable(acquire_nowait) else False
        if not acquired:
            if self._try_coalesce(item):
                return False
            if item.priority is EventPriority.PROTECTED and self._evict_for_protected(item):
                self._record_accepted(item)
                return True
            self._record_admission(item, "full")
            self._record_drop("queue-full", item.janus_type, item.priority)
            return False
        try:
            if not self._accepting:
                self._slots.release()
                self._record_drop("stopping", item.janus_type, item.priority)
                return False
            self._enqueue_reserved(item)
        except Exception as exc:
            self._slots.release()
            self._record_invalid(response, exc)
            return False
        self._record_accepted(item)
        return True

    def _make_envelope(
        self,
        response: JanusResponse,
        *,
        session_id: JanusIdentifier | None,
        sender: JanusIdentifier | None,
    ) -> EventIngressEnvelope:
        route = JANUS_EVENT_ROUTES.get(response.janus)
        if route is None:
            raise ValueError(f"unsupported Janus event type {response.janus!r}")
        priority = self.classifier(response)
        if not isinstance(priority, EventPriority):
            priority = EventPriority(priority)
        effective_session = session_id if session_id is not None else response.session_id
        effective_sender = sender if sender is not None else response.sender
        ordering_key = self._ordering_key(effective_session, effective_sender)
        self._sequence += 1
        coalesce_key = (
            self._coalesce_key(response, ordering_key)
            if priority is EventPriority.COALESCIBLE
            else None
        )
        return EventIngressEnvelope(
            route=route,
            event=response,
            ordering_key=ordering_key,
            admitted_at=time.perf_counter(),
            session_id=effective_session,
            handle_id=effective_sender,
            janus_type=response.janus,
            priority=priority,
            coalesce_key=coalesce_key,
            sequence=self._sequence,
        )

    @staticmethod
    def _coalesce_key(response: JanusResponse, ordering_key: str) -> tuple[object, ...]:
        return (
            response.janus,
            ordering_key,
            getattr(response, "type", None),
            getattr(response, "mid", None),
            getattr(response, "uplink", None),
        )

    def _enqueue_reserved(self, item: _QueuedEvent) -> None:
        shard = self._shard(item.ordering_key)
        self._queues[shard].put_nowait(item)
        self._depth += 1
        if item.coalesce_key is not None:
            self._coalescible_pending[item.coalesce_key] = item

    def _try_coalesce(self, item: _QueuedEvent) -> bool:
        if item.coalesce_key is None:
            return False
        existing = self._coalescible_pending.get(item.coalesce_key)
        if existing is None:
            return False
        existing.event = item.event
        existing.admitted_at = item.admitted_at
        existing.session_id = item.session_id
        existing.handle_id = item.handle_id
        self._statistics["coalesced"] += 1
        self._record_admission(item, "coalesced")
        with suppress(Exception):
            self.metrics.increment(
                COALESCED_TOTAL,
                labels={"janus_type": item.janus_type},
            )
        return True

    def _evict_for_protected(self, protected: _QueuedEvent) -> bool:
        oldest: (
            tuple[
                int,
                asyncio.Queue[_QueuedEvent | object],
                deque[_QueuedEvent | object],
                _QueuedEvent,
            ]
            | None
        ) = None
        for queue in self._queues:
            storage = cast(
                deque[_QueuedEvent | object],
                cast(Any, queue)._queue,
            )
            for candidate in tuple(storage):
                if not isinstance(candidate, EventIngressEnvelope):
                    continue
                if candidate.priority is EventPriority.PROTECTED:
                    continue
                if oldest is None or candidate.sequence < oldest[0]:
                    oldest = (candidate.sequence, queue, storage, candidate)
        if oldest is None:
            return False
        _sequence, queue, storage, evicted = oldest
        storage.remove(evicted)
        queue.task_done()
        if (
            evicted.coalesce_key is not None
            and self._coalescible_pending.get(evicted.coalesce_key) is evicted
        ):
            self._coalescible_pending.pop(evicted.coalesce_key, None)
        # Reuse the evicted item's global slot: depth and semaphore counts do
        # not change, while the destination queue's unfinished count does.
        self._enqueue_reserved(protected)
        self._depth -= 1
        self._statistics["protected_evictions"] += 1
        self._record_drop("protected-eviction", evicted.janus_type, evicted.priority)
        return True

    def _record_accepted(self, item: _QueuedEvent) -> None:
        self._statistics["accepted"] += 1
        self._record_admission(item, "accepted")
        with suppress(Exception):
            self.metrics.set_gauge(QUEUE_DEPTH, self._depth)

    def _record_admission(self, item: _QueuedEvent, result: str) -> None:
        with suppress(Exception):
            self.metrics.increment(
                ADMISSION_TOTAL,
                labels={
                    "janus_type": item.janus_type,
                    "priority": item.priority.value,
                    "result": result,
                },
            )

    def _record_invalid(self, response: JanusResponse, exc: Exception) -> None:
        response_type = getattr(response, "janus", None)
        safe_type = (
            response_type
            if isinstance(response_type, str) and response_type in JANUS_EVENT_ROUTES
            else "unknown"
        )
        with suppress(Exception):
            self.metrics.increment(
                ADMISSION_TOTAL,
                labels={"janus_type": safe_type, "result": "invalid"},
            )
        self._record_drop("invalid", safe_type)
        self.logger.error(
            "Event admission failed",
            "A Janus response could not be admitted for publication",
            context={"error_type": type(exc).__name__, "janus_type": safe_type},
            exc_info=exc,
        )

    def _ensure_route_mapping(self, requested_route: str | None) -> str:
        """Guarantee and return one exact destination for every Janus route."""

        router = getattr(self.broker, "router", None)
        destinations = getattr(router, "destinations", None)
        map_destination = getattr(router, "map_destination", None)
        if not callable(destinations) or not callable(map_destination):
            # Lightweight broker fakes used by applications/tests may implement
            # only lifecycle and publish. A real Broka Broker always has Router.
            return requested_route or DEFAULT_PHYSICAL_ROUTE

        resolved = {
            logical_route: tuple(destinations(logical_route))
            for logical_route in JANUS_EVENT_ROUTES.values()
        }
        if not any(resolved.values()):
            physical_route = requested_route or DEFAULT_PHYSICAL_ROUTE
            map_destination(
                JANUS_LOGICAL_PATTERN,
                Destination(physical_route),
            )
            return physical_route

        configured_names = {
            destination.name
            for route_destinations in resolved.values()
            for destination in route_destinations
        }
        configured_route = next(iter(configured_names)) if len(configured_names) == 1 else None
        physical_route = requested_route or configured_route or DEFAULT_PHYSICAL_ROUTE

        mismatched = {
            logical_route: tuple(destination.name for destination in route_destinations)
            for logical_route, route_destinations in resolved.items()
            if tuple(destination.name for destination in route_destinations) != (physical_route,)
        }
        if mismatched:
            raise ValueError(
                "broker routes must map every janus.* event to the single configured "
                f"physical destination {physical_route!r}"
            )
        return physical_route

    async def _worker(
        self,
        shard: int,
        queue: asyncio.Queue[_QueuedEvent | object],
    ) -> None:
        while True:
            item = await queue.get()
            if item is _STOP:
                queue.task_done()
                return
            assert isinstance(item, _QueuedEvent)
            if (
                item.coalesce_key is not None
                and self._coalescible_pending.get(item.coalesce_key) is item
            ):
                self._coalescible_pending.pop(item.coalesce_key, None)
            route_type = item.janus_type
            started = time.perf_counter()
            try:
                with suppress(Exception):
                    self.metrics.observe(
                        QUEUE_LATENCY_SECONDS,
                        max(0.0, time.perf_counter() - item.admitted_at),
                        labels={"janus_type": route_type},
                    )
                # Full response normalization is deliberately worker-only.
                payload = item.event.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                )
                result = await self._publish_with_retries(item, payload)
                if bool(getattr(result, "accepted", True)):
                    self._statistics["published"] += 1
                    with suppress(Exception):
                        self.metrics.increment(
                            PUBLISHED_TOTAL,
                            labels={
                                "janus_type": route_type,
                                "priority": item.priority.value,
                            },
                        )
                else:
                    self._statistics["publish_errors"] += 1
                    with suppress(Exception):
                        self.metrics.increment(
                            PUBLISH_FAILURES_TOTAL,
                            labels={"janus_type": route_type, "result": "rejected"},
                        )
            except asyncio.CancelledError:
                self._record_drop("worker-cancelled", route_type, item.priority)
                raise
            except Exception as exc:
                self._statistics["publish_errors"] += 1
                with suppress(Exception):
                    self.metrics.increment(
                        PUBLISH_FAILURES_TOTAL,
                        labels={"janus_type": route_type, "result": "error"},
                    )
                self.logger.error(
                    "Event publication failed",
                    "Broka failed to publish an admitted Janus response",
                    context={
                        "error_type": type(exc).__name__,
                        "janus_type": route_type,
                        "shard": shard,
                    },
                    exc_info=exc,
                )
            finally:
                with suppress(Exception):
                    self.metrics.observe(
                        PUBLISH_LATENCY_SECONDS,
                        max(0.0, time.perf_counter() - started),
                        labels={"janus_type": route_type},
                    )
                self._complete_item(queue)

    async def _publish_with_retries(
        self,
        item: _QueuedEvent,
        payload: dict[str, object],
    ) -> object:
        for attempt in range(self.max_publish_retries + 1):
            try:
                async with asyncio.timeout(self.publish_timeout):
                    return await self.broker.publish(
                        payload,
                        route=item.route,
                        options=PublishOptions(
                            delivery_mode=self.delivery_mode,
                            timeout=self.publish_timeout,
                            partition_key=item.ordering_key,
                            ordering_key=item.ordering_key,
                        ),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt >= self.max_publish_retries:
                    raise
                self._statistics["publish_retries"] += 1
                with suppress(Exception):
                    self.metrics.increment(
                        PUBLISH_RETRIES_TOTAL,
                        labels={
                            "janus_type": item.janus_type,
                            "attempt": str(attempt + 1),
                        },
                    )
                await asyncio.sleep(self.retry_backoff * (2**attempt))
        raise AssertionError("bounded publisher retry loop did not terminate")

    def _complete_item(self, queue: asyncio.Queue[_QueuedEvent | object]) -> None:
        queue.task_done()
        self._slots.release()
        self._depth = max(0, self._depth - 1)
        with suppress(Exception):
            self.metrics.set_gauge(QUEUE_DEPTH, self._depth)

    def _discard_queued(self, reason: str) -> None:
        for queue in self._queues:
            while True:
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is not _STOP:
                    if isinstance(item, _QueuedEvent):
                        route_type = item.janus_type
                        priority = item.priority
                        if (
                            item.coalesce_key is not None
                            and self._coalescible_pending.get(item.coalesce_key) is item
                        ):
                            self._coalescible_pending.pop(item.coalesce_key, None)
                    else:
                        route_type = "unknown"
                        priority = None
                    self._record_drop(reason, route_type, priority)
                    self._slots.release()
                    self._depth = max(0, self._depth - 1)
                queue.task_done()
        self._coalescible_pending.clear()
        with suppress(Exception):
            self.metrics.set_gauge(QUEUE_DEPTH, self._depth)

    async def _wait_for_queues(self, deadline: float | None) -> None:
        await self._with_deadline(
            asyncio.gather(*(queue.join() for queue in self._queues)),
            deadline,
        )

    async def _wait_for_tasks(self, deadline: float | None) -> None:
        await self._with_deadline(asyncio.gather(*self._tasks), deadline)

    @staticmethod
    async def _with_deadline(
        awaitable: Awaitable[object],
        deadline: float | None,
    ) -> None:
        if deadline is None:
            await awaitable
            return
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            if hasattr(awaitable, "cancel"):
                awaitable.cancel()
            raise TimeoutError
        async with asyncio.timeout(remaining):
            await awaitable

    def _default_delivery_mode(self) -> DeliveryMode:
        config = getattr(self.broker, "config", None)
        engine = getattr(config, "engine", None)
        if engine == "redis":
            assert config is not None
            settings = config.engine_settings("redis")
            if str(settings.get("mode", "streams")).casefold() == "pubsub":
                return DeliveryMode.AT_MOST_ONCE
        return DeliveryMode.AT_LEAST_ONCE

    @staticmethod
    def _ordering_key(
        session_id: JanusIdentifier | None,
        sender: JanusIdentifier | None,
    ) -> str:
        session = (
            "-" if session_id is None else str(validate_janus_id(session_id, name="session_id"))
        )
        handle = "-" if sender is None else str(validate_janus_id(sender, name="sender"))
        return f"{session}:{handle}"

    def _shard(self, ordering_key: str) -> int:
        digest = hashlib.blake2s(ordering_key.encode("utf-8"), digest_size=4).digest()
        return int.from_bytes(digest, "big") % self.worker_count

    def _record_drop(
        self,
        reason: str,
        janus_type: str,
        priority: EventPriority | None = None,
    ) -> None:
        self._statistics["rejected"] += 1
        labels = {"janus_type": janus_type, "result": reason}
        if priority is not None:
            labels["priority"] = priority.value
        with suppress(Exception):
            self.metrics.increment(
                DROPPED_TOTAL,
                labels=labels,
            )

    async def __aenter__(self) -> JanusEventPublisher:
        await self.start()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.stop(drain=True)


__all__ = [
    "EventClassifier",
    "EventIngressEnvelope",
    "EventPriority",
    "JanusEventPublisher",
    "JanusIdentifier",
    "default_event_classifier",
]
