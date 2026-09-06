"""Generic Janus plugin-handle foundation.

Concrete named plugins live in independent distributions.  This module knows
only how to attach a handle, send an opaque validated body, route events, and
clean up local resources.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, Self, TypedDict, cast, get_origin

from pydantic import BaseModel

from jrtc.core.exceptions import JanusConnectionClosed, PluginLoadError
from jrtc.lib.registry import Registry
from jrtc.models import JanusResponse
from jrtc.models.base import Jsep
from jrtc.models.common import JanusId, validate_janus_id
from jrtc.models.request import (
    HangupRequest,
    PluginMessageRequest,
    TrickleCandidate,
    TrickleMessageRequest,
)

logger = logging.getLogger(__name__)

Listener = Callable[[Any], Awaitable[Any] | Any]


@dataclass(slots=True)
class _QueuedEmission:
    """One local ``emit`` call admitted to the per-handle worker.

    Futures preserve the useful part of the historical fire-and-forget API
    without creating one task per callback.  The containing queue is bounded,
    and the single per-handle worker invokes callbacks sequentially.
    """

    callbacks: tuple[Listener, ...]
    payload: Any
    outcomes: tuple[asyncio.Future[Any], ...]


class PluginOptions(TypedDict, total=False):
    identifier: str
    plugin_id: JanusId
    session: Any
    on_event: Listener
    event_queue_size: int


def _plugin_body(value: BaseModel | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        payload = value.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
            exclude_unset=True,
        )
        # Protocol discriminators are commonly Literal defaults (request,
        # ptype, textroom, etc.). Preserve every Literal field without also
        # serializing unrelated mutation defaults the caller omitted.
        for field_name, field in type(value).model_fields.items():
            if (
                field_name not in {"request", "textroom"}
                and get_origin(field.annotation) is not Literal
            ):
                continue
            serialized_name = field.serialization_alias or field.alias or field_name
            payload.setdefault(serialized_name, getattr(value, field_name))
        return payload
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError("plugin body must be a Pydantic model or mapping")


class Plugin:
    """Base class and backwards-compatible string factory for Janus plugins.

    External packages normally construct their concrete class directly, e.g.
    ``SipPlugin(session=session)``.  ``Plugin(identifier="sip", ...)`` remains
    available and lazily resolves the matching ``jrtc.plugins`` entry
    point without importing unrelated plugins.
    """

    identifier: ClassVar[str | None] = None
    name: ClassVar[str | None] = None
    registry: ClassVar[Registry[Plugin]] = Registry(entry_point_group="jrtc.plugins")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        declared_identifier = cls.__dict__.get("identifier")
        if declared_identifier:
            Plugin.registry.register(str(declared_identifier), cls)

    def __new__(cls, *args: Any, identifier: str | None = None, **kwargs: Any) -> Self:
        if cls is Plugin:
            if not identifier:
                raise TypeError("Plugin(identifier=...) requires a plugin identifier")
            concrete = cls.registry.resolve(identifier)
            if not issubclass(concrete, Plugin):
                raise PluginLoadError(
                    f"Registered object for {identifier!r} is not a Plugin subclass"
                )
            return cast(Self, object.__new__(concrete))
        return object.__new__(cls)

    def __init__(
        self,
        *,
        session: Any,
        plugin_id: JanusId | None = None,
        identifier: str | None = None,
        on_event: Listener | None = None,
        event_queue_size: int = 1024,
        **_: Any,
    ) -> None:
        if session is None:
            raise TypeError("plugin requires an associated Janus session")
        if event_queue_size < 1:
            raise ValueError("event_queue_size must be positive")
        self._session = session
        self._plugin_id: JanusId | None = (
            None if plugin_id is None else validate_janus_id(plugin_id, name="plugin_id")
        )
        self._listeners: dict[str, list[Listener]] = {}
        self._lifecycle_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._event_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=event_queue_size)
        self._event_task: asyncio.Task[None] | None = None
        self._dropped_events = 0
        self._closed = False
        self._handle_lost = False
        self._on_event = on_event

    @classmethod
    def list_registered(cls) -> list[str]:
        """List concrete plugin identifiers imported in this process."""

        return list(cls.registry)

    @property
    def id(self) -> int:
        if self._plugin_id is None:
            raise RuntimeError("plugin has not been attached")
        return self._plugin_id

    @property
    def session(self) -> Any:
        return self._session

    async def on(self, event: str, callback: Listener) -> None:
        if not event or not callable(callback):
            raise ValueError("event and callback are required")
        listeners = self._listeners.setdefault(event, [])
        if callback not in listeners:
            listeners.append(callback)

    async def off(self, event: str, callback: Listener) -> None:
        listeners = self._listeners.get(event)
        if listeners is None:
            return
        try:
            listeners.remove(callback)
        except ValueError:
            return
        if not listeners:
            self._listeners.pop(event, None)

    async def emit(
        self,
        event: str,
        payload: Any,
        *,
        wait: bool = False,
        timeout: float | None = None,
    ) -> list[Any]:
        """Invoke listeners without creating an unbounded task fan-out.

        Awaited emissions and transport events normally share the one bounded
        per-handle worker.  ``wait=False`` remains fire-and-forget and returns
        Future-compatible outcome handles, but no callback gets its own Task.
        A listener recursively using ``wait=True`` is run inline to avoid a
        worker waiting on itself.
        """

        callbacks = tuple(self._listeners.get(event, ()))
        if not callbacks or self._closed or self._handle_lost:
            return []

        if wait and asyncio.current_task() is self._event_task:
            return await self._invoke_listener_batch(callbacks, payload, timeout=timeout)

        loop = asyncio.get_running_loop()
        outcomes = tuple(loop.create_future() for _ in callbacks)
        for outcome in outcomes:
            outcome.add_done_callback(self._listener_outcome_done)
        emission = _QueuedEmission(callbacks=callbacks, payload=payload, outcomes=outcomes)
        if not self._admit_local_event(emission):
            self._cancel_queue_item(emission)
            return []
        if not wait:
            return list(outcomes)

        try:
            done, pending = await asyncio.wait(outcomes, timeout=timeout)
        except BaseException:
            for outcome in outcomes:
                outcome.cancel()
            raise
        for outcome in pending:
            outcome.cancel()
        return [
            outcome.result()
            for outcome in outcomes
            if outcome in done and not outcome.cancelled() and outcome.exception() is None
        ]

    @staticmethod
    async def _invoke_listener(callback: Listener, payload: Any) -> Any:
        # Synchronous listeners remain source-compatible, but run in the
        # plugin worker and therefore must be quick.  JRTC never creates a
        # thread bridge inside its async control plane.
        result = callback(payload)
        if inspect.isawaitable(result):
            return await result
        return result

    def _listener_outcome_done(self, outcome: asyncio.Future[Any]) -> None:
        if outcome.cancelled():
            return
        error = outcome.exception()
        if error is not None:
            logger.error(
                "Plugin event callback failed for handle %s",
                self._plugin_id,
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _invoke_listener_batch(
        self,
        callbacks: tuple[Listener, ...],
        payload: Any,
        *,
        timeout: float | None,
    ) -> list[Any]:
        """Run a stable callback snapshot sequentially with one deadline."""

        results: list[Any] = []
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        for callback in callbacks:
            if deadline is not None and deadline <= asyncio.get_running_loop().time():
                break
            try:
                if deadline is None:
                    result = await self._invoke_listener(callback, payload)
                else:
                    async with asyncio.timeout_at(deadline):
                        result = await self._invoke_listener(callback, payload)
            except TimeoutError:
                break
            except asyncio.CancelledError:
                task = asyncio.current_task()
                if task is not None and task.cancelling():
                    raise
                logger.warning(
                    "Plugin event callback cancelled itself for handle %s",
                    self._plugin_id,
                )
            except Exception:
                logger.exception(
                    "Plugin event callback failed for handle %s",
                    self._plugin_id,
                )
            else:
                results.append(result)
        return results

    def _dispatch_event(self, event: Any) -> None:
        """Route one event to this handle without blocking the transport loop."""

        self._admit_local_event(event)

    def _admit_local_event(self, event: Any) -> bool:
        """Admit one local event immediately using oldest-event eviction."""

        if self._closed or self._handle_lost:
            return False
        if self._event_task is None or self._event_task.done():
            self._event_task = asyncio.create_task(
                self._event_loop(),
                name=f"janus-plugin-events-{self._plugin_id or 'unbound'}",
            )
        try:
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            # Keep the newest state under overload and expose the loss.
            self._dropped_events += 1
            try:
                dropped = self._event_queue.get_nowait()
                self._event_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            else:
                self._cancel_queue_item(dropped)
            self._event_queue.put_nowait(event)
            # Overload accounting is exact; diagnostics are exponentially
            # sampled so a hot reader is not turned into a logging loop.
            if self._dropped_events & (self._dropped_events - 1) == 0:
                logger.warning(
                    "Plugin handle %s event queue overflow; dropped=%d",
                    self._plugin_id,
                    self._dropped_events,
                )
        return True

    async def _event_loop(self) -> None:
        while True:
            event = await self._event_queue.get()
            try:
                if isinstance(event, _QueuedEmission):
                    await self._deliver_queued_emission(event)
                else:
                    await self._deliver_janus_event(event)
            except asyncio.CancelledError:
                self._cancel_queue_item(event)
                raise
            finally:
                self._event_queue.task_done()
            if self._closed or self._handle_lost:
                return

    async def _deliver_queued_emission(self, emission: _QueuedEmission) -> None:
        for index, (callback, outcome) in enumerate(
            zip(emission.callbacks, emission.outcomes, strict=True)
        ):
            if self._closed or self._handle_lost:
                for remaining in emission.outcomes[index:]:
                    remaining.cancel()
                return
            if outcome.cancelled():
                continue
            try:
                result = await self._invoke_listener(callback, emission.payload)
            except asyncio.CancelledError:
                task = asyncio.current_task()
                if task is not None and task.cancelling():
                    for remaining in emission.outcomes[index:]:
                        remaining.cancel()
                    raise
                outcome.cancel()
                logger.warning(
                    "Plugin event callback cancelled itself for handle %s",
                    self._plugin_id,
                )
            except Exception as error:
                if not outcome.done():
                    outcome.set_exception(error)
            else:
                if not outcome.done():
                    outcome.set_result(result)

    async def _deliver_janus_event(self, event: Any) -> None:
        callbacks = list(self._listeners.get("event", ()))
        janus_type = getattr(event, "janus", None)
        if isinstance(janus_type, str):
            for callback in self._listeners.get(janus_type, ()):
                if callback not in callbacks:
                    callbacks.append(callback)
        if self._on_event is not None and self._on_event not in callbacks:
            callbacks.append(self._on_event)
        # Callbacks remain ordered inside the one per-handle worker.  Slow
        # application code can delay this handle, but never the transport.
        await self._invoke_listener_batch(tuple(callbacks), event, timeout=None)
        if janus_type == "detached":
            await self._invalidate_handle()

    @staticmethod
    def _cancel_queue_item(event: Any) -> None:
        if isinstance(event, _QueuedEmission):
            for outcome in event.outcomes:
                outcome.cancel()

    def _discard_pending_events(self) -> None:
        """Drop queued work and settle its outcome futures during shutdown."""

        while True:
            try:
                event = self._event_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self._cancel_queue_item(event)
            self._event_queue.task_done()

    @property
    def dropped_events(self) -> int:
        """Number of oldest events discarded after queue overflow."""

        return self._dropped_events

    # Compatibility lifecycle hooks.  Session ownership now controls actual
    # attach/detach and transport shutdown.
    def setup(self) -> None:
        return None

    def start(self) -> None:
        return None

    def stop(self) -> None:
        event_task = self._event_task
        if event_task is not None and event_task is not asyncio.current_task():
            event_task.cancel()
        self._discard_pending_events()

    def _mark_handle_lost(self) -> None:
        """Synchronously fence a handle whose owning session was lost.

        Session recovery needs the public handle ID to become unusable before
        it schedules asynchronous cleanup.  Repeated loss notifications are
        harmless; :meth:`_invalidate_handle`/``aclose`` finish cleanup later.
        """

        if self._handle_lost:
            return
        self._handle_lost = True
        self._plugin_id = None
        self.stop()

    async def attach(self, *, opaque_id: str | None = None) -> Self:
        async with self._lifecycle_lock:
            if self._closed or self._handle_lost:
                raise RuntimeError("closed plugin handles cannot be attached")
            if self._plugin_id is not None:
                registered = self.session.plugins.get(self.id)
                if registered is None:
                    self.session.plugins.register(self.id, self)
                elif registered is not self:
                    raise RuntimeError(f"Janus handle {self.id} is owned by another plugin")
                if self._event_task is None or self._event_task.done():
                    self._event_task = asyncio.create_task(
                        self._event_loop(), name=f"janus-plugin-events-{self.id}"
                    )
                if self._closed:
                    if self.session.plugins.get(self.id) is self:
                        self.session.plugins.unregister(self.id)
                    raise RuntimeError("plugin was closed while adopting its handle")
                return self
            if not self.name:
                raise TypeError(f"{type(self).__name__} must define the Janus plugin package name")
            handle_id = validate_janus_id(
                await self.session.attach(self.name, opaque_id=opaque_id),
                name="handle_id",
            )
            self._plugin_id = handle_id
            try:
                if not getattr(self.session, "ready", True):
                    raise JanusConnectionClosed(
                        "Janus session was lost while attaching the plugin handle"
                    )
                self.session.plugins.register(handle_id, self)
                if self._closed:
                    raise RuntimeError("plugin was closed while attaching its handle")
            except BaseException:
                self._plugin_id = None
                try:
                    await self.session.detach(handle_id)
                except Exception:
                    logger.exception("Failed to roll back Janus handle %s", handle_id)
                raise
            self._event_task = asyncio.create_task(
                self._event_loop(), name=f"janus-plugin-events-{self.id}"
            )
            return self

    async def detach(self) -> Any | None:
        """Detach idempotently and release all per-handle listeners."""

        async with self._lifecycle_lock:
            if self._plugin_id is None:
                await self.aclose()
                return None
            handle_id = self._plugin_id
            try:
                return await self.session.detach(handle_id)
            finally:
                self._plugin_id = None
                await self.aclose()

    async def send(
        self,
        body: BaseModel | Mapping[str, Any],
        jsep: Jsep | None = None,
        *,
        timeout: float | None = None,
        wait_for_event: bool = True,
    ) -> JanusResponse:
        if self._closed or self._plugin_id is None:
            raise RuntimeError("plugin must be attached before sending messages")
        message = PluginMessageRequest(
            session_id=self.session.id,
            handle_id=self.id,
            body=_plugin_body(body),
            jsep=jsep,
        )
        return await self.session.send(
            message,
            timeout=timeout,
            wait_for_event=wait_for_event,
        )

    async def trickle(
        self,
        candidates: TrickleCandidate | Sequence[TrickleCandidate],
        *,
        timeout: float | None = None,
    ) -> JanusResponse:
        if self._closed or self._plugin_id is None:
            raise RuntimeError("plugin must be attached before trickling ICE")
        if isinstance(candidates, TrickleCandidate):
            request = TrickleMessageRequest(
                session_id=self.session.id,
                handle_id=self.id,
                candidate=candidates,
            )
        else:
            request = TrickleMessageRequest(
                session_id=self.session.id,
                handle_id=self.id,
                candidates=list(candidates),
            )
        return await self.session.send(request, timeout=timeout, wait_for_event=False)

    async def complete_trickle(self, *, timeout: float | None = None) -> JanusResponse:
        return await self.trickle(TrickleCandidate(completed=True), timeout=timeout)

    async def hangup(self, *, timeout: float | None = None) -> JanusResponse:
        if self._closed or self._plugin_id is None:
            raise RuntimeError("plugin must be attached before hanging up")
        request = HangupRequest(session_id=self.session.id, handle_id=self.id)
        return await self.session.send(request, timeout=timeout, wait_for_event=False)

    async def aclose(self) -> None:
        """Close local listeners and tasks without issuing a Janus detach.

        Use :meth:`detach` to release the remote handle immediately. Keeping a
        locally closed handle registered lets the owning session detach it
        during orderly session shutdown.
        """

        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            current = asyncio.current_task()
            event_task, self._event_task = self._event_task, None
            if event_task is not None and event_task is not current:
                event_task.cancel()
                await asyncio.gather(event_task, return_exceptions=True)
            self._discard_pending_events()
            self._listeners.clear()

    async def _aclose(self) -> None:
        """Backward-compatible alias for subclasses overriding old cleanup hooks."""

        await self.aclose()

    async def _invalidate_handle(self) -> None:
        """Forget a server-side handle invalidated with its owning session."""

        async with self._lifecycle_lock:
            handle_id, self._plugin_id = self._plugin_id, None
            self._handle_lost = True
            if handle_id is not None and self.session.plugins.get(handle_id) is self:
                self.session.plugins.unregister(handle_id)
        await self.aclose()

    async def __aenter__(self) -> Self:
        return await self.attach()

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.detach()


# Compatibility alias for code that previously inspected the metaclass.
class PluginMeta:
    registry = Plugin.registry
