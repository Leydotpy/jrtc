"""Instance-scoped Janus session foundation."""

from __future__ import annotations

import asyncio
import enum
import inspect
import math
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from dispio import Dispatcher, ExactMatcher
from logvista import get_logger

from jrtc.auth import CredentialSource, resolve_credentials
from jrtc.conf import settings
from jrtc.core.exceptions import (
    JanusConfigurationError,
    JanusConnectionClosed,
    PluginNotRegistered,
)
from jrtc.lib.manager import PluginManager
from jrtc.models import JanusRequest, JanusResponse
from jrtc.models.common import JanusId, validate_janus_id
from jrtc.models.request import AttachPluginRequest, DetachPluginRequest
from jrtc.models.response import SuccessResponse
from jrtc.transport.base import JanusTransport
from jrtc.transport.websocket import WebsocketTransportClient

if TYPE_CHECKING:
    from jrtc.messaging import JanusEventPublisher

logger = get_logger(__name__)

TransportFactory = Callable[[], JanusTransport | Awaitable[JanusTransport]]


class SessionState(enum.StrEnum):
    NEW = "new"
    CREATING = "creating"
    ACTIVE = "active"
    LOST = "lost"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class SessionLoss:
    """One immutable notification that a session generation became unusable.

    The notification intentionally carries only lifecycle metadata.  It never
    retains a Janus response, SDP, ICE candidate, or transport exception body.
    """

    generation: int
    session_id: JanusId | None
    reason: str
    occurred_monotonic: float
    error_type: str | None = None


type SessionLossHandler = Callable[[SessionLoss], None]


def _default_transport_factory(
    url: str,
    request_timeout: float,
    event_publisher: JanusEventPublisher | None,
) -> TransportFactory:
    if url.startswith(("http://", "https://")):

        def http_factory() -> JanusTransport:
            try:
                from jrtc.transport.http import HttpTransportClient
            except ImportError as exc:
                raise JanusConfigurationError(
                    "Install jrtc[http] to use the REST transport"
                ) from exc
            return HttpTransportClient(
                url,
                request_timeout=request_timeout,
                event_publisher=event_publisher,
            )

        return http_factory
    if not url.startswith(("ws://", "wss://")):
        raise JanusConfigurationError(
            "Janus URLs must use ws://, wss://, http://, or https://. "
            "Inject a JanusTransport for message-queue or Unix-socket transports."
        )

    def factory() -> WebsocketTransportClient:
        return WebsocketTransportClient(
            url,
            request_timeout=request_timeout,
            # Socket loss invalidates every Janus session and handle bound to
            # that connection.  Recreating those resources belongs to the
            # session manager; reconnecting only the socket would leave stale
            # IDs attached to a new transport generation.
            reconnect=False,
            event_publisher=event_publisher,
        )

    return factory


class AbstractBaseSession:
    """Base session with ordinary instance ownership and per-session handles."""

    def __init__(
        self,
        *,
        session_id: JanusId | None = None,
        transport: JanusTransport | None = None,
        transport_factory: TransportFactory | None = None,
        url: str | None = None,
        credentials: CredentialSource = None,
        request_timeout: float | None = None,
        event_publisher: JanusEventPublisher | None = None,
    ) -> None:
        self._session_id: JanusId | None = None
        self._claim_session_id: JanusId | None = (
            None if session_id is None else validate_janus_id(session_id, name="session_id")
        )
        self._state = SessionState.NEW
        self._transport = transport
        self._owns_transport = transport is None
        self._event_publisher = event_publisher
        self._request_timeout = float(
            request_timeout
            if request_timeout is not None
            else getattr(settings, "JANUS_REQUEST_TIMEOUT", 15.0)
        )
        if not math.isfinite(self._request_timeout) or self._request_timeout <= 0:
            raise ValueError("request_timeout must be finite and greater than zero")
        if transport is not None and transport_factory is not None:
            raise ValueError("transport and transport_factory are mutually exclusive")
        self._transport_factory: TransportFactory | None
        if transport_factory is not None:
            self._transport_factory = transport_factory
        elif transport is None:
            endpoint = str(
                url
                or getattr(
                    settings,
                    "JANUS_SESSION_URL",
                    "ws://localhost:8188/janus",
                )
            )
            self._transport_factory = _default_transport_factory(
                endpoint,
                self._request_timeout,
                event_publisher,
            )
        else:
            # An injected transport owns its endpoint semantics (AMQP, MQTT,
            # nanomsg, Unix sockets, or an application-specific transport).
            self._transport_factory = None
        self._credentials = credentials
        self._plugins: PluginManager[Any] = PluginManager()
        self._transport_listeners_registered = False
        self._setup_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._lost_session_id: JanusId | None = None
        self._generation = 0
        self._loss_handler: SessionLossHandler | None = None
        self._loss_observers: dict[object, SessionLossHandler] = {}
        # Loss invalidation is idempotent and recreation waits for cleanup, so
        # one session can own at most one generation-cleanup task.
        self._cleanup_task: asyncio.Task[None] | None = None
        self._metrics = {"session_losses": 0, "loss_observer_failures": 0}
        self._event_dispatcher = Dispatcher(name="janus.session.events")
        self._event_dispatcher.add(
            ExactMatcher("timeout"),
            self._handle_timeout,
            name="session-timeout",
        )
        self._event_dispatcher.default(
            self._route_plugin_event,
            name="session-plugin-event",
        )
        self._event_dispatcher.registry.freeze()

    @property
    def id(self) -> int:
        if self._session_id is None:
            raise RuntimeError(f"session has no active ID (state={self._state})")
        return self._session_id

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def ready(self) -> bool:
        return (
            self._state is SessionState.ACTIVE
            and self._session_id is not None
            and self._transport is not None
            and self._transport.open
        )

    @property
    def lost_session_id(self) -> JanusId | None:
        return self._lost_session_id

    @property
    def generation(self) -> int:
        """Activation-attempt generation used to fence stale loss signals."""

        return self._generation

    @property
    def metrics(self) -> dict[str, int]:
        """Return a cheap snapshot of session lifecycle counters."""

        return dict(self._metrics)

    @property
    def plugins(self) -> PluginManager[Any]:
        return self._plugins

    @property
    def transport(self) -> JanusTransport | None:
        return self._transport

    def set_loss_handler(self, handler: SessionLossHandler | None) -> None:
        """Assign the single non-blocking owner of session recovery.

        A session deliberately supports one handler rather than a fan-out of
        recovery callbacks.  This prevents two managers from independently
        replacing the same session generation.  The handler must do constant,
        synchronous work (normally scheduling one bounded manager task).
        """

        if handler is not None and not callable(handler):
            raise TypeError("session loss handler must be callable")
        if (
            handler is not None
            and self._loss_handler is not None
            and self._loss_handler is not handler
        ):
            raise RuntimeError("session already has a recovery owner")
        self._loss_handler = handler

    def remove_loss_handler(self, handler: SessionLossHandler) -> None:
        """Release recovery ownership if *handler* is still the owner."""

        if self._loss_handler is handler:
            self._loss_handler = None

    def add_loss_observer(self, observer: SessionLossHandler) -> Callable[[], None]:
        """Observe future losses without taking ownership of recovery.

        At most 16 registrations are retained per session. Callbacks run after
        synchronous handle fencing and the manager notification, on the owner
        loop. They must do constant-time, nonblocking work: enqueue metadata in
        a bounded application worker, never perform I/O or replace sessions.
        The returned unsubscribe function is idempotent. No losses are replayed.
        """

        if not callable(observer) or inspect.iscoroutinefunction(observer):
            raise TypeError("session loss observer must be a synchronous callable")
        if len(self._loss_observers) >= 16:
            raise RuntimeError("session loss observer limit reached")
        key = object()
        self._loss_observers[key] = observer

        def unsubscribe() -> None:
            self._loss_observers.pop(key, None)

        return unsubscribe

    async def _setup(self) -> None:
        async with self._setup_lock:
            if self._transport is None:
                if self._transport_factory is None:
                    raise JanusConfigurationError("no Janus transport is configured")
                value = self._transport_factory()
                self._transport = await value if inspect.isawaitable(value) else value
            if not all(
                hasattr(self._transport, attribute)
                for attribute in (
                    "add_close_listener",
                    "add_message_listener",
                    "open",
                    "remove_close_listener",
                    "remove_message_listener",
                    "send",
                    "start",
                    "stop",
                )
            ):
                raise TypeError("transport_factory did not return a JanusTransport")
            self._register_transport_listeners()
            if not self._transport.open:
                await self._transport.start()

    def _register_transport_listeners(self) -> None:
        if self._transport is None or self._transport_listeners_registered:
            return
        self._transport.add_message_listener(self._route_event)
        self._transport.add_close_listener(self._transport_closed)
        self._transport_listeners_registered = True

    def _unregister_transport_listeners(self) -> None:
        if self._transport is None or not self._transport_listeners_registered:
            return
        self._transport.remove_message_listener(self._route_event)
        self._transport.remove_close_listener(self._transport_closed)
        self._transport_listeners_registered = False

    def _authorized_copy(self, request: JanusRequest) -> JanusRequest:
        copy = request.model_copy()
        credentials = resolve_credentials(self._credentials)
        if credentials is not None:
            credentials.apply(copy)
        return copy

    async def send(
        self,
        data: JanusRequest,
        *,
        timeout: float | None = None,
        wait_for_event: bool = False,
    ) -> JanusResponse:
        if self._state in {SessionState.LOST, SessionState.CLOSING, SessionState.CLOSED}:
            raise JanusConnectionClosed(f"session cannot send while state={self._state}")
        await self._setup()
        assert self._transport is not None
        response = await self._transport.send(
            self._authorized_copy(data),
            timeout=self._request_timeout if timeout is None else timeout,
            wait_for_event=wait_for_event,
        )
        if self._state is SessionState.LOST:
            raise JanusConnectionClosed("session was lost while the request was in flight")
        return response

    async def attach(self, plugin: str, *, opaque_id: str | None = None) -> JanusId:
        if not self.ready:
            raise JanusConnectionClosed("session must be active before attaching a plugin")
        response = await self.send(
            AttachPluginRequest(
                session_id=self.id,
                plugin=plugin,
                opaque_id=opaque_id,
            )
        )
        if (
            not isinstance(response, SuccessResponse)
            or response.data is None
            or response.data.id is None
        ):
            raise JanusConnectionClosed(
                f"attach did not return a handle ID (janus={response.janus!r})"
            )
        if not self.ready:
            raise JanusConnectionClosed("session was lost while attaching a plugin handle")
        return response.data.id

    async def detach(self, handle_id: JanusId) -> JanusId:
        handle_id = validate_janus_id(handle_id, name="handle_id")
        if self._state in {SessionState.ACTIVE, SessionState.CLOSING}:
            try:
                request = DetachPluginRequest(session_id=self.id, handle_id=handle_id)
                if self._state is SessionState.ACTIVE:
                    await self.send(request, wait_for_event=False)
                elif self._transport is not None and self._transport.open:
                    await self._transport.send(
                        self._authorized_copy(request),
                        timeout=self._request_timeout,
                        wait_for_event=False,
                    )
            finally:
                with suppress(PluginNotRegistered):
                    self._plugins.unregister(handle_id)
        else:
            with suppress(PluginNotRegistered):
                self._plugins.unregister(handle_id)
        return handle_id

    def _route_event(self, response: JanusResponse) -> None:
        """Route one local response without involving the external subscriber path."""

        response_session_id = getattr(response, "session_id", None)
        if (
            response_session_id is not None
            and self._session_id is not None
            and response_session_id != self._session_id
        ):
            return
        self._event_dispatcher.dispatch(
            response,
            __dispatch_key=response.janus,
        )

    def _handle_timeout(self, _response: JanusResponse) -> None:
        self._invalidate("Janus reported a session timeout")

    def _route_plugin_event(self, response: JanusResponse) -> None:
        sender = getattr(response, "sender", None)
        if sender is None:
            return
        try:
            self._plugins.dispatch(sender, response)
        except PluginNotRegistered:
            logger.debug("No local plugin owns Janus handle %s", sender)
        except Exception:
            logger.exception("Could not route event for Janus handle %s", sender)

    def _transport_closed(self, error: BaseException | None = None) -> None:
        self._invalidate("transport connection closed", error=error)

    def _invalidate(self, reason: str, *, error: BaseException | None = None) -> None:
        """Synchronously fence a lost generation and notify its recovery owner."""

        if self._state in {SessionState.CLOSING, SessionState.CLOSED, SessionState.LOST}:
            return
        self._lost_session_id = self._session_id
        self._session_id = None
        self._state = SessionState.LOST
        plugins = tuple(self._plugins.as_dict().values())
        for plugin in plugins:
            mark_lost = getattr(plugin, "_mark_handle_lost", None)
            if callable(mark_lost):
                try:
                    result = mark_lost()
                    if inspect.isawaitable(result):
                        close = getattr(result, "close", None)
                        if callable(close):
                            close()
                        raise TypeError("_mark_handle_lost must be synchronous")
                except Exception as exc:
                    # One third-party plugin must not prevent the manager from
                    # observing the session loss and fencing every other handle.
                    logger.warning(
                        "Stale handle fencing failed",
                        "A plugin rejected synchronous session-loss invalidation",
                        context={
                            "error_type": type(exc).__name__,
                            "generation": self._generation,
                            "session_id": self._lost_session_id,
                        },
                    )
        self._plugins.clear()
        self._unregister_transport_listeners()

        abort_session = getattr(self._transport, "abort_session", None)
        if self._lost_session_id is not None and callable(abort_session):
            abort_session(
                self._lost_session_id,
                JanusConnectionClosed("Janus session was lost"),
            )

        self._metrics["session_losses"] += 1
        loss = SessionLoss(
            generation=self._generation,
            session_id=self._lost_session_id,
            reason=reason,
            occurred_monotonic=time.monotonic(),
            error_type=None if error is None else type(error).__name__,
        )
        handler = self._loss_handler
        if handler is not None:
            try:
                handler(loss)
            except Exception as exc:
                logger.error(
                    "Session recovery notification failed",
                    "The Janus session loss handler raised",
                    context={
                        "error_type": type(exc).__name__,
                        "generation": self._generation,
                        "session_id": self._lost_session_id,
                    },
                    exc_info=exc,
                )

        for key, observer in tuple(self._loss_observers.items()):
            if key not in self._loss_observers:
                continue
            try:
                result = observer(loss)
                if inspect.isawaitable(result):
                    close = getattr(result, "close", None)
                    if callable(close):
                        close()
                    raise TypeError("session loss observers must not return awaitables")
            except Exception as exc:
                self._metrics["loss_observer_failures"] += 1
                logger.warning(
                    "Session loss observer failed",
                    "Application observer did not accept lifecycle metadata",
                    context={"error_type": type(exc).__name__, "generation": loss.generation},
                )

        lost_transport = self._transport

        async def cleanup_lost_generation() -> None:
            for plugin in plugins:
                close = getattr(plugin, "_invalidate_handle", None)
                if not callable(close):
                    close = getattr(plugin, "aclose", None)
                if not callable(close):
                    continue
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Stale handle cleanup failed",
                        "Could not close a plugin invalidated with its Janus session",
                        context={
                            "error_type": type(exc).__name__,
                            "generation": loss.generation,
                            "session_id": loss.session_id,
                        },
                    )
            if self._owns_transport and lost_transport is not None:
                try:
                    await lost_transport.stop()
                finally:
                    if self._transport is lost_transport:
                        self._transport = None

        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(
                cleanup_lost_generation(),
                name=f"janus-session-loss-cleanup-{self._generation}",
            )
        except RuntimeError:
            logger.debug(
                "Session loss cleanup deferred",
                "No event loop is available for invalidated session cleanup",
                context={
                    "generation": self._generation,
                    "session_id": self._lost_session_id,
                },
            )
        else:
            self._cleanup_task = task
            task.add_done_callback(self._loss_cleanup_done)

        logger.warning(
            "Janus session lost",
            "Invalidated a Janus session generation",
            context={
                "error_type": loss.error_type,
                "generation": loss.generation,
                "reason": loss.reason,
                "session_id": loss.session_id,
            },
        )

    async def _close_local(self) -> None:
        self._loss_observers.clear()
        self._unregister_transport_listeners()
        plugins = tuple(self._plugins.as_dict().values())
        self._plugins.clear()
        if plugins:
            await asyncio.gather(
                *(plugin.aclose() for plugin in plugins if hasattr(plugin, "aclose")),
                return_exceptions=True,
            )
        cleanup = self._cleanup_task
        if cleanup is not None and cleanup is not asyncio.current_task():
            await asyncio.gather(cleanup, return_exceptions=True)
            if self._cleanup_task is cleanup:
                self._cleanup_task = None
        if self._owns_transport and self._transport is not None:
            await self._transport.stop()
        self._transport = None

    async def _drain_cleanup_tasks(self) -> None:
        """Wait for the bounded cleanup from a previous lost generation."""

        cleanup = self._cleanup_task
        if cleanup is not None and cleanup is not asyncio.current_task():
            await asyncio.gather(cleanup, return_exceptions=True)
            if self._cleanup_task is cleanup:
                self._cleanup_task = None

    def _loss_cleanup_done(self, task: asyncio.Task[None]) -> None:
        if self._cleanup_task is task:
            self._cleanup_task = None
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.warning(
                "Session loss cleanup failed",
                "Could not fully release a lost Janus session generation",
                context={
                    "error_type": type(error).__name__,
                    "generation": self._generation,
                    "session_id": self._lost_session_id,
                },
            )

    async def create(self) -> Self:
        raise NotImplementedError

    async def destroy(self) -> None:
        async with self._lifecycle_lock:
            self._state = SessionState.CLOSING
            try:
                await self._close_local()
            finally:
                self._session_id = None
                self._claim_session_id = None
                self._state = SessionState.CLOSED

    async def __aenter__(self) -> Self:
        return await self.create()

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.destroy()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(id={self._session_id!r}, state={self._state!r})"

    def __str__(self) -> str:
        return str(self._session_id) if self._session_id is not None else self._state.value
