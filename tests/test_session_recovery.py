from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from typing import Any, Self, cast

import pytest

from jrtc.core.exceptions import JanusConnectionClosed
from jrtc.lib.plugins.base import Plugin
from jrtc.manager import JanusSessionManager
from jrtc.models.request import KeepAliveRequest, PingRequest
from jrtc.models.response import AckResponse
from jrtc.session.base import SessionLoss, SessionState
from jrtc.session.websocket import JanusSession
from jrtc.transport.websocket import WebsocketTransportClient


class _ManagedSession:
    """Deterministic manager double with externally controlled activation."""

    def __init__(self, *, create_gate: asyncio.Event | None = None) -> None:
        self.state = SessionState.NEW
        self.generation = 0
        self.create_gate = create_gate
        self.create_started = asyncio.Event()
        self.destroyed = asyncio.Event()
        self._loss_handler: Callable[[SessionLoss], None] | None = None

    @property
    def ready(self) -> bool:
        return self.state is SessionState.ACTIVE

    async def create(self) -> Self:
        self.generation += 1
        self.state = SessionState.CREATING
        self.create_started.set()
        if self.create_gate is not None:
            await self.create_gate.wait()
        self.state = SessionState.ACTIVE
        return self

    async def destroy(self) -> None:
        self.state = SessionState.CLOSED
        self.destroyed.set()

    def set_loss_handler(self, handler: Callable[[SessionLoss], None] | None) -> None:
        if handler is not None and self._loss_handler not in {None, handler}:
            raise RuntimeError("session already has a recovery owner")
        self._loss_handler = handler

    def remove_loss_handler(self, handler: Callable[[SessionLoss], None]) -> None:
        if self._loss_handler is handler:
            self._loss_handler = None

    def signal_loss(self, reason: str) -> None:
        self.state = SessionState.LOST
        if self._loss_handler is not None:
            self._loss_handler(
                SessionLoss(
                    generation=self.generation,
                    session_id=100 + self.generation,
                    reason=reason,
                    occurred_monotonic=time.monotonic(),
                )
            )

    def _invalidate(self, reason: str) -> None:
        self.signal_loss(reason)


async def test_loss_notification_and_health_sweep_schedule_one_replacement() -> None:
    replacement_gate = asyncio.Event()
    initial = _ManagedSession()
    replacement = _ManagedSession(create_gate=replacement_gate)
    created = iter((initial, replacement))
    manager = JanusSessionManager(
        pool_size=1,
        session_factory=lambda: next(created),  # type: ignore[arg-type]
        monitor_interval=60,
        restart_backoff=0.001,
    )
    await manager.start()

    initial.signal_loss("socket close")
    initial.signal_loss("keepalive failure")
    initial.signal_loss("request timeout")
    assert not manager._request_recovery(0, initial, 1)
    await asyncio.wait_for(replacement.create_started.wait(), timeout=1)

    assert len(manager._replacement_tasks) == 1
    assert manager.metrics["recoveries_started"] == 1
    assert manager.metrics["recovery_signals_deduplicated"] >= 3

    replacement_task = manager._replacement_tasks[0]
    replacement_gate.set()
    await asyncio.wait_for(asyncio.shield(replacement_task), timeout=1)

    assert manager.sessions == (replacement,)
    assert replacement.ready
    assert initial.destroyed.is_set()
    assert manager.metrics["recoveries_completed"] == 1
    await manager.stop()


async def test_manager_shutdown_cancels_and_cleans_an_inflight_recovery() -> None:
    replacement_gate = asyncio.Event()
    initial = _ManagedSession()
    replacement = _ManagedSession(create_gate=replacement_gate)
    created = iter((initial, replacement))
    manager = JanusSessionManager(
        pool_size=1,
        session_factory=lambda: next(created),  # type: ignore[arg-type]
        monitor_interval=60,
        restart_backoff=0.001,
    )
    await manager.start()
    initial.signal_loss("transport connection closed")
    await asyncio.wait_for(replacement.create_started.wait(), timeout=1)

    await asyncio.wait_for(manager.stop(), timeout=1)

    assert manager.sessions == ()
    assert not manager._replacement_tasks
    assert initial.destroyed.is_set()
    assert replacement.destroyed.is_set()


class _SessionTransport:
    def __init__(self, *, fail_keepalive: bool = False) -> None:
        self.open = True
        self.fail_keepalive = fail_keepalive
        self.message_listeners: set[Callable[..., Any]] = set()
        self.close_listeners: set[Callable[..., Any]] = set()
        self.aborted: list[tuple[int, BaseException]] = []

    def add_message_listener(self, listener: Callable[..., Any]) -> None:
        self.message_listeners.add(listener)

    def remove_message_listener(self, listener: Callable[..., Any]) -> None:
        self.message_listeners.discard(listener)

    def add_close_listener(self, listener: Callable[..., Any]) -> None:
        self.close_listeners.add(listener)

    def remove_close_listener(self, listener: Callable[..., Any]) -> None:
        self.close_listeners.discard(listener)

    async def start(self) -> None:
        self.open = True

    async def stop(self) -> None:
        self.open = False

    async def send(
        self,
        request: Any,
        *,
        timeout: float | None = None,
        wait_for_event: bool = False,
    ) -> AckResponse:
        del timeout, wait_for_event
        if self.fail_keepalive and isinstance(request, KeepAliveRequest):
            raise JanusConnectionClosed("injected keepalive failure")
        return AckResponse(janus="ack", transaction=request.transaction)

    def abort_session(self, session_id: int, error: BaseException) -> None:
        self.aborted.append((session_id, error))


class _SessionPlugin(Plugin):
    identifier = "tests.session-recovery"
    name = "janus.plugin.tests"


async def test_session_loss_synchronously_fences_plugins_before_cleanup() -> None:
    transport = _SessionTransport()
    session = JanusSession(transport=transport)
    session._session_id = 55
    session._generation = 4
    session._state = SessionState.ACTIVE
    session._register_transport_listeners()
    plugin = _SessionPlugin(session=session, plugin_id=12)
    session.plugins.register(12, plugin)
    losses: list[SessionLoss] = []
    session.set_loss_handler(losses.append)

    session._transport_closed(JanusConnectionClosed("injected close"))

    assert session.state is SessionState.LOST
    assert session.lost_session_id == 55
    assert len(session.plugins) == 0
    assert transport.message_listeners == set()
    assert transport.close_listeners == set()
    assert transport.aborted[0][0] == 55
    assert len(losses) == 1
    assert losses[0].generation == 4
    assert losses[0].error_type == "JanusConnectionClosed"
    with pytest.raises(RuntimeError, match="not been attached"):
        _ = plugin.id

    await session._drain_cleanup_tasks()
    await session.destroy()


async def test_keepalive_threshold_notifies_recovery_owner_immediately() -> None:
    transport = _SessionTransport(fail_keepalive=True)
    session = JanusSession(
        transport=transport,
        keepalive_interval=0.001,
        keepalive_failures=1,
    )
    session._session_id = 66
    session._generation = 2
    session._state = SessionState.ACTIVE
    losses: list[SessionLoss] = []
    session.set_loss_handler(losses.append)

    await asyncio.wait_for(session._keepalive_loop(), timeout=1)

    assert session.state is SessionState.LOST
    assert len(losses) == 1
    assert losses[0].reason == "keepalive failure threshold reached"
    await session._drain_cleanup_tasks()
    await session.destroy()


def test_session_allows_only_one_recovery_owner() -> None:
    session = JanusSession(transport=_SessionTransport())

    def first(_loss: SessionLoss) -> None:
        return None

    def second(_loss: SessionLoss) -> None:
        return None

    session.set_loss_handler(first)

    with pytest.raises(RuntimeError, match="recovery owner"):
        session.set_loss_handler(second)

    session.remove_loss_handler(first)
    session.set_loss_handler(second)


class _SendConnection:
    def __init__(self, capacity: int = 1) -> None:
        self.sent: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=capacity)

    async def send(self, payload: str) -> None:
        self.sent.put_nowait(json.loads(payload))

    async def close(self) -> None:
        return None


async def test_transport_loss_terminates_outstanding_requests_predictably() -> None:
    transport = WebsocketTransportClient(reconnect=False)
    connection = _SendConnection()
    transport._connection = cast(Any, connection)
    transport._connected_event.set()
    pending = asyncio.create_task(transport.send(PingRequest(transaction="pending-during-loss")))
    await connection.sent.get()

    transport._fail_pending(JanusConnectionClosed("injected socket loss"))

    with pytest.raises(JanusConnectionClosed, match="injected socket loss"):
        await pending
    assert transport.metrics["outstanding_transactions"] == 0
    await transport.stop()


async def test_session_loss_aborts_only_its_own_multiplexed_requests() -> None:
    transport = WebsocketTransportClient(reconnect=False)
    connection = _SendConnection(capacity=2)
    transport._connection = cast(Any, connection)
    transport._connected_event.set()
    lost = asyncio.create_task(
        transport.send(
            KeepAliveRequest(session_id=101, transaction="lost-session"),
        )
    )
    healthy = asyncio.create_task(
        transport.send(
            KeepAliveRequest(session_id=202, transaction="healthy-session"),
        )
    )
    await connection.sent.get()
    await connection.sent.get()

    transport.abort_session(101, JanusConnectionClosed("session 101 was lost"))

    with pytest.raises(JanusConnectionClosed, match="session 101 was lost"):
        await lost
    assert not healthy.done()
    assert transport.metrics["outstanding_transactions"] == 1

    transport._resolve_ack(AckResponse(janus="ack", transaction="healthy-session"))
    assert (await healthy).transaction == "healthy-session"
    assert transport.metrics["outstanding_transactions"] == 0
    await transport.stop()
