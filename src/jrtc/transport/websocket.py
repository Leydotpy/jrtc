"""Cancellation-safe WebSocket transport for the Janus protocol."""

from __future__ import annotations

import asyncio
import inspect
import json
import math
import time
from collections import deque
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from logvista import get_logger
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.typing import Subprotocol

from jrtc.core.exceptions import (
    JanusConnectionClosed,
    JanusErrorResponse,
    JanusProtocolError,
    JanusRequestTimeout,
    JanusTransportError,
)
from jrtc.models import JanusRequest, JanusResponse
from jrtc.models.common import JanusId, validate_janus_id
from jrtc.models.response import parse_janus_response
from jrtc.transport.base import CloseListener, MessageListener

if TYPE_CHECKING:
    from jrtc.messaging import JanusEventPublisher

logger = get_logger(__name__)


@dataclass(slots=True)
class _PendingTransaction:
    future: asyncio.Future[JanusResponse]
    request: str
    session_id: JanusId | None
    wait_for_event: bool
    started_at: float
    acknowledged: bool = False


class WebsocketTransportClient:
    """One multiplexed Janus WebSocket connection.

    The transport may reconnect its socket, but every session bound to a closed
    Janus WebSocket is notified and invalidated. Session managers must create
    fresh sessions/handles after reconnect; stale IDs are never reused.
    """

    def __init__(
        self,
        url: str = "ws://localhost:8188/janus",
        protocol: str = "janus-protocol",
        *,
        connect_timeout: float = 10.0,
        request_timeout: float = 15.0,
        max_pending_transactions: int = 4096,
        max_message_size: int = 2**20,
        ping_interval: float = 20.0,
        ping_timeout: float = 20.0,
        reconnect: bool = True,
        event_publisher: JanusEventPublisher | None = None,
        listener_queue_capacity: int = 1024,
    ) -> None:
        if any(
            not math.isfinite(value) or value <= 0
            for value in (connect_timeout, request_timeout, ping_interval, ping_timeout)
        ):
            raise ValueError("transport timeouts must be finite and greater than zero")
        if max_message_size < 1:
            raise ValueError("max_message_size must be positive")
        if max_pending_transactions < 1:
            raise ValueError("max_pending_transactions must be at least one")
        if (
            isinstance(listener_queue_capacity, bool)
            or not isinstance(listener_queue_capacity, int)
            or listener_queue_capacity < 1
        ):
            raise ValueError("listener_queue_capacity must be a positive integer")
        self._url = url
        self._protocol = protocol
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._max_message_size = max_message_size
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._reconnect = reconnect

        self._connection: ClientConnection | None = None
        self._transactions: dict[str, _PendingTransaction] = {}
        self._pending_slots = asyncio.Semaphore(max_pending_transactions)
        self._listen_task: asyncio.Task[None] | None = None
        self._connected_event = asyncio.Event()
        self._message_listeners: set[MessageListener] = set()
        self._close_listeners: set[CloseListener] = set()
        self._connect_lock = asyncio.Lock()
        self._stopping = False
        self._generation = 0
        self._listener_queue: asyncio.Queue[Awaitable[object]] = asyncio.Queue(
            maxsize=listener_queue_capacity
        )
        self._listener_task: asyncio.Task[None] | None = None
        self._rtt_samples: deque[float] = deque(maxlen=2048)
        self._metrics_started_at = time.perf_counter()
        self._current_received_at: float | None = None
        from jrtc.messaging import JanusResponseDispatcher

        self._dispatcher = JanusResponseDispatcher(
            publisher=event_publisher,
            on_transaction=self._resolve_transaction,
            on_ack=self._resolve_ack,
            on_error=self._resolve_error,
        )
        self._metrics = {
            "received": 0,
            "resolved": 0,
            "errors": 0,
            "events": 0,
            "reconnects": 0,
            "pending_peak": 0,
            "listener_dropped": 0,
            "receive_resolution_seconds_total": 0.0,
            "receive_resolution_seconds_max": 0.0,
            "dispatch_seconds_total": 0.0,
        }

    @property
    def open(self) -> bool:
        return self._connection is not None and self._connected_event.is_set()

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def metrics(self) -> dict[str, int | float]:
        snapshot = dict(self._metrics)
        elapsed = max(time.perf_counter() - self._metrics_started_at, 1e-9)
        samples = sorted(self._rtt_samples)
        snapshot.update(
            {
                "frames_per_second": float(self._metrics["received"]) / elapsed,
                "outstanding_transactions": len(self._transactions),
                "request_rtt_p50_ms": self._percentile(samples, 0.50) * 1000,
                "request_rtt_p95_ms": self._percentile(samples, 0.95) * 1000,
                "request_rtt_p99_ms": self._percentile(samples, 0.99) * 1000,
            }
        )
        return snapshot

    def add_message_listener(self, listener: MessageListener) -> None:
        if not callable(listener):
            raise TypeError("message listener must be callable")
        self._message_listeners.add(listener)

    def remove_message_listener(self, listener: MessageListener) -> None:
        self._message_listeners.discard(listener)

    def add_close_listener(self, listener: CloseListener) -> None:
        if not callable(listener):
            raise TypeError("close listener must be callable")
        self._close_listeners.add(listener)

    def remove_close_listener(self, listener: CloseListener) -> None:
        self._close_listeners.discard(listener)

    async def start(self) -> None:
        async with self._connect_lock:
            if self.open:
                return
            if self._listen_task is None or self._listen_task.done():
                self._stopping = False
                self._listen_task = asyncio.create_task(
                    self._connection_loop(), name="janus-websocket-listener"
                )
            listener = self._listen_task

        connected = asyncio.create_task(self._connected_event.wait())
        try:
            done, _ = await asyncio.wait(
                {connected, listener},
                timeout=self._connect_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except BaseException:
            connected.cancel()
            await asyncio.gather(connected, return_exceptions=True)
            raise
        if connected in done and connected.result():
            return
        connected.cancel()
        await asyncio.gather(connected, return_exceptions=True)
        if listener.done():
            error = listener.exception()
            if error is not None:
                raise JanusTransportError("Janus WebSocket listener terminated") from error
        await self.stop()
        raise JanusTransportError(
            f"Could not connect to Janus at {self._url!r} within {self._connect_timeout:g}s"
        )

    async def stop(self) -> None:
        self._stopping = True
        self._connected_event.clear()
        connection, self._connection = self._connection, None
        if connection is not None:
            try:
                await connection.close()
            except Exception as exc:
                logger.debug(
                    "Transport shutdown",
                    "Could not close the Janus WebSocket cleanly",
                    context={"error_type": type(exc).__name__},
                    exc_info=exc,
                )
        listener, self._listen_task = self._listen_task, None
        if listener is not None and listener is not asyncio.current_task():
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
        callback_worker, self._listener_task = self._listener_task, None
        if callback_worker is not None and callback_worker is not asyncio.current_task():
            callback_worker.cancel()
            await asyncio.gather(callback_worker, return_exceptions=True)
        self._discard_listener_queue()
        self._fail_pending(JanusConnectionClosed("Janus WebSocket stopped"))

    async def send(
        self,
        message: JanusRequest,
        *,
        timeout: float | None = None,
        wait_for_event: bool = False,
    ) -> JanusResponse:
        if not self.open or self._connection is None:
            raise JanusConnectionClosed("Janus WebSocket is not connected")
        effective_timeout = self._request_timeout if timeout is None else timeout
        if not math.isfinite(effective_timeout) or effective_timeout <= 0:
            raise ValueError("timeout must be finite and greater than zero")

        transaction = message.transaction or uuid4().hex
        future: asyncio.Future[JanusResponse] | None = None
        try:
            async with asyncio.timeout(effective_timeout):
                async with self._pending_slots:
                    if not self.open or self._connection is None:
                        raise JanusConnectionClosed("Janus WebSocket is not connected")
                    while transaction in self._transactions:
                        transaction = uuid4().hex
                    message.transaction = transaction
                    future = asyncio.get_running_loop().create_future()
                    self._transactions[transaction] = _PendingTransaction(
                        future=future,
                        request=message.janus,
                        session_id=getattr(message, "session_id", None),
                        wait_for_event=wait_for_event,
                        started_at=time.perf_counter(),
                    )
                    self._metrics["pending_peak"] = max(
                        self._metrics["pending_peak"],
                        len(self._transactions),
                    )
                    connection = self._connection
                    payload = message.model_dump_json(by_alias=True, exclude_none=True)
                    try:
                        await connection.send(payload)
                    except asyncio.CancelledError:
                        raise
                    except ConnectionClosed as exc:
                        raise JanusConnectionClosed(
                            "Janus WebSocket closed while sending a request"
                        ) from exc
                    except (OSError, RuntimeError) as exc:
                        raise JanusTransportError(
                            "Janus WebSocket failed while sending a request"
                        ) from exc
                    logger.debug(
                        "Transport metric",
                        "Janus WebSocket request sent",
                        context={
                            "janus_type": message.janus,
                            "pending": len(self._transactions),
                            "transaction": transaction,
                        },
                    )
                    return await future
        except TimeoutError as exc:
            raise JanusRequestTimeout(transaction, effective_timeout) from exc
        finally:
            if future is not None:
                pending = self._transactions.get(transaction)
                if pending is not None and pending.future is future:
                    self._transactions.pop(transaction, None)
                if not future.done():
                    future.cancel()

    async def _connection_loop(self) -> None:
        first_connection = True
        try:
            connector = connect(
                self._url,
                subprotocols=[cast(Subprotocol, self._protocol)],
                ping_interval=self._ping_interval,
                ping_timeout=self._ping_timeout,
                max_size=self._max_message_size,
                open_timeout=self._connect_timeout,
            )
            async for websocket in connector:
                if self._stopping:
                    await websocket.close()
                    break
                if not first_connection:
                    self._metrics["reconnects"] += 1
                first_connection = False
                self._generation += 1
                self._connection = websocket
                self._connected_event.set()
                logger.debug(
                    "Transport metric",
                    "Janus WebSocket connected",
                    context={"generation": self._generation},
                )
                try:
                    await self._process_message(websocket)
                except ConnectionClosed as exc:
                    logger.warning(
                        "Transport connection closed",
                        "Janus WebSocket connection ended",
                        context={"error_type": type(exc).__name__},
                    )
                finally:
                    if self._connection is websocket:
                        self._connection = None
                    self._connected_event.clear()
                    closed = JanusConnectionClosed("Janus WebSocket connection was lost")
                    self._fail_pending(closed)
                    self._notify_close(closed)
                if not self._reconnect:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._stopping:
                logger.exception(
                    "Transport connection failed",
                    "Janus WebSocket connection loop failed",
                    context={"error_type": type(exc).__name__},
                )
                raise

    async def _process_message(self, connection: Any | None = None) -> None:
        websocket = connection or self._connection
        if websocket is None:
            raise JanusConnectionClosed("Janus WebSocket is not connected")
        async for raw_message in websocket:
            try:
                if isinstance(raw_message, bytes):
                    raw_message = raw_message.decode("utf-8")
                payload = json.loads(raw_message)
                response = parse_janus_response(payload)
                logger.debug(
                    "Transport metric",
                    "Janus WebSocket response parsed",
                    context={"janus_type": response.janus},
                )
            except (UnicodeDecodeError, json.JSONDecodeError, JanusProtocolError) as exc:
                self._metrics["errors"] += 1
                logger.warning(
                    "Malformed Janus response",
                    "Discarding an invalid WebSocket message",
                    context={
                        "error_type": type(exc).__name__,
                        "errors": self._metrics["errors"],
                    },
                )
                continue

            self._metrics["received"] += 1
            janus_type = response.janus
            started = time.perf_counter()
            self._current_received_at = started
            try:
                await self._dispatcher.dispatch(
                    response,
                    session_id=response.session_id,
                    sender=response.sender,
                )
            finally:
                self._current_received_at = None
            duration = max(0.0, time.perf_counter() - started)
            self._metrics["dispatch_seconds_total"] += duration
            if janus_type in self._dispatcher.dispatchable_events:
                self._metrics["events"] += 1
            logger.debug(
                "Transport metric",
                "Janus WebSocket response dispatched",
                context={
                    "duration_ms": round(duration * 1000, 3),
                    "janus_type": janus_type,
                    "received": self._metrics["received"],
                },
            )

    def _pending(self, transaction: str | None) -> tuple[str, _PendingTransaction] | None:
        if not transaction:
            return None
        key = str(transaction)
        pending = self._transactions.get(key)
        if isinstance(pending, dict):
            pending = _PendingTransaction(
                future=pending["future"],
                request=str(pending.get("request", "")),
                session_id=pending.get("session_id"),
                wait_for_event=True,
                started_at=float(pending.get("started_at", time.perf_counter())),
            )
        if pending is None or pending.future.done():
            return None
        return key, pending

    def _resolve_ack(self, response: JanusResponse) -> None:
        entry = self._pending(response.transaction)
        if entry is None:
            return
        transaction, pending = entry
        pending.acknowledged = True
        if pending.request == "keepalive" or not pending.wait_for_event:
            pending.future.set_result(response)
            self._transactions.pop(transaction, None)
            self._metrics["resolved"] += 1
            self._record_resolution(pending)

    def _resolve_error(self, response: JanusResponse) -> None:
        entry = self._pending(response.transaction)
        if entry is None:
            return
        transaction, pending = entry
        error = getattr(response, "error", None)
        if error is None:
            return
        pending.future.set_exception(
            JanusErrorResponse(
                error.code,
                error.reason,
                transaction=transaction,
                response=response,
            )
        )
        self._transactions.pop(transaction, None)
        self._metrics["errors"] += 1
        self._record_resolution(pending)

    def _resolve_transaction(self, response: JanusResponse) -> None:
        entry = self._pending(response.transaction)
        if entry is not None:
            transaction, pending = entry
            pending.future.set_result(response)
            self._transactions.pop(transaction, None)
            self._metrics["resolved"] += 1
            self._record_resolution(pending)
        if response.janus in self._dispatcher.dispatchable_events:
            self._notify_message(response)

    def _record_resolution(self, pending: _PendingTransaction) -> None:
        self._rtt_samples.append(max(0.0, time.perf_counter() - pending.started_at))
        if self._current_received_at is not None:
            latency = max(0.0, time.perf_counter() - self._current_received_at)
            self._metrics["receive_resolution_seconds_total"] += latency
            self._metrics["receive_resolution_seconds_max"] = max(
                self._metrics["receive_resolution_seconds_max"],
                latency,
            )

    def _notify_message(self, response: JanusResponse) -> None:
        for listener in tuple(self._message_listeners):
            try:
                result = listener(response)
                if inspect.isawaitable(result):
                    self._admit_listener_awaitable(result)
            except asyncio.CancelledError as exc:
                logger.error(
                    "Local listener failed",
                    "Janus transport message listener raised cancellation",
                    context={"error_type": type(exc).__name__, "janus_type": response.janus},
                )
            except Exception as exc:
                logger.error(
                    "Local listener failed",
                    "Janus transport message listener raised",
                    context={"error_type": type(exc).__name__, "janus_type": response.janus},
                    exc_info=exc,
                )

    def _notify_close(self, error: BaseException | None) -> None:
        for listener in tuple(self._close_listeners):
            try:
                result = listener(error)
                if inspect.isawaitable(result):
                    self._admit_listener_awaitable(result)
            except asyncio.CancelledError as exc:
                logger.error(
                    "Local listener failed",
                    "Janus transport close listener raised cancellation",
                    context={"error_type": type(exc).__name__},
                )
            except Exception as exc:
                logger.error(
                    "Local listener failed",
                    "Janus transport close listener raised",
                    context={"error_type": type(exc).__name__},
                    exc_info=exc,
                )

    def _admit_listener_awaitable(self, result: Awaitable[object]) -> None:
        try:
            self._listener_queue.put_nowait(result)
        except asyncio.QueueFull:
            self._metrics["listener_dropped"] += 1
            close = getattr(result, "close", None)
            if callable(close):
                close()
            return
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(
                self._drain_listener_queue(),
                name="janus-websocket-listener-callbacks",
            )

    async def _drain_listener_queue(self) -> None:
        while True:
            try:
                result = self._listener_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await result
            except asyncio.CancelledError:
                task = asyncio.current_task()
                if task is not None and task.cancelling():
                    raise
                logger.warning(
                    "Local listener cancelled",
                    "A Janus transport listener cancelled its own awaitable",
                )
            except Exception as exc:
                logger.error(
                    "Local listener failed",
                    "A deferred Janus transport listener raised",
                    context={"error_type": type(exc).__name__},
                    exc_info=exc,
                )
            finally:
                self._listener_queue.task_done()

    def _discard_listener_queue(self) -> None:
        while True:
            try:
                result = self._listener_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            close = getattr(result, "close", None)
            if callable(close):
                close()
            self._listener_queue.task_done()

    @staticmethod
    def _percentile(samples: list[float], percentile: float) -> float:
        if not samples:
            return 0.0
        index = max(0, math.ceil(percentile * len(samples)) - 1)
        return samples[min(index, len(samples) - 1)]

    def _fail_pending(self, error: Exception) -> None:
        for item in tuple(self._transactions.values()):
            if isinstance(item, dict):
                future = item.get("future")
                started_at = float(item.get("started_at", time.perf_counter()))
            else:
                future = item.future
                started_at = item.started_at
            if not future.done():
                future.set_exception(error)
                self._rtt_samples.append(max(0.0, time.perf_counter() - started_at))
        self._transactions.clear()

    def abort_session(self, session_id: JanusId, error: Exception) -> None:
        """Fail only transactions owned by one invalidated Janus session.

        A transport may multiplex several sessions, so session-level recovery
        must not disturb unrelated transactions on the same connection.
        """

        session_id = validate_janus_id(session_id, name="session_id")
        for transaction, item in tuple(self._transactions.items()):
            item_session_id = item.get("session_id") if isinstance(item, dict) else item.session_id
            if item_session_id != session_id:
                continue
            future = item.get("future") if isinstance(item, dict) else item.future
            started_at = (
                float(item.get("started_at", time.perf_counter()))
                if isinstance(item, dict)
                else item.started_at
            )
            if not future.done():
                future.set_exception(error)
                self._rtt_samples.append(max(0.0, time.perf_counter() - started_at))
            self._transactions.pop(transaction, None)

    async def __aenter__(self) -> WebsocketTransportClient:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.stop()


async def create_socket_client(url: str, **kwargs: Any) -> WebsocketTransportClient:
    client = WebsocketTransportClient(url, **kwargs)
    await client.start()
    return client
