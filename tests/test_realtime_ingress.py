from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from jrtc.core import exceptions as exception_module
from jrtc.core.exceptions import JanusErrorResponse
from jrtc.messaging import JanusEventPublisher, LocalListenerRegistry
from jrtc.models.request import PingRequest
from jrtc.models.response import (
    DetachedResponse,
    EventResponse,
    MediaEventResponse,
    TimeoutResponse,
)
from jrtc.transport.websocket import WebsocketTransportClient


def _event(*, transaction: str | None = None, sender: int = 1, sequence: int = 1) -> EventResponse:
    return EventResponse.model_validate(
        {
            "janus": "event",
            "transaction": transaction,
            "session_id": 1,
            "sender": sender,
            "plugindata": {
                "plugin": "janus.plugin.test",
                "data": {"sequence": sequence},
            },
        }
    )


def _raw(response: Any) -> str:
    return response.model_dump_json(by_alias=True, exclude_none=True)


class _Incoming:
    def __init__(self, *messages: str) -> None:
        self._messages = iter(messages)

    def __aiter__(self) -> _Incoming:
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._messages)
        except StopIteration:
            raise StopAsyncIteration from None


class _Connection:
    def __init__(self, capacity: int = 16) -> None:
        self.sent: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=capacity)

    async def send(self, payload: str) -> None:
        self.sent.put_nowait(json.loads(payload))

    async def close(self) -> None:
        return None


class _BlockingBroker:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.published: list[tuple[dict[str, Any], dict[str, Any]]] = []

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    async def publish(self, message: dict[str, Any], **kwargs: Any) -> Any:
        self.published.append((message, kwargs))
        self.started.set()
        await self.release.wait()
        return SimpleNamespace(accepted=True)


class _FailingBroker:
    def __init__(self, expected_attempts: int = 1) -> None:
        self.attempts = 0
        self.expected_attempts = expected_attempts
        self.attempted = asyncio.Event()

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    async def publish(self, _message: dict[str, Any], **_kwargs: Any) -> Any:
        self.attempts += 1
        if self.attempts >= self.expected_attempts:
            self.attempted.set()
        raise RuntimeError("injected publisher failure")


def _open_transport(
    *,
    publisher: JanusEventPublisher | None = None,
    connection_capacity: int = 16,
) -> tuple[WebsocketTransportClient, _Connection]:
    transport = WebsocketTransportClient(
        reconnect=False,
        event_publisher=publisher,
        max_pending_transactions=connection_capacity,
    )
    connection = _Connection(connection_capacity)
    transport._connection = connection
    transport._connected_event.set()
    return transport, connection


async def test_full_publisher_never_delays_transactions_or_plugin_local_routing() -> None:
    broker = _BlockingBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=1,
        owns_broker=False,
    )
    await publisher.start()
    assert publisher.try_admit(_event(sequence=0))
    await asyncio.wait_for(broker.started.wait(), timeout=1)

    transport, connection = _open_transport(publisher=publisher)
    locally_routed: list[int] = []
    transport.add_message_listener(lambda response: locally_routed.append(response.sender or 0))
    requests = [
        asyncio.create_task(
            transport.send(
                PingRequest(transaction=f"blocked-{index}"),
                wait_for_event=True,
            )
        )
        for index in range(2)
    ]
    sent = [await connection.sent.get() for _ in requests]
    responses = [
        _event(transaction=item["transaction"], sender=index + 10)
        for index, item in enumerate(sent)
    ]

    await transport._process_message(_Incoming(*(_raw(item) for item in responses)))
    resolved = await asyncio.wait_for(asyncio.gather(*requests), timeout=0.5)

    assert [item.transaction for item in resolved] == [item.transaction for item in responses]
    assert locally_routed == [10, 11]
    assert publisher.queue_depth == 1
    assert publisher.statistics["rejected"] >= 2
    assert not broker.release.is_set()

    broker.release.set()
    await publisher.stop(drain=True, timeout=1)
    await transport.stop()


async def test_ack_waits_for_final_event_and_out_of_order_transactions_match() -> None:
    transport, connection = _open_transport()
    first = asyncio.create_task(
        transport.send(PingRequest(transaction="first"), wait_for_event=True)
    )
    second = asyncio.create_task(
        transport.send(PingRequest(transaction="second"), wait_for_event=True)
    )
    await connection.sent.get()
    await connection.sent.get()

    await transport._process_message(
        _Incoming(json.dumps({"janus": "ack", "transaction": "first"}))
    )
    assert not first.done()

    second_response = _event(transaction="second", sender=2)
    first_response = _event(transaction="first", sender=1)
    await transport._process_message(_Incoming(_raw(second_response), _raw(first_response)))

    assert (await second).transaction == second_response.transaction
    assert (await first).transaction == "first"
    assert transport.metrics["outstanding_transactions"] == 0
    await transport.stop()


async def test_publisher_serialization_occurs_only_after_ingress_returns(monkeypatch: Any) -> None:
    broker = _BlockingBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=2,
        owns_broker=False,
    )
    await publisher.start()
    calls: list[str] = []
    original = EventResponse.model_dump

    def observed_dump(self: EventResponse, *args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(self.janus)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(EventResponse, "model_dump", observed_dump)
    assert publisher.try_admit(_event())
    assert calls == []

    await asyncio.wait_for(broker.started.wait(), timeout=1)
    assert calls == ["event"]
    broker.release.set()
    await publisher.stop(drain=True, timeout=1)


async def test_coalescing_replaces_queued_telemetry_and_records_metrics() -> None:
    broker = _BlockingBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=2,
        owns_broker=False,
    )
    await publisher.start()
    assert publisher.try_admit(_event(sequence=0))
    await asyncio.wait_for(broker.started.wait(), timeout=1)
    older = MediaEventResponse(
        janus="media",
        session_id=1,
        sender=4,
        type="video",
        receiving=False,
    )
    newer = older.model_copy(update={"receiving": True})
    assert publisher.try_admit(older)
    assert not publisher.try_admit(newer)
    assert publisher.queue_depth == 2
    assert publisher.statistics["coalesced"] == 1

    broker.release.set()
    await publisher.stop(drain=True, timeout=1)
    assert [item[0]["janus"] for item in broker.published] == ["event", "media"]
    assert broker.published[1][0]["receiving"] is True


async def test_protected_event_evicts_oldest_queued_normal_event() -> None:
    broker = _BlockingBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=2,
        owns_broker=False,
    )
    await publisher.start()
    assert publisher.try_admit(_event(sequence=1))
    await asyncio.wait_for(broker.started.wait(), timeout=1)
    assert publisher.try_admit(_event(sender=2, sequence=2))
    assert publisher.try_admit(TimeoutResponse(janus="timeout", session_id=1))
    assert publisher.queue_depth == 2
    assert publisher.statistics["protected_evictions"] == 1

    broker.release.set()
    await publisher.stop(drain=True, timeout=1)
    assert [item[0]["janus"] for item in broker.published] == ["event", "timeout"]


async def test_protected_event_is_rejected_when_all_capacity_is_in_flight() -> None:
    broker = _BlockingBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=1,
        owns_broker=False,
    )
    await publisher.start()
    assert publisher.try_admit(TimeoutResponse(janus="timeout", session_id=1))
    await asyncio.wait_for(broker.started.wait(), timeout=1)

    assert not publisher.try_admit(DetachedResponse(janus="detached", session_id=1, sender=5))
    assert publisher.queue_depth == 1
    assert publisher.statistics["rejected"] == 1

    broker.release.set()
    await publisher.stop(drain=True, timeout=1)


async def test_retry_exhaustion_is_bounded_and_worker_survives() -> None:
    broker = _FailingBroker(expected_attempts=3)
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=1,
        owns_broker=False,
        max_publish_retries=2,
        retry_backoff=0.001,
        publish_timeout=0.1,
    )
    await publisher.start()
    assert publisher.try_admit(_event())
    await asyncio.wait_for(broker.attempted.wait(), timeout=1)
    await asyncio.wait_for(publisher._queues[0].join(), timeout=1)

    assert broker.attempts == 3
    assert publisher.statistics["publish_retries"] == 2
    assert publisher.statistics["publish_errors"] == 1
    assert publisher.running
    await publisher.stop(drain=True, timeout=1)


async def test_external_publisher_exception_never_terminates_reader() -> None:
    broker = _FailingBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=2,
        owns_broker=False,
    )
    await publisher.start()
    transport, connection = _open_transport(publisher=publisher)
    request = asyncio.create_task(transport.send(PingRequest(transaction="after-error")))
    await connection.sent.get()

    await transport._process_message(
        _Incoming(
            _raw(_event()),
            json.dumps({"janus": "success", "transaction": "after-error"}),
        )
    )
    assert (await request).transaction == "after-error"
    await asyncio.wait_for(broker.attempted.wait(), timeout=1)
    assert publisher.statistics["publish_errors"] == 1

    await publisher.stop(drain=True, timeout=1)
    await transport.stop()


def test_every_publisher_worker_queue_has_an_explicit_bound() -> None:
    publisher = JanusEventPublisher(
        _FailingBroker(),  # type: ignore[arg-type]
        workers=4,
        queue_capacity=7,
        owns_broker=False,
    )
    assert all(queue.maxsize == 7 for queue in publisher._queues)


async def test_local_listener_overflow_is_bounded_and_drains_in_order() -> None:
    registry = LocalListenerRegistry(queue_capacity=1)
    started = asyncio.Event()
    release = asyncio.Event()
    received: list[int] = []

    async def listener(response: EventResponse) -> None:
        received.append(response.sender)
        if len(received) == 1:
            started.set()
            await release.wait()

    registry.add("event", listener)
    assert registry.try_notify("event", _event(sender=1))
    await asyncio.wait_for(started.wait(), timeout=1)
    assert registry.try_notify("event", _event(sender=2))
    assert not registry.try_notify("event", _event(sender=3))

    assert registry.queue_depth == 1
    assert registry.dropped == 1
    release.set()
    await registry.aclose(drain=True, timeout=1)

    assert received == [1, 2]
    assert registry.queue_depth == 0


async def test_local_listener_self_cancellation_does_not_stop_the_worker() -> None:
    registry = LocalListenerRegistry(queue_capacity=1)
    continued = asyncio.Event()

    async def cancelling_listener(_response: Any) -> None:
        raise asyncio.CancelledError

    async def following_listener(_response: Any) -> None:
        continued.set()

    registry.add("event", cancelling_listener)
    registry.add("event", following_listener)
    assert registry.try_notify("event", _event())

    await asyncio.wait_for(continued.wait(), timeout=1)
    await registry.aclose(drain=True, timeout=1)


async def test_local_listener_shutdown_cancellation_discards_bounded_work() -> None:
    registry = LocalListenerRegistry(queue_capacity=2)
    started = asyncio.Event()
    blocked = asyncio.Event()

    async def listener(_response: Any) -> None:
        started.set()
        await blocked.wait()

    registry.add("event", listener)
    assert registry.try_notify("event", _event(sequence=1))
    await asyncio.wait_for(started.wait(), timeout=1)
    assert registry.try_notify("event", _event(sequence=2))

    shutdown = asyncio.create_task(registry.aclose(drain=True))
    await asyncio.sleep(0)
    shutdown.cancel()
    with pytest.raises(asyncio.CancelledError):
        await shutdown

    assert registry.queue_depth == 0
    assert registry.dropped == 1
    assert registry._worker is None


async def test_http_poll_dispatch_defers_slow_async_transport_listeners() -> None:
    pytest.importorskip("httpx")
    from jrtc.transport.http import HttpTransportClient

    transport = HttpTransportClient(listener_queue_capacity=1)
    started = asyncio.Event()
    release = asyncio.Event()
    received: list[int] = []

    async def listener(response: EventResponse) -> None:
        received.append(response.sender)
        if len(received) == 1:
            started.set()
            await release.wait()

    transport.add_message_listener(listener)
    await transport._handle_response(_event(sender=1))
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.wait_for(transport._handle_response(_event(sender=2)), timeout=0.5)
    await asyncio.wait_for(transport._handle_response(_event(sender=3)), timeout=0.5)

    assert transport.metrics["listener_dropped"] == 1
    release.set()
    await asyncio.wait_for(transport._listener_queue.join(), timeout=1)
    assert received == [1, 2]
    await transport.stop()


def test_janus_error_diagnostics_never_log_the_response_payload(monkeypatch: Any) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    class _CaptureLogger:
        def error(self, *args: Any, **kwargs: Any) -> None:
            calls.append((args, kwargs))

    monkeypatch.setattr(exception_module, "logger", _CaptureLogger())
    response = {
        "janus": "error",
        "jsep": {"sdp": "v=0\r\na=ice-pwd:sensitive-value"},
    }
    error = JanusErrorResponse(
        500,
        "gateway rejected request",
        transaction="safe-transaction-id",
        response=response,
    )

    assert error.response is response
    assert len(calls) == 1
    _args, kwargs = calls[0]
    assert kwargs["context"] == {
        "code": 500,
        "transaction": "safe-transaction-id",
    }
    assert "sensitive-value" not in repr(calls)
