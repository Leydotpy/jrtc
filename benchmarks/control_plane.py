"""Deterministic, in-process benchmarks for the JRTC realtime control plane.

These benchmarks deliberately avoid a live Janus or broker dependency.  They
measure the structural properties that must hold before deployment-specific
load tests are useful: concurrent transaction correlation, independence from a
stalled publisher, bounded event ingress, and ICE request-count reduction.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import platform
import sys
import time
import tracemalloc
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from types import SimpleNamespace
from typing import Any

from jrtc.lib.plugins.base import Plugin
from jrtc.messaging import JanusEventPublisher
from jrtc.models.request import PingRequest, TrickleCandidate, TrickleMessageRequest
from jrtc.models.response import AckResponse, EventResponse
from jrtc.transport.websocket import WebsocketTransportClient


@dataclass(frozen=True, slots=True)
class LatencySummary:
    p50_ms: float
    p95_ms: float
    p99_ms: float
    maximum_ms: float


class _LoopbackConnection:
    """Bounded fake socket that releases responses in reverse request order."""

    def __init__(
        self,
        expected: int,
        response_factory: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        self._expected = expected
        self._response_factory = response_factory
        self._requests: list[dict[str, Any]] = []
        self._incoming: asyncio.Queue[str | None] = asyncio.Queue(maxsize=expected + 1)
        self._finished = False

    async def send(self, payload: str) -> None:
        request = json.loads(payload)
        self._requests.append(request)
        if len(self._requests) == self._expected:
            for item in reversed(self._requests):
                self._incoming.put_nowait(json.dumps(self._response_factory(item)))

    async def close(self) -> None:
        self.finish()

    def finish(self) -> None:
        if not self._finished:
            self._finished = True
            self._incoming.put_nowait(None)

    def __aiter__(self) -> _LoopbackConnection:
        return self

    async def __anext__(self) -> str:
        value = await self._incoming.get()
        self._incoming.task_done()
        if value is None:
            raise StopAsyncIteration
        return value


class _StalledBroker:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.publish_calls = 0
        self.startup_calls = 0
        self.shutdown_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def shutdown(self) -> None:
        self.shutdown_calls += 1

    async def publish(self, _message: object, **_kwargs: object) -> object:
        self.publish_calls += 1
        self.started.set()
        await self.release.wait()
        return SimpleNamespace(accepted=True)


class _BenchmarkSession:
    def __init__(self) -> None:
        self.id = 7
        self.requests: list[tuple[TrickleMessageRequest, bool]] = []

    async def send(
        self,
        request: TrickleMessageRequest,
        *,
        timeout: float | None = None,
        wait_for_event: bool = False,
    ) -> AckResponse:
        del timeout
        self.requests.append((request, wait_for_event))
        return AckResponse(janus="ack", transaction=request.transaction)


class _BenchmarkPlugin(Plugin):
    identifier = "control-plane-benchmark"
    name = "janus.plugin.control-plane-benchmark"


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil((percentile / 100.0) * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def _latencies(values: Sequence[float]) -> LatencySummary:
    return LatencySummary(
        p50_ms=round(_percentile(values, 50) * 1000, 3),
        p95_ms=round(_percentile(values, 95) * 1000, 3),
        p99_ms=round(_percentile(values, 99) * 1000, 3),
        maximum_ms=round(max(values, default=0.0) * 1000, 3),
    )


async def _event_loop_probe(stop: asyncio.Event, samples: deque[float]) -> None:
    loop = asyncio.get_running_loop()
    interval = 0.002
    expected = loop.time() + interval
    while not stop.is_set():
        await asyncio.sleep(max(0.0, expected - loop.time()))
        now = loop.time()
        samples.append(max(0.0, now - expected))
        expected = now + interval


def _success_response(request: dict[str, Any]) -> dict[str, Any]:
    return {"janus": "success", "transaction": request["transaction"]}


def _event_response(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "janus": "event",
        "transaction": request["transaction"],
        "session_id": 1,
        "sender": 1,
        "plugindata": {
            "plugin": "janus.plugin.control-plane-benchmark",
            "data": {"status": "ok"},
        },
    }


async def _transaction_run(
    total: int,
    pool_size: int,
    *,
    publisher: JanusEventPublisher | None = None,
    event_responses: bool = False,
) -> dict[str, object]:
    tracemalloc.start()
    counts = [
        total // pool_size + (1 if index < total % pool_size else 0) for index in range(pool_size)
    ]
    transports: list[WebsocketTransportClient] = []
    connections: list[_LoopbackConnection] = []
    processors: list[asyncio.Task[None]] = []
    response_factory = _event_response if event_responses else _success_response
    for expected in counts:
        connection = _LoopbackConnection(expected, response_factory)
        transport = WebsocketTransportClient(
            reconnect=False,
            max_pending_transactions=max(1, expected),
            event_publisher=publisher,
        )
        # The benchmark supplies a protocol-compatible in-memory connection so
        # the real send, parser, dispatcher, and transaction tables are used.
        transport._connection = connection
        transport._connected_event.set()
        transports.append(transport)
        connections.append(connection)
        processors.append(asyncio.create_task(transport._process_message(connection)))

    latencies: list[float] = []
    lag_samples: deque[float] = deque(maxlen=20_000)
    stop_probe = asyncio.Event()
    probe = asyncio.create_task(_event_loop_probe(stop_probe, lag_samples))
    started = time.perf_counter()
    cpu_started = time.process_time()

    async def request_one(index: int) -> None:
        transport = transports[index % pool_size]
        before = time.perf_counter()
        await transport.send(
            PingRequest(transaction=f"benchmark-{pool_size}-{index}"),
            wait_for_event=event_responses,
        )
        latencies.append(time.perf_counter() - before)

    await asyncio.gather(*(request_one(index) for index in range(total)))
    elapsed = time.perf_counter() - started
    cpu_elapsed = time.process_time() - cpu_started
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    stop_probe.set()
    await probe
    for connection in connections:
        connection.finish()
    await asyncio.gather(*processors)
    transport_metrics = [transport.metrics for transport in transports]
    for transport in transports:
        await transport.stop()

    resolved = sum(int(item["resolved"]) for item in transport_metrics)
    resolution_total = sum(
        float(item["receive_resolution_seconds_total"]) for item in transport_metrics
    )
    dispatch_total = sum(float(item["dispatch_seconds_total"]) for item in transport_metrics)

    return {
        "sessions": pool_size,
        "transactions": total,
        "frames_per_second": round(total / elapsed, 1),
        "elapsed_seconds": round(elapsed, 4),
        "cpu_seconds": round(cpu_elapsed, 4),
        "peak_traced_bytes": peak,
        "rtt": asdict(_latencies(latencies)),
        "event_loop_lag": asdict(_latencies(tuple(lag_samples))),
        "receive_to_transaction_resolution": {
            "average_us": round(
                (resolution_total / resolved) * 1_000_000 if resolved else 0.0,
                3,
            ),
            "maximum_us": round(
                max(
                    (float(item["receive_resolution_seconds_max"]) for item in transport_metrics),
                    default=0.0,
                )
                * 1_000_000,
                3,
            ),
        },
        "average_dispatch_us": round(
            (dispatch_total / total) * 1_000_000 if total else 0.0,
            3,
        ),
        "outstanding_after": sum(len(transport._transactions) for transport in transports),
    }


async def scenario_a(total: int) -> list[dict[str, object]]:
    return [await _transaction_run(total, pool_size) for pool_size in (1, 2, 4)]


async def scenario_b(total: int, queue_capacity: int) -> dict[str, object]:
    # Compare identical plugin-event parsing, admission, metrics, and worker
    # setup. The only changed variable in the second run is backend completion.
    healthy_broker = _StalledBroker()
    healthy_broker.release.set()
    healthy_publisher = JanusEventPublisher(
        healthy_broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=queue_capacity,
        publish_timeout=10.0,
    )
    await healthy_publisher.start()
    baseline = await _transaction_run(
        total,
        1,
        publisher=healthy_publisher,
        event_responses=True,
    )
    await healthy_publisher.stop(drain=True, timeout=5.0)
    broker = _StalledBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=queue_capacity,
        publish_timeout=10.0,
    )
    await publisher.start()
    result = await _transaction_run(
        total,
        1,
        publisher=publisher,
        event_responses=True,
    )
    result["transactions_completed_while_broker_stalled"] = not broker.release.is_set()
    result["unstalled_baseline_rtt"] = baseline["rtt"]
    baseline_rtt = baseline["rtt"]
    stalled_rtt = result["rtt"]
    assert isinstance(baseline_rtt, dict)
    assert isinstance(stalled_rtt, dict)
    baseline_p95 = float(baseline_rtt["p95_ms"])
    stalled_p95 = float(stalled_rtt["p95_ms"])
    result["p95_delta_ms"] = round(stalled_p95 - baseline_p95, 3)
    baseline_resolution = baseline["receive_to_transaction_resolution"]
    stalled_resolution = result["receive_to_transaction_resolution"]
    assert isinstance(baseline_resolution, dict)
    assert isinstance(stalled_resolution, dict)
    result["resolution_average_delta_us"] = round(
        float(stalled_resolution["average_us"]) - float(baseline_resolution["average_us"]),
        3,
    )
    result["ingress_depth_while_stalled"] = publisher.queue_depth
    result["ingress_capacity"] = queue_capacity
    if publisher.queue_depth > queue_capacity:
        raise RuntimeError("publisher queue exceeded its configured capacity")
    broker.release.set()
    await publisher.stop(drain=True, timeout=5.0)
    result["publish_calls"] = broker.publish_calls
    return result


async def scenario_c(total: int, queue_capacity: int) -> dict[str, object]:
    broker = _StalledBroker()
    publisher = JanusEventPublisher(
        broker,  # type: ignore[arg-type]
        workers=1,
        queue_capacity=queue_capacity,
        publish_timeout=10.0,
    )
    await publisher.start()
    accepted = 0
    tracemalloc.start()
    started = time.perf_counter()
    for index in range(total):
        response = EventResponse.model_validate(
            {
                "janus": "event",
                "session_id": 10,
                "sender": (index % 128) + 1,
                "plugindata": {
                    "plugin": "janus.plugin.control-plane-benchmark",
                    "data": {"sequence": index},
                },
            }
        )
        accepted += int(publisher.try_admit(response, session_id=10, sender=response.sender))
    elapsed = time.perf_counter() - started
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    depth = publisher.queue_depth
    if depth > queue_capacity:
        raise RuntimeError("publisher queue exceeded its configured capacity")
    broker.release.set()
    await publisher.stop(drain=True, timeout=5.0)
    return {
        "events": total,
        "accepted": accepted,
        "rejected_or_coalesced": total - accepted,
        "queue_depth_at_peak": depth,
        "queue_capacity": queue_capacity,
        "bounded": depth <= queue_capacity,
        "admission_events_per_second": round(total / elapsed, 1),
        "peak_traced_bytes": peak,
    }


async def scenario_d(total: int) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    candidates = [
        TrickleCandidate(
            candidate=f"candidate:{index} 1 udp 1 192.0.2.1 9 typ host",
            sdpMid="0",
            sdpMLineIndex=0,
        )
        for index in range(total)
    ]
    for batch_size in (1, 4, 8, 16, 32):
        session = _BenchmarkSession()
        plugin = _BenchmarkPlugin(session=session, plugin_id=9)
        started = time.perf_counter()
        for offset in range(0, total, batch_size):
            batch = candidates[offset : offset + batch_size]
            await plugin.trickle(batch[0] if batch_size == 1 else batch)
        candidate_requests = len(session.requests)
        await plugin.complete_trickle()
        elapsed = time.perf_counter() - started
        if any(wait_for_event for _request, wait_for_event in session.requests):
            raise RuntimeError("ICE trickle unexpectedly waited for a final plugin event")
        results.append(
            {
                "batch_size": batch_size,
                "candidates": total,
                "candidate_requests": candidate_requests,
                "requests_including_completion": len(session.requests),
                "expected_candidate_requests": math.ceil(total / batch_size),
                "elapsed_seconds": round(elapsed, 5),
            }
        )
        await plugin.aclose()
    return results


async def run(args: argparse.Namespace) -> dict[str, object]:
    return {
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "note": "in-process structural benchmark; confirm capacity on live Janus/broker",
        },
        "scenario_a_transaction_storm": await scenario_a(args.transactions),
        "scenario_b_broker_stall": await scenario_b(
            args.stalled_transactions,
            args.queue_capacity,
        ),
        "scenario_c_event_storm": await scenario_c(args.events, args.queue_capacity),
        "scenario_d_batched_ice": await scenario_d(args.ice_candidates),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transactions", type=int, default=2_000)
    parser.add_argument("--stalled-transactions", type=int, default=1_000)
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--ice-candidates", type=int, default=1_024)
    parser.add_argument("--queue-capacity", type=int, default=64)
    values = parser.parse_args()
    for name, value in vars(values).items():
        if isinstance(value, int) and value < 4:
            parser.error(f"--{name.replace('_', '-')} must be at least 4")
    return values


if __name__ == "__main__":
    json.dump(asyncio.run(run(parse_args())), sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
