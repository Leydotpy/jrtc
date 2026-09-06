"""Stable routes and metric names for Janus broker events."""

from __future__ import annotations

from types import MappingProxyType
from typing import Final

DEFAULT_PHYSICAL_ROUTE: Final = "janus.events"
JANUS_LOGICAL_PATTERN: Final = "janus.*"

JANUS_EVENT_ROUTES = MappingProxyType(
    {
        "event": "janus.event",
        "webrtcup": "janus.webrtcup",
        "media": "janus.media",
        "slowlink": "janus.slowlink",
        "hangup": "janus.hangup",
        "detached": "janus.detached",
        "trickle": "janus.trickle",
        "timeout": "janus.timeout",
    }
)
DISPATCHABLE_JANUS_TYPES: Final = frozenset(JANUS_EVENT_ROUTES)

ADMISSION_TOTAL: Final = "janus_event_admission_total"
PUBLISHED_TOTAL: Final = "janus_event_published_total"
PUBLISH_FAILURES_TOTAL: Final = "janus_event_publish_failures_total"
PUBLISH_RETRIES_TOTAL: Final = "janus_event_publish_retries_total"
DROPPED_TOTAL: Final = "janus_event_dropped_total"
COALESCED_TOTAL: Final = "janus_event_coalesced_total"
QUEUE_DEPTH: Final = "janus_event_queue_depth"
QUEUE_LATENCY_SECONDS: Final = "janus_event_queue_latency_seconds"
PUBLISH_LATENCY_SECONDS: Final = "janus_event_publish_latency_seconds"
DISPATCH_TOTAL: Final = "janus_dispatch_total"
DISPATCH_FAILURES_TOTAL: Final = "janus_dispatch_failures_total"
DISPATCH_DURATION_SECONDS: Final = "janus_dispatch_duration_seconds"
LISTENER_FAILURES_TOTAL: Final = "janus_listener_failures_total"
LISTENER_DROPPED_TOTAL: Final = "janus_listener_dropped_total"
LISTENER_QUEUE_DEPTH: Final = "janus_listener_queue_depth"
LISTENER_QUEUE_LATENCY_SECONDS: Final = "janus_listener_queue_latency_seconds"

__all__ = [
    "ADMISSION_TOTAL",
    "COALESCED_TOTAL",
    "DEFAULT_PHYSICAL_ROUTE",
    "DISPATCHABLE_JANUS_TYPES",
    "DISPATCH_DURATION_SECONDS",
    "DISPATCH_FAILURES_TOTAL",
    "DISPATCH_TOTAL",
    "DROPPED_TOTAL",
    "JANUS_EVENT_ROUTES",
    "JANUS_LOGICAL_PATTERN",
    "LISTENER_DROPPED_TOTAL",
    "LISTENER_FAILURES_TOTAL",
    "LISTENER_QUEUE_DEPTH",
    "LISTENER_QUEUE_LATENCY_SECONDS",
    "PUBLISHED_TOTAL",
    "PUBLISH_FAILURES_TOTAL",
    "PUBLISH_LATENCY_SECONDS",
    "PUBLISH_RETRIES_TOTAL",
    "QUEUE_DEPTH",
    "QUEUE_LATENCY_SECONDS",
]
