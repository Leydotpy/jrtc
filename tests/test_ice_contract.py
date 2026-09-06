from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from jrtc.lib.manager import PluginManager
from jrtc.lib.plugins.base import Plugin
from jrtc.models.request import (
    MAX_TRICKLE_CANDIDATES,
    TrickleCandidate,
    TrickleMessageRequest,
    TrickleRequest,
)
from jrtc.models.response import AckResponse


class _IcePlugin(Plugin):
    identifier = "tests.ice-contract"
    name = "janus.plugin.tests"


class _RecordingSession:
    id = 41

    def __init__(self) -> None:
        self.plugins: PluginManager[Plugin] = PluginManager()
        self.calls: list[tuple[Any, float | None, bool]] = []

    async def send(
        self,
        request: Any,
        *,
        timeout: float | None = None,
        wait_for_event: bool = False,
    ) -> AckResponse:
        self.calls.append((request, timeout, wait_for_event))
        return AckResponse(janus="ack", transaction=request.transaction)


def _candidate(index: int) -> TrickleCandidate:
    return TrickleCandidate(
        candidate=f"candidate:{index} 1 UDP 1 192.0.2.{index + 1} 9 typ host",
        sdpMid="video",
        sdpMLineIndex=index,
    )


def _dump(request: TrickleRequest) -> dict[str, Any]:
    return request.model_dump(mode="json", by_alias=True, exclude_none=True)


async def test_one_candidate_uses_the_singular_field_without_waiting_for_an_event() -> None:
    session = _RecordingSession()
    plugin = _IcePlugin(session=session, plugin_id=7)
    candidate = _candidate(0)

    await plugin.trickle(candidate, timeout=0.25)

    assert len(session.calls) == 1
    request, timeout, wait_for_event = session.calls[0]
    assert isinstance(request, TrickleMessageRequest)
    assert _dump(request)["candidate"] == candidate.model_dump(
        by_alias=True,
        exclude_none=True,
    )
    assert "candidates" not in _dump(request)
    assert timeout == 0.25
    assert wait_for_event is False


async def test_sixteen_candidates_remain_one_ordered_plural_request() -> None:
    session = _RecordingSession()
    plugin = _IcePlugin(session=session, plugin_id=8)
    candidates = tuple(_candidate(index) for index in range(16))

    await plugin.trickle(candidates)

    assert len(session.calls) == 1
    request, _timeout, wait_for_event = session.calls[0]
    payload = _dump(request)
    assert "candidate" not in payload
    assert len(payload["candidates"]) == 16
    assert [item["candidate"] for item in payload["candidates"]] == [
        item.candidate for item in candidates
    ]
    assert wait_for_event is False


async def test_completion_is_an_explicit_singular_marker() -> None:
    session = _RecordingSession()
    plugin = _IcePlugin(session=session, plugin_id=9)

    await plugin.complete_trickle()

    request, _timeout, wait_for_event = session.calls[0]
    payload = _dump(request)
    assert payload["candidate"] == {"completed": True}
    assert "candidates" not in payload
    assert wait_for_event is False


async def test_empty_and_oversized_candidate_sequences_are_rejected() -> None:
    plugin = _IcePlugin(session=_RecordingSession(), plugin_id=10)

    with pytest.raises(ValidationError):
        await plugin.trickle(())
    with pytest.raises(ValidationError):
        await plugin.trickle(tuple(_candidate(0) for _ in range(MAX_TRICKLE_CANDIDATES + 1)))


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"candidates": []},
        {"candidate": _candidate(0), "candidates": [_candidate(1)]},
    ],
)
def test_request_requires_exactly_one_nonempty_candidate_container(
    payload: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        TrickleRequest.model_validate(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"candidate": ""},
        {"candidate": 1},
        {"candidate": "candidate:1", "completed": True},
        {"candidate": "candidate:1", "sdpMid": ""},
        {"candidate": "candidate:1", "sdpMid": 1},
        {"candidate": "candidate:1", "sdpMLineIndex": -1},
        {"candidate": "candidate:1", "sdpMLineIndex": "0"},
        {"candidate": "candidate:1", "sdpMLineIndex": True},
        {"completed": False},
        {"completed": 1},
        {"completed": True, "sdpMid": "video"},
        {"completed": True, "sdpMLineIndex": 0},
    ],
)
def test_candidate_and_completion_shapes_are_strict(payload: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        TrickleCandidate.model_validate(payload)


async def test_plugin_subclasses_preserve_the_batch_contract() -> None:
    class ApplicationPlugin(_IcePlugin):
        identifier = "tests.application-ice-contract"

    session = _RecordingSession()
    plugin = ApplicationPlugin(session=session, plugin_id=11)
    candidates = [_candidate(3), _candidate(1), _candidate(2)]

    await plugin.trickle(candidates)

    request, _timeout, _wait_for_event = session.calls[0]
    assert [item.sdp_mline_index for item in request.candidates or ()] == [3, 1, 2]
