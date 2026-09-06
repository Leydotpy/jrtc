from __future__ import annotations

from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

import jrtc
from jrtc.core.exceptions import JanusProtocolError
from jrtc.lib.manager import PluginManager
from jrtc.lib.plugins.base import Plugin
from jrtc.models.common import validate_janus_id
from jrtc.models.request import DetachPluginRequest, KeepAliveRequest, PluginMessageRequest
from jrtc.models.response import EventResponse, SuccessResponse, parse_janus_response
from jrtc.session import JanusSession


class _Plugin(Plugin):
    identifier = "tests.identifier-types"
    name = "janus.plugin.tests"


@pytest.mark.parametrize("invalid_id", ["123", True, False, 0, -1, 1.0, None])
def test_outbound_session_ids_reject_non_positive_or_non_integer_values(
    invalid_id: Any,
) -> None:
    with pytest.raises(ValidationError):
        KeepAliveRequest.model_validate({"janus": "keepalive", "session_id": invalid_id})


@pytest.mark.parametrize("field", ["session_id", "handle_id"])
def test_outbound_handle_envelope_does_not_coerce_string_ids(field: str) -> None:
    payload: dict[str, Any] = {
        "janus": "message",
        "session_id": 101,
        "handle_id": 202,
        "body": {},
    }
    payload[field] = str(payload[field])

    with pytest.raises(ValidationError):
        PluginMessageRequest.model_validate(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "janus": "event",
            "session_id": "101",
            "sender": 202,
            "plugindata": {"plugin": "janus.plugin.echotest", "data": {}},
        },
        {
            "janus": "event",
            "session_id": 101,
            "sender": "202",
            "plugindata": {"plugin": "janus.plugin.echotest", "data": {}},
        },
        {"janus": "success", "data": {"id": "303"}},
    ],
)
def test_inbound_responses_reject_string_ids(payload: dict[str, Any]) -> None:
    with pytest.raises(JanusProtocolError):
        parse_janus_response(payload)


def test_protocol_ids_remain_integers_across_models() -> None:
    request = DetachPluginRequest(session_id=101, handle_id=202)
    response = parse_janus_response(
        {
            "janus": "event",
            "session_id": 101,
            "sender": 202,
            "plugindata": {"plugin": "janus.plugin.echotest", "data": {}},
        }
    )
    success = SuccessResponse.model_validate({"janus": "success", "data": {"id": 303}})

    assert type(request.session_id) is int
    assert type(request.handle_id) is int
    assert isinstance(response, EventResponse)
    assert type(response.session_id) is int
    assert type(response.sender) is int
    assert success.data is not None
    assert type(success.data.id) is int
    assert validate_janus_id(1 << 100) == 1 << 100
    assert "JanusId" in jrtc.__all__
    schema = TypeAdapter(jrtc.JanusId).json_schema()
    assert schema["type"] == "integer"
    assert schema["exclusiveMinimum"] == 0


def test_plain_lifecycle_boundaries_reject_string_ids() -> None:
    manager: PluginManager[object] = PluginManager()
    plugin = object()
    manager.register(202, plugin)

    assert manager.as_dict() == {202: plugin}
    with pytest.raises(ValueError, match="handle_id"):
        manager.get("202")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="session_id"):
        JanusSession(session_id="101")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="plugin_id"):
        _Plugin(session=object(), plugin_id="202")  # type: ignore[arg-type]
