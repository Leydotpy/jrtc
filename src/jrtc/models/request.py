"""Typed, plugin-agnostic outbound Janus protocol envelopes."""

from __future__ import annotations

from typing import Annotated, Any, Final, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from jrtc.models.base import Jsep
from jrtc.models.common import JanusId

MAX_TRICKLE_CANDIDATES: Final = 256


def _transaction_id() -> str:
    return uuid4().hex


class BaseJanusRequest(BaseModel):
    """Fields shared by every Janus request.

    ``token`` and ``apisecret`` are part of the outer envelope and are required
    on every request when the corresponding Janus authentication mode is
    enabled.  A session may populate them automatically without plugin code
    knowing the credentials.
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
    )

    janus: str
    transaction: str = Field(default_factory=_transaction_id, min_length=1, max_length=256)
    token: str | None = Field(default=None, min_length=1, repr=False)
    apisecret: str | None = Field(default=None, min_length=1, repr=False)


class CreateSessionRequest(BaseJanusRequest):
    janus: Literal["create"] = "create"


class KeepAliveRequest(BaseJanusRequest):
    janus: Literal["keepalive"] = "keepalive"
    session_id: JanusId


class ClaimSessionRequest(BaseJanusRequest):
    janus: Literal["claim"] = "claim"
    session_id: JanusId


class DestroySessionRequest(BaseJanusRequest):
    janus: Literal["destroy"] = "destroy"
    session_id: JanusId


class AttachPluginRequest(BaseJanusRequest):
    janus: Literal["attach"] = "attach"
    session_id: JanusId
    plugin: str = Field(min_length=1)
    opaque_id: str | None = None


class DetachPluginRequest(BaseJanusRequest):
    janus: Literal["detach"] = "detach"
    session_id: JanusId
    handle_id: JanusId


class PluginMessageRequest(BaseJanusRequest):
    """Outer envelope for any external plugin's validated body."""

    janus: Literal["message"] = "message"
    session_id: JanusId
    handle_id: JanusId
    body: dict[str, Any]
    jsep: Jsep | None = None


class TrickleCandidate(BaseModel):
    """One ICE candidate or the end-of-candidates marker.

    Janus expects ``{"completed": true}`` directly as the candidate object;
    nesting it under a second ``candidate`` key is a protocol error.
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
    )

    candidate: str | None = Field(default=None, strict=True, min_length=1)
    completed: Literal[True] | None = None
    sdp_mid: str | None = Field(default=None, alias="sdpMid", strict=True, min_length=1)
    sdp_mline_index: int | None = Field(
        default=None,
        alias="sdpMLineIndex",
        strict=True,
        ge=0,
    )

    @field_validator("completed", mode="before")
    @classmethod
    def _validate_completed_type(cls, value: Any) -> Any:
        # Pydantic otherwise accepts the integer ``1`` for ``Literal[True]``.
        # The Janus completion marker is a JSON boolean, not a truthy sentinel.
        if value is not None and value is not True:
            raise ValueError("completed must be the boolean true")
        return value

    @model_validator(mode="after")
    def _validate_shape(self) -> TrickleCandidate:
        if (self.candidate is None) == (self.completed is None):
            raise ValueError("provide exactly one of candidate or completed=true")
        if self.completed is not None and (
            self.sdp_mid is not None or self.sdp_mline_index is not None
        ):
            raise ValueError("completed candidates cannot include SDP indexes")
        return self


class TrickleRequest(BaseJanusRequest):
    janus: Literal["trickle"] = "trickle"
    candidate: TrickleCandidate | None = None
    candidates: list[TrickleCandidate] | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_TRICKLE_CANDIDATES,
    )

    @model_validator(mode="after")
    def _validate_candidate_container(self) -> TrickleRequest:
        if (self.candidate is None) == (self.candidates is None):
            raise ValueError("provide exactly one of candidate or candidates")
        return self


class TrickleMessageRequest(TrickleRequest):
    session_id: JanusId
    handle_id: JanusId


class HangupRequest(BaseJanusRequest):
    janus: Literal["hangup"] = "hangup"
    session_id: JanusId
    handle_id: JanusId


class InfoRequest(BaseJanusRequest):
    janus: Literal["info"] = "info"


class PingRequest(BaseJanusRequest):
    janus: Literal["ping"] = "ping"


# Compatibility aliases retained for external packages that used these names.
type PluginRequestBody = dict[str, Any]
PluginJespMessageRequest = PluginMessageRequest

type JanusRequest = Annotated[
    CreateSessionRequest
    | KeepAliveRequest
    | ClaimSessionRequest
    | DestroySessionRequest
    | AttachPluginRequest
    | DetachPluginRequest
    | PluginMessageRequest
    | TrickleMessageRequest
    | HangupRequest
    | InfoRequest
    | PingRequest,
    Field(discriminator="janus"),
]
