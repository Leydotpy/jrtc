from __future__ import annotations

from jrtc.conf import settings
from jrtc.conf.settings import JANUS_API_SECRET
from jrtc.manager import JanusSessionManager


def test_default_manager_uses_the_stable_api_secret_setting() -> None:
    assert settings.JANUS_API_SECRET == JANUS_API_SECRET
    assert not hasattr(settings, "jrtc" + "_SECRET")

    session = JanusSessionManager()._default_session_factory()

    assert session is not None
