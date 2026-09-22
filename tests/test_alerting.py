"""Tests for the alerting subsystem."""

from __future__ import annotations

from src.alerting import Alert, AlertManager
from src.alerting.channels import EmailChannel, SlackChannel
from src.utils.settings import Settings


def _settings(**overrides) -> Settings:
    base = {
        "alert_email_enabled": False,
        "slack_webhook_url": "",
        "alert_suppression_minutes": 30,
        **overrides,
    }
    return Settings(**base)


def test_channels_report_unconfigured():
    s = _settings()
    assert EmailChannel(s).configured is False
    assert SlackChannel(s).configured is False


def test_email_channel_configured_flag():
    s = _settings(
        alert_email_enabled=True, smtp_host="smtp.example.com",
        alert_email_to="ops@example.com",
    )
    assert EmailChannel(s).configured is True


def test_below_threshold_not_sent():
    mgr = AlertManager(settings=_settings(), min_severity="error")
    result = mgr.send(Alert(key="k", subject="s", body="b", severity="warning"))
    assert result == {}


def test_no_channels_configured_returns_empty():
    mgr = AlertManager(settings=_settings(), min_severity="error")
    result = mgr.send(Alert(key="k", subject="s", body="b", severity="error"))
    assert result == {}  # nothing configured, nothing delivered


def test_dedup_key_stable():
    a1 = Alert(key="k", subject="s", body="b", severity="error")
    a2 = Alert(key="k", subject="different", body="different", severity="error")
    assert a1.dedup_key == a2.dedup_key


def test_suppression_via_repository():
    class FakeRepo:
        def __init__(self):
            self.recorded = []

        def recent_alert_exists(self, key, minutes):
            return True

        def record_alert(self, **kwargs):
            self.recorded.append(kwargs)

    repo = FakeRepo()
    mgr = AlertManager(settings=_settings(), repository=repo, min_severity="error")
    result = mgr.send(Alert(key="k", subject="s", body="b", severity="error"))
    assert result == {}
    assert repo.recorded and repo.recorded[0]["suppressed"] is True


def test_slack_send_success(monkeypatch):
    s = _settings(slack_webhook_url="https://hooks.example.com/xxx")
    channel = SlackChannel(s)

    class FakeResp:
        def raise_for_status(self):
            return None

    monkeypatch.setattr(
        "src.alerting.channels.requests.post", lambda *a, **k: FakeResp()
    )
    assert channel.send("subject", "body", "error") is True
