"""Tests for logging configuration and notification channel edge cases."""

from __future__ import annotations

import json
import logging

from src.alerting.channels import EmailChannel, SlackChannel
from src.utils.logging_config import JsonFormatter, configure_logging, get_logger
from src.utils.settings import Settings


def test_json_formatter_includes_context():
    formatter = JsonFormatter()
    record = logging.LogRecord(
        "test", logging.INFO, __file__, 1, "hello", (), None
    )
    record.__dict__["run_id"] = "abc"
    payload = json.loads(formatter.format(record))
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["run_id"] == "abc"


def test_json_formatter_with_exception():
    formatter = JsonFormatter()
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord(
            "test", logging.ERROR, __file__, 1, "failed", (), sys.exc_info()
        )
    payload = json.loads(formatter.format(record))
    assert "exception" in payload


def test_configure_logging_plain_and_json():
    configure_logging(level="DEBUG", json_output=False)
    assert logging.getLogger().level == logging.DEBUG
    configure_logging(level="INFO", json_output=True)
    assert logging.getLogger().level == logging.INFO
    log = get_logger("x")
    log.info("structured", extra={"k": "v"})  # should not raise


def _settings(**overrides) -> Settings:
    base = dict(alert_email_enabled=False, slack_webhook_url="")
    base.update(overrides)
    return Settings(**base)


def test_email_send_uses_smtp(monkeypatch):
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout=15):
            sent["host"] = host

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def ehlo(self):
            pass

        def starttls(self):
            pass

        def login(self, user, pw):
            sent["login"] = user

        def sendmail(self, frm, to, msg):
            sent["to"] = to

    monkeypatch.setattr("src.alerting.channels.smtplib.SMTP", FakeSMTP)
    s = _settings(
        alert_email_enabled=True, smtp_host="smtp.example.com",
        smtp_user="u", smtp_password="p", alert_email_to="a@x.com,b@x.com",
    )
    assert EmailChannel(s).send("subj", "body", "error") is True
    assert sent["to"] == ["a@x.com", "b@x.com"]


def test_email_send_handles_failure(monkeypatch):
    def boom(*a, **k):
        raise OSError("no smtp")

    monkeypatch.setattr("src.alerting.channels.smtplib.SMTP", boom)
    s = _settings(
        alert_email_enabled=True, smtp_host="smtp.example.com",
        alert_email_to="a@x.com",
    )
    assert EmailChannel(s).send("subj", "body") is False


def test_slack_send_handles_failure(monkeypatch):
    def boom(*a, **k):
        raise ConnectionError("down")

    monkeypatch.setattr("src.alerting.channels.requests.post", boom)
    s = _settings(slack_webhook_url="https://hooks.example.com/x")
    assert SlackChannel(s).send("subj", "body") is False


def test_unconfigured_channels_return_false():
    s = _settings()
    assert EmailChannel(s).send("s", "b") is False
    assert SlackChannel(s).send("s", "b") is False
