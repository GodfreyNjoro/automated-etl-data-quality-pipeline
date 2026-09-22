"""Notification channels: SMTP email and Slack incoming webhooks.

Each channel implements a small :meth:`send` contract and reports success or
failure without raising, so that a broken notification path never brings down a
pipeline run.
"""

from __future__ import annotations

import smtplib
from abc import ABC, abstractmethod
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

from src.utils.logging_config import get_logger
from src.utils.settings import Settings


class NotificationChannel(ABC):
    """Common contract for notification channels."""

    name: str = "channel"

    @abstractmethod
    def send(self, subject: str, body: str, severity: str = "error") -> bool:
        """Send a notification. Returns True on success."""

    @property
    @abstractmethod
    def configured(self) -> bool:
        """Whether the channel has enough configuration to send."""


class EmailChannel(NotificationChannel):
    """Send alerts over SMTP."""

    name = "email"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.log = get_logger("alerting.email")

    @property
    def configured(self) -> bool:
        s = self.settings
        return bool(s.alert_email_enabled and s.smtp_host and s.alert_email_to)

    def send(self, subject: str, body: str, severity: str = "error") -> bool:
        if not self.configured:
            self.log.info("Email channel not configured; skipping")
            return False
        s = self.settings
        recipients = [addr.strip() for addr in s.alert_email_to.split(",") if addr.strip()]
        message = MIMEMultipart()
        message["From"] = s.alert_email_from
        message["To"] = ", ".join(recipients)
        message["Subject"] = f"[{severity.upper()}] {subject}"
        message.attach(MIMEText(body, "plain"))
        try:
            with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=15) as server:
                server.ehlo()
                try:
                    server.starttls()
                    server.ehlo()
                except smtplib.SMTPException:
                    pass  # server may not support STARTTLS (e.g. local relay)
                if s.smtp_user:
                    server.login(s.smtp_user, s.smtp_password)
                server.sendmail(s.alert_email_from, recipients, message.as_string())
            self.log.info("Alert email sent", extra={"recipients": recipients})
            return True
        except Exception as exc:  # noqa: BLE001
            self.log.error("Failed to send alert email", extra={"error": str(exc)})
            return False


class SlackChannel(NotificationChannel):
    """Send alerts to a Slack incoming webhook."""

    name = "slack"

    _COLORS = {"error": "#D50000", "warning": "#FF9800", "info": "#2196F3"}

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.log = get_logger("alerting.slack")

    @property
    def configured(self) -> bool:
        return bool(self.settings.slack_webhook_url)

    def send(self, subject: str, body: str, severity: str = "error") -> bool:
        if not self.configured:
            self.log.info("Slack channel not configured; skipping")
            return False
        payload = {
            "attachments": [
                {
                    "color": self._COLORS.get(severity, "#607D8B"),
                    "title": subject,
                    "text": body,
                    "footer": "ETL Data Quality Monitor",
                }
            ]
        }
        try:
            resp = requests.post(
                self.settings.slack_webhook_url, json=payload, timeout=15
            )
            resp.raise_for_status()
            self.log.info("Alert posted to Slack")
            return True
        except Exception as exc:  # noqa: BLE001
            self.log.error("Failed to post Slack alert", extra={"error": str(exc)})
            return False
