"""Alert manager: routing, thresholds, suppression and deduplication.

The manager decides *whether* an alert should fire (severity thresholds),
prevents alert storms via time-window suppression backed by the metadata
database, and fans a notification out to every configured channel.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.alerting.channels import EmailChannel, NotificationChannel, SlackChannel
from src.utils.logging_config import get_logger
from src.utils.settings import Settings, get_settings

if TYPE_CHECKING:  # pragma: no cover
    from src.metadata.repository import MetadataRepository

_SEVERITY_ORDER = {"info": 0, "warning": 1, "error": 2, "critical": 3}


@dataclass
class Alert:
    """A notification to be evaluated and potentially delivered."""

    key: str
    subject: str
    body: str
    severity: str = "error"
    context: dict = field(default_factory=dict)

    @property
    def dedup_key(self) -> str:
        """Stable hash used for suppression across identical alerts."""
        digest = hashlib.sha1(
            f"{self.key}|{self.severity}".encode()
        ).hexdigest()[:16]
        return f"{self.key}:{digest}"


class AlertManager:
    """Coordinate alert delivery across channels with suppression."""

    def __init__(
        self,
        settings: Settings | None = None,
        repository: MetadataRepository | None = None,
        min_severity: str = "error",
    ) -> None:
        self.settings = settings or get_settings()
        self.repository = repository
        self.min_severity = min_severity
        self.log = get_logger("alerting.manager")
        self.channels: list[NotificationChannel] = [
            EmailChannel(self.settings),
            SlackChannel(self.settings),
        ]

    def _meets_threshold(self, severity: str) -> bool:
        return _SEVERITY_ORDER.get(severity, 2) >= _SEVERITY_ORDER.get(
            self.min_severity, 2
        )

    def _is_suppressed(self, alert: Alert) -> bool:
        if self.repository is None:
            return False
        window = self.settings.alert_suppression_minutes
        return self.repository.recent_alert_exists(alert.dedup_key, window)

    def send(self, alert: Alert) -> dict[str, bool]:
        """Evaluate and dispatch an alert. Returns per-channel delivery status."""
        results: dict[str, bool] = {}

        if not self._meets_threshold(alert.severity):
            self.log.info(
                "Alert below severity threshold; skipping",
                extra={"key": alert.key, "severity": alert.severity},
            )
            return results

        if self._is_suppressed(alert):
            self.log.info(
                "Alert suppressed (recent duplicate)",
                extra={"key": alert.key, "severity": alert.severity},
            )
            if self.repository is not None:
                self.repository.record_alert(
                    alert_key=alert.dedup_key,
                    channel="all",
                    severity=alert.severity,
                    subject=alert.subject,
                    body=alert.body,
                    sent=False,
                    suppressed=True,
                )
            return results

        any_sent = False
        for channel in self.channels:
            if not channel.configured:
                continue
            ok = channel.send(alert.subject, alert.body, alert.severity)
            results[channel.name] = ok
            any_sent = any_sent or ok
            if self.repository is not None:
                self.repository.record_alert(
                    alert_key=alert.dedup_key,
                    channel=channel.name,
                    severity=alert.severity,
                    subject=alert.subject,
                    body=alert.body,
                    sent=ok,
                    suppressed=False,
                )

        if not any_sent:
            self.log.warning(
                "No channels delivered the alert",
                extra={"key": alert.key, "severity": alert.severity},
            )
        return results
