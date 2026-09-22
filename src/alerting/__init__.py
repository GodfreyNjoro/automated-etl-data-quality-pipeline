"""Alerting subsystem: email and Slack notifications with suppression."""

from src.alerting.alert_manager import Alert, AlertManager

__all__ = ["Alert", "AlertManager"]
