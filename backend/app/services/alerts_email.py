from __future__ import annotations

import smtplib
from datetime import UTC, datetime
from email.message import EmailMessage

from app.config import Settings
from app.models.types import AlertEvent


def send_email_alert(settings: Settings, subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["From"] = settings.alert_from
    msg["To"] = settings.alert_to
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=15) as smtp:
        smtp.starttls()
        if settings.smtp_user:
            smtp.login(settings.smtp_user, settings.smtp_password)
        smtp.send_message(msg)


def send_narrated_brief_email(
    settings: Settings,
    subject: str,
    markdown_body: str,
    html_body: str,
) -> None:
    msg = EmailMessage()
    msg["From"] = settings.alert_from
    msg["To"] = settings.alert_to
    msg["Subject"] = subject
    msg.set_content(markdown_body)
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=15) as smtp:
        smtp.starttls()
        if settings.smtp_user:
            smtp.login(settings.smtp_user, settings.smtp_password)
        smtp.send_message(msg)


def build_critical_alert(reason: str, citations: list[dict]) -> AlertEvent:
    from app.models.types import Citation

    return AlertEvent(
        alert_id=f"critical_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}",
        severity="critical",
        trigger_reason=reason,
        citations=[Citation(**item) for item in citations],
        created_at=datetime.now(UTC),
    )
