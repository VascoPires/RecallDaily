from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage

from recalldaily.config import Credentials
from recalldaily.render import Digest


def build_message(digest: Digest, subject: str, creds: Credentials) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = creds.sender
    msg["To"] = ", ".join(creds.recipients)
    # Plain-text part first, HTML as the preferred alternative.
    msg.set_content(digest.text)
    msg.add_alternative(digest.html, subtype="html")
    return msg


def send(msg: EmailMessage, creds: Credentials, host: str, port: int) -> None:
    context = ssl.create_default_context()
    if port == 465:
        with smtplib.SMTP_SSL(host, port, context=context, timeout=30) as server:
            server.login(creds.user, creds.password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=30) as server:
            server.starttls(context=context)
            server.login(creds.user, creds.password)
            server.send_message(msg)
