"""Settings from recalldaily.toml (optional) and credentials from the environment."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, fields
from pathlib import Path

DEFAULT_CONFIG_FILE = "recalldaily.toml"


@dataclass
class Settings:
    library: str = "data/recall.db"
    num_quotes: int = 4
    cooldown_days: int = 14
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 465
    subject: str = "Your Recall Daily – {date}"
    date_format: str = "%d.%m.%Y"
    footer: str = "Recall Daily"


def load_settings(path: str | Path | None = None) -> Settings:
    """Load settings; a missing default config file just means 'use defaults'."""
    explicit = path is not None
    path = Path(path or DEFAULT_CONFIG_FILE)
    if not path.exists():
        if explicit:
            raise FileNotFoundError(path)
        return Settings()
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    known = {f.name for f in fields(Settings)}
    unknown = set(data) - known
    if unknown:
        raise ValueError(f"{path}: unknown setting(s): {', '.join(sorted(unknown))}")
    return Settings(**data)


@dataclass
class Credentials:
    user: str
    password: str
    sender: str
    recipients: list[str]


def load_credentials(env: dict[str, str] | None = None) -> Credentials:
    env = os.environ if env is None else env
    missing = [k for k in ("MAIL_USER", "MAIL_PASSWORD", "DAILY_REPORT_TO") if not env.get(k)]
    if missing:
        raise RuntimeError(f"Missing environment variable(s): {', '.join(missing)}")
    return Credentials(
        user=env["MAIL_USER"],
        password=env["MAIL_PASSWORD"],
        sender=env.get("DAILY_REPORT_FROM") or env["MAIL_USER"],
        recipients=[r.strip() for r in env["DAILY_REPORT_TO"].split(",") if r.strip()],
    )
