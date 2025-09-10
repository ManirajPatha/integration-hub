# common/settings.py
from __future__ import annotations

import sys
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class Settings(BaseSettings):
    # -------- Hub / server ----------
    hub_port: int = Field(8080, alias="HUB_PORT")

    # -------- D365 / Dataverse (required) ----------
    # We expose them in lowercase, but accept .env UPPERCASE via alias.
    d365_org_url: str = Field(..., alias="D365_ORG_URL")            # e.g. https://org9c010d4b.crm.dynamics.com
    d365_tenant_id: str = Field(..., alias="D365_TENANT_ID")        # GUID
    d365_client_id: str = Field(..., alias="D365_CLIENT_ID")
    d365_client_secret: str = Field(..., alias="D365_CLIENT_SECRET")

    # -------- Output location (optional) ----------
    submission_dir: str | None = Field(default=None, alias="SUBMISSION_DIR")

    # -------- Email (optional) ----------
    smtp_host: str | None = Field(default=None, alias="SMTP_HOST")
    smtp_port: int | None = Field(default=None, alias="SMTP_PORT")
    smtp_sender: str | None = Field(default=None, alias="SMTP_SENDER")
    submit_email_to: str | None = Field(default=None, alias="SUBMIT_EMAIL_TO")

    # -------- SFTP (optional) ----------
    sftp_host: str | None = Field(default=None, alias="SFTP_HOST")
    sftp_port: int | None = Field(default=None, alias="SFTP_PORT")
    sftp_user: str | None = Field(default=None, alias="SFTP_USER")
    sftp_password: str | None = Field(default=None, alias="SFTP_PASSWORD")

    # pydantic-settings config
    model_config = SettingsConfigDict(
        env_file=".env",                 # load env from repo root
        env_file_encoding="utf-8",
        case_sensitive=False,            # allow lower/upper in env
        populate_by_name=True,
        extra="ignore",                  # ignore unknown env keys
    )

    # Basic validation
    @field_validator("d365_org_url")
    @classmethod
    def _must_be_https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("D365_ORG_URL must start with https://")
        return v

    # --------- Backward-compatible UPPERCASE attributes ----------
    # These let other modules keep using settings.D365_ORG_URL, etc.
    @property
    def D365_ORG_URL(self) -> str:
        return self.d365_org_url

    @property
    def D365_TENANT_ID(self) -> str:
        return self.d365_tenant_id

    @property
    def D365_CLIENT_ID(self) -> str:
        return self.d365_client_id

    @property
    def D365_CLIENT_SECRET(self) -> str:
        return self.d365_client_secret

    @property
    def SUBMISSION_DIR(self) -> str | None:
        return self.submission_dir

    @property
    def HUB_PORT(self) -> int:
        return self.hub_port


def _pretty_fail(msg: str) -> None:
    # Print a friendly error once (useful with uvicorn reload)
    print(f"\n[settings] {msg}\n", file=sys.stderr)
    sys.exit(1)


try:
    settings = Settings()
except Exception as e:
    _pretty_fail(
        "Missing or invalid settings. Ensure .env (repo root) contains:\n"
        "  D365_ORG_URL=https://org9c010d4b.crm.dynamics.com\n"
        "  D365_TENANT_ID=<GUID>\n"
        "  D365_CLIENT_ID=<app id>\n"
        "  D365_CLIENT_SECRET=<secret value>\n"
        "Optional:\n"
        "  HUB_PORT=8080\n"
        "  SUBMISSION_DIR=C:/Users/MANIRAJ/OneDrive/Documents/Git/integration-hub/out\n"
        "  SMTP_HOST, SMTP_PORT, SMTP_SENDER, SUBMIT_EMAIL_TO\n"
        "  SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD\n\n"
        f"Raw error: {e}"
    )