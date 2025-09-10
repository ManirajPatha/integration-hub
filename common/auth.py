# common/auth.py
from __future__ import annotations
import httpx
from common.settings import settings

TIMEOUT = 60  # seconds

async def get_dataverse_token() -> str:
    """
    Client credentials flow for Dataverse: scope = <org_url>/.default
    """
    tenant = settings.d365_tenant_id
    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    scope = f"{settings.d365_org_url}/.default"

    data = {
        "client_id": settings.d365_client_id,
        "client_secret": settings.d365_client_secret,
        "grant_type": "client_credentials",
        "scope": scope,
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        r = await cli.post(token_url, data=data)
        r.raise_for_status()
        j = r.json()
        return j["access_token"]
