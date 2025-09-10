# connectors/d365/auth.py
from __future__ import annotations
import time
import httpx
from common.settings import settings

# very small in-proc cache so we don't hit AAD every call
_token_cache: dict[str, tuple[str, float]] = {}  # {scope: (access_token, expires_at)}

async def get_access_token() -> str:
    """
    Client-credentials flow for Dataverse.
    Scope must be '<org-url>/.default'
    """
    scope = f"{settings.D365_ORG_URL}/.default"

    # 1) cached?
    tok = _token_cache.get(scope)
    now = time.time()
    if tok and tok[1] - 60 > now:  # 60s of slack
        return tok[0]

    # 2) fetch new
    token_url = f"https://login.microsoftonline.com/{settings.D365_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": settings.D365_CLIENT_ID,
        "client_secret": settings.D365_CLIENT_SECRET,
        "scope": scope,
        "grant_type": "client_credentials",
    }
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.post(token_url, data=data)
        r.raise_for_status()
        j = r.json()
        access_token = j["access_token"]
        expires_in = int(j.get("expires_in", 3600))
        _token_cache[scope] = (access_token, now + expires_in)
        return access_token
