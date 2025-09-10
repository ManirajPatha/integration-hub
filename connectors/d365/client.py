# connectors/d365/client.py
from __future__ import annotations
import httpx
from urllib.parse import urlparse
from common.settings import settings
from common.auth import get_dataverse_token

TIMEOUT = 60  # seconds

def _is_absolute(url: str) -> bool:
    try:
        p = urlparse(url)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False

async def d365_get(path: str, params: dict | None = None, extra_headers: dict | None = None):
    token = await get_dataverse_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if extra_headers:
        headers.update(extra_headers)

    base = f"{settings.d365_org_url}/api/data/v9.2"
    # Support absolute nextLink by not re-prepending base
    url = path if _is_absolute(path) else f"{base}{path}"

    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        r = await cli.get(url, params=params or {}, headers=headers)
        if r.status_code >= 400:
            # include body to help debug
            raise httpx.HTTPStatusError(
                f"{r.status_code} {r.reason_phrase} - {r.text}",
                request=r.request,
                response=r,
            )
        return r.json()
