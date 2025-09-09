import httpx, asyncio, math, random
from typing import Dict, Any, Optional
from common.settings import settings
from connectors.d365.client import _get_token

async def d365_get_json(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str,str]] = None):
    base_headers = {
        "Authorization": f"Bearer {await _get_token()}",
        "Accept": "application/json"
    }
    if headers: base_headers.update(headers)

    attempt = 0
    while True:
        try:
            async with httpx.AsyncClient(timeout=30) as cli:
                r = await cli.get(url, params=params or {}, headers=base_headers)
            if r.status_code in (429, 503, 502, 504):
                retry_after = int(r.headers.get("Retry-After", "0") or 0)
                backoff = max(retry_after, min(30, int(2 ** attempt + random.random())))
                await asyncio.sleep(backoff)
                attempt += 1
                continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            if attempt >= 5:  # give up after 6 tries
                raise
            await asyncio.sleep(min(30, int(2 ** attempt + random.random())))
            attempt += 1