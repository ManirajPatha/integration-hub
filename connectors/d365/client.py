# connectors/d365/client.py
from __future__ import annotations
import asyncio
import httpx
from urllib.parse import urlparse
from typing import Optional, Dict, Any
from common.settings import settings
from common.auth import get_dataverse_token

TIMEOUT = 60  # seconds
RETRIES = 3
BACKOFF_BASE = 0.8  # seconds

def _is_absolute(url: str) -> bool:
    try:
        p = urlparse(url)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False

def _needs_consistency(params: Optional[Dict[str, Any]]) -> bool:
    if not params:
        return False
    # $count can be bool or string; treat truthy as needing the header
    v = params.get("$count")
    return bool(v) and str(v).lower() != "false"

async def _request(method: str, url_or_path: str,
                   params: Optional[Dict[str, Any]] = None,
                   json: Any = None,
                   extra_headers: Optional[Dict[str, str]] = None,
                   max_page_size: Optional[int] = None):
    token = await get_dataverse_token()

    base = f"{settings.d365_org_url.rstrip('/')}/api/data/v9.2"
    is_abs = _is_absolute(url_or_path)
    url = url_or_path if is_abs else f"{base}{url_or_path}"

    # If this is a nextLink (absolute), DO NOT append params again.
    effective_params = None if is_abs else (params or {})

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    if _needs_consistency(effective_params):
        headers["ConsistencyLevel"] = "eventual"
    if max_page_size:
        headers["Prefer"] = f"odata.maxpagesize={int(max_page_size)}"
    if extra_headers:
        headers.update(extra_headers)

    last_exc = None
    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        for attempt in range(1, RETRIES + 1):
            try:
                r = await cli.request(method, url, params=effective_params, json=json, headers=headers)
                # Fast path
                if r.status_code < 400:
                    return r.json()
                # Throttle or transient
                if r.status_code in (429, 502, 503, 504):
                    # Honor Retry-After if present
                    ra = r.headers.get("Retry-After")
                    delay = float(ra) if ra else BACKOFF_BASE * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                    continue
                # Other errors -> raise with body for debugging
                raise httpx.HTTPStatusError(
                    f"{r.status_code} {r.reason_phrase} - {r.text}",
                    request=r.request,
                    response=r,
                )
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                last_exc = e
                # retry transient network errors
                if attempt < RETRIES:
                    await asyncio.sleep(BACKOFF_BASE * (2 ** (attempt - 1)))
                    continue
                raise
        # If we somehow fall out of loop
        if last_exc:
            raise last_exc

async def d365_get(path_or_nextlink: str,
                   params: Optional[Dict[str, Any]] = None,
                   extra_headers: Optional[Dict[str, str]] = None,
                   max_page_size: Optional[int] = None):
    """
    GET wrapper. If you pass an absolute @odata.nextLink, do NOT pass params.
    """
    # guard: if absolute AND params provided, ignore to prevent duplication
    if _is_absolute(path_or_nextlink):
        params = None
    return await _request("GET", path_or_nextlink, params=params,
                          extra_headers=extra_headers, max_page_size=max_page_size)

async def d365_post(path: str,
                    payload: Any,
                    extra_headers: Optional[Dict[str, str]] = None):
    """
    POST wrapper for actions/operations.
    """
    return await _request("POST", path, json=payload, extra_headers=extra_headers)