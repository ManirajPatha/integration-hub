# connectors/d365/paginate.py
from __future__ import annotations
from typing import AsyncGenerator, Tuple, Dict, Any
from connectors.d365.client import d365_get
import httpx

async def paginate_table(
    path: str,
    params: Dict[str, Any] | None = None,
    page_size: int = 200
) -> AsyncGenerator[Tuple[Dict[str, Any], bool], None]:
    """
    Yields (row, page_bumped). page_bumped=True on the first row of each new page.
    Follows @odata.nextLink. Adds Prefer: odata.maxpagesize.
    """
    q = dict(params or {})
    headers = {"Prefer": f"odata.maxpagesize={page_size}"}

    # first page
    j = await d365_get(path, params=q, extra_headers=headers)
    page_bumped = True
    for item in j.get("value", []):
        yield item, page_bumped
        page_bumped = False

    next_link = j.get("@odata.nextLink")
    while next_link:
        # nextLink already contains query, ignore params
        async with httpx.AsyncClient(timeout=30) as cli:
            # d365_get can be used, but we need to honor nextLink fully:
            r = await cli.get(next_link, headers={"Accept":"application/json"})
            r.raise_for_status()
            j = r.json()
        page_bumped = True
        for item in j.get("value", []):
            yield item, page_bumped
            page_bumped = False
        next_link = j.get("@odata.nextLink")