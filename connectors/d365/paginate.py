from typing import AsyncIterator, Dict, Any, Optional
from connectors.d365.http import d365_get_json
from common.settings import settings

async def paginate_table(path: str, params: Optional[Dict[str, Any]]=None, page_size: int=500) -> AsyncIterator[Dict[str, Any]]:
    base = f"{settings.d365_org_url}/api/data/v9.2"
    url  = f"{base}{path}"
    q = dict(params or {})
    headers = { "Prefer": f"odata.maxpagesize={page_size}" }

    while True:
        j = await d365_get_json(url, q, headers)
        for item in j.get("value", []):
            yield item
        next_link = j.get("@odata.nextLink")
        if not next_link:
            break
        url = next_link
        q = {}