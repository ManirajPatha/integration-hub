# connectors/d365/rows.py (or wherever you read rows)
from typing import Dict, Any, List, Set, Optional
from connectors.d365.client import d365_get, d365_get_absolute
from connectors.d365.metadata import get_table

async def fetch_rows(table_logical: str, top: int = 500) -> Dict[str, Any]:
    """
    Reads all rows for a logical table with safe paging and de-duplication.
    """
    meta = await get_table(table_logical)  # {logical,set,pk,pname}
    set_name = meta["set"]
    pk = meta["pk"]  # e.g., 'cr83d_sourcingeventid'

    # Stable ordering prevents overlap across pages
    params = {
        "$top": str(top),
        "$orderby": "createdon asc, " + pk + " asc"
    }

    items: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    # 1) first page
    page = await d365_get(f"/{set_name}", params=params)

    def add_rows(rows):
        for r in rows:
            rid = r.get(pk)
            if rid and rid not in seen:
                seen.add(rid)
                items.append(r)

    add_rows(page.get("value", []))

    # 2) follow @odata.nextLink exactly, without re-applying params
    next_link: Optional[str] = page.get("@odata.nextLink")
    while next_link:
        page = await d365_get_absolute(next_link)  # absolute URL, no params
        add_rows(page.get("value", []))
        next_link = page.get("@odata.nextLink")

    return {"ok": True, "count": len(items), "items": items}