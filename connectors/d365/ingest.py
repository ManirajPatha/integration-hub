from datetime import datetime, timezone
from connectors.d365.paginate import paginate_table
from connectors.d365.mapping import map_d365_event
from common.cursors import get_cursor, set_cursor

TABLE_PATH = "/cr83d_sourcingevents"  # adjust to your logical name

def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00","Z")

async def poll_sourcing_events(tenant_id: str, limit_pages: int = 2):
    cursor = get_cursor(tenant_id, "cr83d_sourcingevents")  # your resource key
    # Build filter query (simple mode: modifiedon > cursor)
    # If your table uses different column, adjust it.
    flt = None
    if cursor:
        flt = f"modifiedon gt {cursor}"  # cursor must be in OData datetime format

    select_cols = "cr83d_sourcingeventid,cr83d_title,cr83d_status,cr83d_due_at,modifiedon,createdon"
    params = {"$select": select_cols, "$orderby": "modifiedon asc"}
    if flt: params["$filter"] = flt

    pages = 0
    latest_seen = cursor
    async for row in paginate_table(TABLE_PATH, params=params, page_size=200):
        ev = map_d365_event(row, tenant_id)
        print("EVENT:", ev.model_dump())  # TODO: replace with bus.publish(...)
        # track latest modifiedon
        mod = row.get("modifiedon")
        if mod:
            latest_seen = mod
        # (optional) stop after N pages in a manual run
        # handled below by counting pages in pagination layer if needed

        # NOTE: paginate_table yields items, not pages; so we set cursor after loop

    if latest_seen:
        set_cursor(tenant_id, "cr83d_sourcingevents", latest_seen)