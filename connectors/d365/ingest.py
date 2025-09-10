# connectors/d365/ingest.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from connectors.d365.metadata import get_table
from connectors.d365.paginate import paginate_table
from connectors.d365.mapping import map_d365_event
from common.cursors import get_cursor, set_cursor
from pathlib import Path
import json

# ---- Configure your custom table + columns here ----
TABLE_PATH = "/cr83d_sourcingevents"  # entity set (plural) name
SELECT = (
    "cr83d_sourcingeventid,cr83d_title,cr83d_status,"
    "cr83d_due_at,modifiedon,createdon"
)

# ------------- helpers ----------------
def _iso(dt: datetime) -> str:
    """Return Dataverse-friendly Zulu timestamp (no micros)."""
    return dt.replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def _is_iso_z(ts: str) -> bool:
    """Light validator for 'YYYY-MM-DDTHH:MM:SSZ' style strings."""
    try:
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return True
    except Exception:
        return False

def _max_dt(a: Optional[str], b: Optional[str]) -> Optional[str]:
    """Return the later of two ISO Z timestamps (or the non-None one)."""
    if not a: return b
    if not b: return a
    da = datetime.fromisoformat(a.replace("Z", "+00:00"))
    db = datetime.fromisoformat(b.replace("Z", "+00:00"))
    return a if da >= db else b

# ------------- main poller ----------------
async def poll_sourcing_events(
    tenant_id: str,
    limit_pages: int = 2,
    max_records: Optional[int] = None,
    force_full: bool = False,
    since_iso: Optional[str] = None,
) -> int:
    """
    Poll the custom table and publish/print mapped events.
    - Uses a persisted cursor on 'modifiedon' unless force_full=True.
    - You can seed a one-off starting point with since_iso (ISO Z).
    - Returns number of records processed (int).

    Args:
        tenant_id: logical tenant key.
        limit_pages: stop after N pages (defensive for manual runs).
        max_records: optional cap on total items processed.
        force_full: ignore stored cursor; start from beginning.
        since_iso: ISO-Z string (e.g., '2025-09-08T21:54:24Z') to override cursor for this call only.

    Cursor rules (in priority order):
      1) if force_full -> no $filter
      2) elif since_iso (valid) -> filter from since_iso
      3) elif stored cursor -> filter from stored cursor
      4) else -> no $filter (first-time full catch-up)
    """

    # 1) Load existing cursor (string ISO-Z or None)
    stored_cursor = get_cursor(tenant_id, "cr83d_sourcingevents")

    # 2) Decide effective starting point
    effective_cursor: Optional[str] = None
    if not force_full:
        if since_iso and _is_iso_z(since_iso):
            effective_cursor = since_iso
        elif stored_cursor and _is_iso_z(stored_cursor):
            effective_cursor = stored_cursor

    # 3) Build query params
    params = {"$select": SELECT, "$orderby": "modifiedon asc"}
    if effective_cursor:
        # Dataverse OData v4 accepts datetimeoffset literals like 2025-09-08T21:54:24Z (no quotes).
        # Add a not-null guard in case some rows have null modifiedon.
        params["$filter"] = f"(modifiedon ne null) and (modifiedon gt {effective_cursor})"

    processed = 0
    pages_seen = 0
    latest_seen: Optional[str] = effective_cursor

    # 4) Iterate with pagination
    async for row, page_bumped in paginate_table(TABLE_PATH, params=params, page_size=200):
        if page_bumped:
            pages_seen += 1
            if limit_pages and pages_seen > limit_pages:
                break

        # Skip rows missing modifiedon (should be rare)
        mod = row.get("modifiedon")
        if not mod:
            # You could choose to use createdon here as a fallback
            mod = row.get("createdon")

        # Map to canonical model (and later publish to the bus)
        ev = map_d365_event(row, tenant_id)
        print("EVENT:", ev.model_dump())  # TODO: replace with bus.publish(...)

        # Update cursor tracker with the latest 'modifiedon' we saw
        if mod:
            latest_seen = _max_dt(latest_seen, mod)

        processed += 1
        if max_records and processed >= max_records:
            break

    # 5) Persist updated cursor only if we advanced
    if latest_seen and latest_seen != stored_cursor:
        set_cursor(tenant_id, "cr83d_sourcingevents", latest_seen)

    return processed


# ------------- convenience variant (no cursor) -------------
async def poll_sourcing_events_no_cursor(
    tenant_id: str,
    limit_pages: int = 2,
    max_records: Optional[int] = None,
) -> int:
    """Convenience helper to process all rows ignoring any stored cursor."""
    return await poll_sourcing_events(
        tenant_id=tenant_id,
        limit_pages=limit_pages,
        max_records=max_records,
        force_full=True,
        since_iso=None,
    )

def _store_raw_row(tenant: str, logical: str, row: dict):
    p = Path(".runtime") / "data" / tenant
    p.mkdir(parents=True, exist_ok=True)
    (p / f"{logical}.jsonl").open("a", encoding="utf-8").write(
        json.dumps(row, ensure_ascii=False) + "\n"
    )

def _max_iso(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if not a: return b
    if not b: return a
    from datetime import datetime
    da = datetime.fromisoformat(a.replace("Z","+00:00"))
    db = datetime.fromisoformat(b.replace("Z","+00:00"))
    return a if da >= db else b

async def poll_table(
    tenant: str,
    logical: str,
    limit_pages: int = 2,
    max_records: Optional[int] = None,
    force_full: bool = False,
    since_iso: Optional[str] = None,
) -> int:
    """
    Generic poller for ANY table by logical name (e.g., 'cr83d_sourcingevent').
    Persists a cursor on 'modifiedon' per (tenant, logical).
    """
    meta = await get_table(logical)  # uses EntityDefinitions(LogicalName='...')
    set_name = meta["EntitySetName"]

    # decide cursor
    stored = get_cursor(tenant, logical)
    effective = None
    if not force_full:
        effective = since_iso or stored

    params = {"$orderby": "modifiedon asc"}
    if effective:
        params["$filter"] = f"(modifiedon ne null) and (modifiedon gt {effective})"

    processed = 0
    pages = 0
    latest = effective

    async for row, page_bumped in paginate_table(f"/{set_name}", params=params, page_size=200):
        if page_bumped:
            pages += 1
            if limit_pages and pages > limit_pages:
                break

        mod = row.get("modifiedon") or row.get("createdon")
        _store_raw_row(tenant, logical, row)
        if mod:
            latest = _max_iso(latest, mod)

        processed += 1
        if max_records and processed >= max_records:
            break

    if latest and latest != stored:
        set_cursor(tenant, logical, latest)

    return processed