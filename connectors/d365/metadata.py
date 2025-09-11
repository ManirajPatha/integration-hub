# connectors/d365/metadata.py
from typing import Any, Optional, List, Dict
from urllib.parse import urlparse
import json

from connectors.d365.client import d365_get
from common.cursors import get_cursor, set_cursor   # <- reuse the simple kv store

# ---------- PAGED TABLE DISCOVERY (already added) ----------

def _split_nextlink(next_link: str) -> str:
    p = urlparse(next_link)
    return f"{p.path}?{p.query}" if p.query else p.path

async def find_tables(prefix: Optional[str] = None) -> List[Dict]:
    """
    Robust version:
    - No $select, no $count, no $orderby, no $top (avoids 0x80060888).
    - Paginates via @odata.nextLink if present.
    - Applies prefix filter client-side (case-insensitive).
    """
    out: List[Dict] = []
    path = "/EntityDefinitions"
    params = None  # <- IMPORTANT: no query params

    norm_prefix = prefix.lower() if prefix else None

    while True:
        j = await d365_get(path, params=params)

        for e in j.get("value", []):
            logical = e.get("LogicalName")
            if not logical:
                continue
            if norm_prefix and not logical.lower().startswith(norm_prefix):
                continue

            out.append({
                "logical": logical,
                "set": e.get("EntitySetName"),
                "pk": e.get("PrimaryIdAttribute"),
                "pname": e.get("PrimaryNameAttribute"),
            })

        # Paging (nextLink can be absolute; d365_get handles it)
        next_link = j.get("@odata.nextLink")
        if not next_link:
            break
        path = next_link
        params = None

    return out

def _get_any(d: Dict, *keys: str):
    """Return the first present (non-None) value among given keys, case-insensitive."""
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    # case-insensitive fallback
    lower = {k.lower(): v for k, v in d.items()}
    for k in keys:
        v = lower.get(k.lower())
        if v is not None:
            return v
    return None

async def get_table(logical: str) -> Dict:
    """
    Robust variant: reuse the already-working paged list (find_tables)
    and pick the matching logical name case-insensitively.
    This avoids single-entity metadata quirks that return empty/null.
    """
    all_tables: List[Dict] = await find_tables()  # no prefix, get everything
    lwr = logical.lower()
    for t in all_tables:
        if (t.get("logical") or "").lower() == lwr:
            return t
    # Not found -> return empty structure (caller can handle)
    return {"logical": None, "set": None, "pk": None, "pname": None}

async def read_table_rows_generic(
    logical: str,
    top: int = 50,
    page_token: Optional[str] = None,   # accept nextLink from caller
) -> Dict[str, Any]:
    """
    - Uses $top only (no $skip).
    - If page_token (an @odata.nextLink) is provided, fetches that page directly.
    - Returns items and next_page_token (if more pages available).
    """
    # If we have a nextLink, just call it directly
    if page_token:
        j = await d365_get(page_token, params=None)
        return {
            "items": j.get("value", []),
            "next_page_token": j.get("@odata.nextLink")
        }

    # First page: resolve table metadata → build minimal $select
    meta = await get_table(logical)  # {logical,set,pk,pname}
    sel_cols = [meta["pk"]] if meta.get("pk") else []
    if meta.get("pname"):
        sel_cols.append(meta["pname"])

    params = {"$top": str(top)}
    if sel_cols:
        params["$select"] = ",".join(sel_cols)

    j = await d365_get(f"/{meta['set']}", params=params)
    return {
        "items": j.get("value", []),
        "next_page_token": j.get("@odata.nextLink")
    }

# ---------- SIMPLE PER-TENANT REGISTRY (bring these back) ----------

_REG_KEY = "d365_registered_tables"

def list_registered_tables(tenant_id: str) -> list[str]:
    """
    Return the list of logical table names previously registered for this tenant.
    Stored using the same lightweight cursor store.
    """
    raw = get_cursor(tenant_id, _REG_KEY)
    if not raw:
        return []
    try:
        return json.loads(raw)
    except Exception:
        # backward compat if older versions stored comma-separated string
        return [s for s in raw.split(",") if s]

def register_tables(tenant_id: str, tables: list[str]) -> list[str]:
    """
    Save/overwrite the set of registered tables for this tenant.
    """
    uniq = sorted(set(tables))
    set_cursor(tenant_id, _REG_KEY, json.dumps(uniq))
    return uniq
