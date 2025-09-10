# connectors/d365/metadata.py
from typing import Optional, List, Dict
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

async def get_table(logical: str) -> Dict:
    j = await d365_get(
        f"/EntityDefinitions(LogicalName='{logical}')",
        params={"$select": "LogicalName,EntitySetName,PrimaryIdAttribute,PrimaryNameAttribute"},
    )
    return {
        "logical": j.get("LogicalName"),
        "set": j.get("EntitySetName"),
        "pk": j.get("PrimaryIdAttribute"),
        "pname": j.get("PrimaryNameAttribute"),
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
