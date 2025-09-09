# connectors/d365/mapping.py
from common.models import SourcingEvent
from datetime import datetime

def _parse_dt(v):
    if not v: return None
    try: return datetime.fromisoformat(v.replace("Z","+00:00"))
    except: return None

def map_d365_event(raw: dict, tenant_id: str) -> SourcingEvent:
    return SourcingEvent(
        id = raw.get("cr83d_sourcingeventid"),
        platform = "d365",
        tenant_id = tenant_id,
        title = raw.get("cr83d_title"),
        status = raw.get("cr83d_status"),
        created_at = _parse_dt(raw.get("createdon")),
        due_at = _parse_dt(raw.get("cr83d_due_at")),
    )