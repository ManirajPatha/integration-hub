from common.models import SourcingEvent
from datetime import datetime

def _parse_dt(v: str | None):
    if not v:
        return None
    try:
        # Dataverse returns Z times, normalize to aware datetime
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        return None

def map_d365_event(raw: dict, tenant_id: str) -> SourcingEvent:
    return SourcingEvent(
        id         = raw.get("cr83d_sourcingeventid"),
        platform   = "d365",
        tenant_id  = tenant_id,
        title      = raw.get("cr83d_title"),
        status     = raw.get("cr83d_status"),
        created_at = _parse_dt(raw.get("createdon")),
        due_at     = _parse_dt(raw.get("cr83d_due_at")),
        # If you want to carry description along, put it in a meta requirement or extend your schema
        # For now, we can stash it in requirements meta as a single text block if desired.
    )