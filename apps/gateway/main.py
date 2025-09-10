# apps/gateway/main.py
from fastapi import FastAPI, HTTPException, Body, Request
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import json, os, time, tempfile, logging
from dotenv import load_dotenv
from common.cursors import list_cursors, reset_cursors
from connectors.d365.metadata import find_tables, get_table
from common.registry import get_tables, register_tables, set_tables
from fastapi import Query
from connectors.d365.metadata import list_registered_tables

load_dotenv()  # picks up .env from the current working directory

log = logging.getLogger("integration-hub")
logging.basicConfig(level=logging.INFO)

# ---------- Settings (ensure .env is read) ----------
try:
    from common.settings import settings  # uses pydantic-settings with env_file=".env"
except Exception:
    class _S:
        d365_org_url = os.getenv("D365_ORG_URL", "https://example.crm.dynamics.com")
        d365_tenant_id = os.getenv("D365_TENANT_ID", "TENANT")
        d365_client_id = os.getenv("D365_CLIENT_ID", "CLIENT")
        d365_client_secret = os.getenv("D365_CLIENT_SECRET", "SECRET")
        hub_port = int(os.getenv("HUB_PORT", "8080"))
    settings = _S()

def _mask(v: str, head: int = 6, tail: int = 4) -> str:
    if not v or len(v) <= head + tail:
        return v
    return f"{v[:head]}...{v[-tail:]}"

# ---------- D365 client helpers ----------
try:
    from connectors.d365.client import d365_whoami, d365_get
except Exception:
    import httpx
    _TOKEN: Dict[str, Any] = {}

    async def _get_token() -> str:
        # basic cache with expiry if available
        if _TOKEN.get("exp", 0) > time.time():
            return _TOKEN["val"]

        url = f"https://login.microsoftonline.com/{settings.d365_tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": settings.d365_client_id,
            "client_secret": settings.d365_client_secret,
            "grant_type": "client_credentials",
            "scope": f"{settings.d365_org_url}/.default",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.post(url, data=data, headers=headers)
            try:
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                # bubble up useful error text
                raise HTTPException(status_code=r.status_code, detail=f"Token error: {r.text}") from e
            j = r.json()
            _TOKEN["val"] = j["access_token"]
            _TOKEN["exp"] = time.time() + j.get("expires_in", 3600) - 60
            return _TOKEN["val"]

    async def d365_get(path: str, params: Dict[str, Any] | None = None):
        token = await _get_token()
        base = f"{settings.d365_org_url}/api/data/v9.2"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.get(f"{base}{path}", params=params or {}, headers=headers)
            try:
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                raise HTTPException(status_code=r.status_code, detail=f"D365 GET {path} failed: {r.text}") from e
            return r.json()

    async def d365_whoami():
        j = await d365_get("/WhoAmI")
        return True, j

# ---------- Poller stub ----------
try:
    from connectors.d365.ingest import poll_sourcing_events
except Exception:
    async def poll_sourcing_events(tenant_id: str) -> int:
        # Demo: list Accounts. Note: $select and $top must be separate params.
        data = await d365_get("/accounts", params={"$select": "name", "$top": 5})
        return len(data.get("value", []))

# ---------- Models ----------
class AttachmentIn(BaseModel):
    name: str
    url: Optional[str] = None
    byte_size: Optional[int] = None
    content_type: Optional[str] = None

class SubmitRequest(BaseModel):
    submission_package_id: str = Field(..., min_length=3)
    answers: Dict[str, Any]
    attachments: List[AttachmentIn] = []
    route: str = Field("dryrun", description="dryrun | email | sftp")

# ---------- App ----------
app = FastAPI(title="integration-hub", version="0.1.0")

@app.on_event("startup")
def _print_cfg():
    log.info(
        "CFG org=%s tenant=%s client=%s",
        settings.d365_org_url,
        _mask(settings.d365_tenant_id),
        _mask(settings.d365_client_id),
    )

@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "integration-hub",
        "d365_org_url": settings.d365_org_url,
        "tenant": _mask(settings.d365_tenant_id),
        "client": _mask(settings.d365_client_id),
    }

@app.post("/tenants/{tenant_id}/connectors/d365:test")
async def test_d365(tenant_id: str):
    ok, info = await d365_whoami()
    if not ok:
        return {"ok": False, "error": info.get("error", "unknown")}
    return {"ok": True, "whoami": info}

@app.post("/tenants/{tenant}/connectors/d365:poll")
async def poll_generic(tenant: str, body: dict | None = None):
    from common.registry import get_tables
    from connectors.d365.ingest import poll_table

    body = body or {}
    tables = body.get("tables") or get_tables(tenant)
    limit_pages = body.get("limit_pages", 2)
    max_records = body.get("max_records")  # or None
    force_full = body.get("force_full", False)  # <== NEW
    since_iso  = body.get("since_iso")         # <== NEW

    total = 0
    for logical in tables:
        total += await poll_table(
            tenant=tenant,
            logical=logical,
            limit_pages=limit_pages,
            max_records=max_records,
            force_full=force_full,
            since_iso=since_iso,
        )
    return {"ok": True, "count": total, "tables": tables}
    
@app.post("/tenants/{tenant_id}/connectors/d365:pull")
async def pull_items(tenant_id: str):
    try:
        # Use your verified Entity Set Name here. Adjust select fields if needed.
        data = await d365_get("/cr83d_sourcingevents", params={
            "$select": "cr83d_sourcingeventid,cr83d_title,cr83d_status,cr83d_due_at,modifiedon,createdon",
            "$top": 5
        })
        items = data.get("value", [])
        return {"ok": True, "count": len(items), "items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"pull_failed: {e}")

def _validate_answers(answers: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    if not answers.get("event_id"):
        errs.append("event_id is required")
    if not answers.get("supplier_name"):
        errs.append("supplier_name is required")
    email = answers.get("contact_email")
    if not email or "@" not in email:
        errs.append("contact_email is invalid")
    title = answers.get("proposal_title", "")
    if len(title) > 120:
        errs.append("proposal_title too long (max 120)")
    return errs

def _write_submission_zip(tenant_id: str, submission_id: str, answers: dict, attachments: list) -> Path:
    # 1) choose base directory
    base_dir = os.getenv("SUBMISSION_DIR") or tempfile.gettempdir()

    # 2) create per-tenant subfolder (optional but nice for organization)
    out_dir = Path(base_dir) / tenant_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # 3) final path
    out_path = out_dir / f"submission_{submission_id}.zip"

    # 4) write zip contents
    with ZipFile(out_path, "w", compression=ZIP_DEFLATED) as z:
        z.writestr("answers.json", json.dumps(answers, indent=2).encode("utf-8"))
        if attachments:
            z.writestr("attachments_manifest.json", json.dumps(
                [a.model_dump() if hasattr(a, "model_dump") else a for a in attachments],
                indent=2
            ).encode("utf-8"))

    return out_path

@app.post("/tenants/{tenant_id}/connectors/d365/submit")
async def submit_pack(tenant_id: str, req: SubmitRequest = Body(...)):
    # 1) Validate
    errors = _validate_answers(req.answers)
    if errors:
        return {"ok": False, "error": "validation_failed", "details": errors}

    # 2) Build ZIP (dry-run by default)
    zip_path = _write_submission_zip(tenant_id, req.submission_package_id, req.answers, req.attachments)

    # 3) Delivery routes (stubs now)
    if req.route == "dryrun":
        return {"ok": True, "location": f"local:{zip_path}"}
    elif req.route == "email":
        # TODO: integrate SMTP and attach zip_path
        return {"ok": True, "location": f"email:queued:{zip_path.name}"}
    elif req.route == "sftp":
        # TODO: integrate SFTP upload and return remote path
        return {"ok": True, "location": f"sftp:/inbound/{zip_path.name}"}
    else:
        raise HTTPException(status_code=400, detail="Unknown route; use dryrun|email|sftp")
    
@app.get("/")
def hub_root():
    return {
        "service": "integration-hub",
        "endpoints": {
            "health": "/health",
            "test": "POST /tenants/{tenant_id}/connectors/d365:test",
            "poll": "POST /tenants/{tenant_id}/connectors/d365:poll",
            "submit": "POST /tenants/{tenant_id}/connectors/d365/submit",
            "docs": "/docs",
            "redoc": "/redoc"
        }
    }

@app.get("/connectors/d365/tables")
async def list_tables(prefix: str | None = None):
    try:
        return {"ok": True, "tables": await find_tables(prefix)}
    except Exception as e:
        # surface a readable error
        raise HTTPException(status_code=500, detail=f"hub tables failed: {e}")

@app.get("/connectors/d365/tables/{logical}")
async def get_table_meta(logical: str):
    meta = await get_table(logical)
    return {"ok": True, "table": {
        "logical": meta.get("LogicalName"),
        "set": meta.get("EntitySetName"),
        "pk": meta.get("PrimaryIdAttribute"),
        "pname": meta.get("PrimaryNameAttribute"),
    }}

@app.post("/tenants/{tenant}/connectors/d365/tables:register")
async def tables_register(tenant: str, body: dict = Body(...)):
    tables = body.get("tables") or []
    if not isinstance(tables, list) or not tables:
        raise HTTPException(status_code=400, detail="Provide non-empty 'tables' array")
    updated = register_tables(tenant, tables)
    return {"ok": True, "tables": updated}

@app.get("/tenants/{tenant}/connectors/d365/tables/{logical}/rows")
async def read_rows(
    tenant: str,
    logical: str,
    top: int = Query(50, ge=1, le=500),
    skip: int = Query(0, ge=0),
):
    """
    Return raw rows previously ingested by the generic poller.
    Data is read from .runtime/data/{tenant}/{logical}.jsonl
    """
    data_dir = Path(".runtime") / "data" / tenant
    f = data_dir / f"{logical}.jsonl"

    if not f.exists():
        return {"ok": True, "count": 0, "items": []}

    lines = f.read_text(encoding="utf-8").splitlines()
    slice_ = lines[skip : skip + top]
    items = [json.loads(x) for x in slice_]
    return {"ok": True, "count": len(items), "items": items}

from connectors.d365.metadata import list_registered_tables

@app.get("/tenants/{tenant}/connectors/d365/tables")
async def tables_list(tenant: str, prefix: str | None = Query(None)):
    rows = await list_registered_tables(tenant, prefix=prefix)
    return {"ok": True, "tables": rows}


class ResetBody(BaseModel):
    tables: list[str] | None = None

@app.get("/tenants/{tenant}/connectors/d365/cursors")
async def show_cursors(tenant: str):
    return {"ok": True, "cursors": list_cursors(tenant)}

@app.post("/tenants/{tenant}/connectors/d365/cursors:reset")
async def reset_cursors_route(tenant: str, body: ResetBody):
    n = reset_cursors(tenant, body.tables)
    return {"ok": True, "reset": n}