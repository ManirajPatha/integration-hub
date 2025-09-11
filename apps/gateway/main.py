# apps/gateway/main.py
import csv
import io
from fastapi import FastAPI, HTTPException, Body, Request
from pydantic import BaseModel, Field
from typing import List, Literal, Optional, Dict, Any
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import json, os, time, tempfile, logging
from dotenv import load_dotenv
from common.cursors import list_cursors, reset_cursors, set_cursor
from common.files import save_bytes_local, upload_zip_via_sftp
from connectors.d365.metadata import find_tables, get_table, read_table_rows_generic
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

class PollRequest(BaseModel):
    tables: Optional[List[str]] = Field(default=None, description="Logical names to poll; if omitted, use registered tables")
    limit_pages: int = Field(default=2, ge=1, le=50)
    max_records: Optional[int] = Field(default=None, ge=1)
    force_full: bool = Field(default=False, description="Ignore stored cursor and read from start")
    since_iso: Optional[str] = Field(default=None, description="Override cursor once (ISO Z, e.g. 2025-09-08T21:54:24Z)")

@app.post("/tenants/{tenant}/connectors/d365:poll")
async def poll_generic(
    tenant: str,
    # allow passing via querystring for quick Postman testing
    q_force_full: bool = Query(False, alias="force_full"),
    q_limit_pages: int = Query(2, ge=1, le=50, alias="limit_pages"),
    q_max_records: Optional[int] = Query(None, ge=1, alias="max_records"),
    q_since_iso: Optional[str] = Query(None, alias="since_iso"),
    body: Optional[PollRequest] = Body(None),
):
    """
    Polls one or more logical tables for the given tenant.
    Priority: query string overrides body for quick testing.
    """
    from common.registry import get_tables
    from connectors.d365.ingest import poll_table

    # 1) Merge body + query params (queries win for easy Postman use)
    req = body or PollRequest()
    force_full   = q_force_full   if q_force_full is not None else req.force_full
    limit_pages  = q_limit_pages  if q_limit_pages is not None else req.limit_pages
    max_records  = q_max_records  if q_max_records is not None else req.max_records
    since_iso    = q_since_iso    if q_since_iso else req.since_iso
    tables       = req.tables or get_tables(tenant)

    # 2) Guard: must have at least one table
    if not tables:
        raise HTTPException(status_code=400, detail=f"No tables registered for tenant '{tenant}'. Register via POST /tenants/{tenant}/connectors/d365/tables:register")

    # 3) Poll each table
    total = 0
    for logical in tables:
        # helpful log (you can replace print with your logger)
        print(f"[poll] tenant={tenant} table={logical} force_full={force_full} since={since_iso} limit_pages={limit_pages} max_records={max_records}")
        total += await poll_table(
            tenant=tenant,
            logical=logical,
            limit_pages=limit_pages,
            max_records=max_records,
            force_full=force_full,
            since_iso=since_iso,
        )

    return {"ok": True, "count": total, "tables": tables, "force_full": force_full, "since_iso": since_iso}
    
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
        "logical": meta.get("logical"),
        "set": meta.get("set"),
        "pk": meta.get("pk"),
        "pname": meta.get("pname"),
    }}

@app.post("/tenants/{tenant}/connectors/d365/tables:register")
async def tables_register(tenant: str, body: dict = Body(...)):
    tables = body.get("tables") or []
    if not isinstance(tables, list) or not tables:
        raise HTTPException(status_code=400, detail="Provide non-empty 'tables' array")
    updated = register_tables(tenant, tables)
    return {"ok": True, "tables": updated}

@app.get("/tenants/{tenant_id}/connectors/d365/tables/{logical}/rows")
async def rows(
    tenant_id: str,
    logical: str,
    top: int = 50,
    page_token: str | None = None,  # new
):
    res = await read_table_rows_generic(logical, top=top, page_token=page_token)
    items = res.get("items", [])
    next_token = res.get("next_page_token")
    return {"ok": True, "count": len(items), "items": items, "next_page_token": next_token}

@app.post("/tenants/{tenant_id}/connectors/d365/tables/{logical}/export")
async def export_table(
    tenant_id: str,
    logical: str,
    format: Literal["json", "csv"] = Query("json"),
    route: Literal["local", "email", "sftp"] = Query("local"), # type: ignore
    select: Optional[str] = Query(None, description="Comma-separated columns to select"),
    top: int = Query(1000, ge=1, le=5000),
):
    """
    Export a D365 table as JSON or CSV.
    - format: json|csv
    - route: local|email|sftp (same delivery style as submissions)
    - select: optional "$select" columns, e.g. "cr83d_emp_id,cr83d_name,createdon"
    - top: max rows to fetch (simple single-page export)
    """

    # 1) Resolve entity set from logical name
    meta = await get_table(logical)
    set_name = meta["set"]
    if not set_name:
        raise HTTPException(status_code=404, detail=f"Unknown table: {logical}")

    # 2) Fetch rows
    params = {"$top": str(top)}
    if select:
        params["$select"] = select
    data = await d365_get(f"/{set_name}", params=params)
    rows = data.get("value", [])

    # 3) Build file bytes
    ts = time.strftime("%Y%m%d_%H%M%S")
    if format == "json":
        content = json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8")
        filename = f"{logical}_{ts}.json"
        mime = ("application", "json")
    else:
        # CSV: flatten dicts; ignore OData metadata keys
        if rows:
            fieldnames = sorted({k for r in rows for k in r.keys() if not k.startswith("@")})
        else:
            fieldnames = []
        sio = io.StringIO(newline="")
        writer = csv.DictWriter(sio, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
        content = sio.getvalue().encode("utf-8")
        filename = f"{logical}_{ts}.csv"
        mime = ("text", "csv")

    # 4) Deliver: local / email / sftp
    if route == "local":
        location = save_bytes_local(content, tenant_id, filename)
    elif route == "email":
        host = os.getenv("SMTP_HOST", "localhost")
        port = int(os.getenv("SMTP_PORT", "1025"))
        sender = os.getenv("SMTP_SENDER", "noreply@example.com")
        to = os.getenv("SUBMIT_EMAIL_TO", "demo@example.com")
        subject = f"D365 Export {logical} ({ts})"
        location = send_bytes_via_email(host, port, sender, to, subject, filename, content, *mime) # type: ignore
    else:  # sftp
        host = os.getenv("SFTP_HOST", "localhost")
        port = int(os.getenv("SFTP_PORT", "22"))
        user = os.getenv("SFTP_USER", "user")
        password = os.getenv("SFTP_PASSWORD", "pass")
        remote_path = f"/inbound/{tenant_id}/exports/{filename}"
        # Reuse upload_zip_via_sftp for any bytes: quick adaptation:
        # Wrap in-memory content as a "file"; paramiko write works the same
        # so we can call a small sibling function OR inline here:
        location = upload_zip_via_sftp(host, port, user, password, remote_path, content)

    return {"ok": True, "format": format, "count": len(rows), "location": location, "file": filename}



class ResetBody(BaseModel):
    tables: Optional[List[str]] = Field(
        default=None,
        description="List of logical table names to reset. If omitted, resets all registered tables."
    )

@app.get("/tenants/{tenant}/connectors/d365/cursors")
async def show_cursors(tenant: str):
    """
    Returns all stored cursors for this tenant, keyed by the same resource keys
    your poller uses (typically the entity set names).
    """
    return {"ok": True, "cursors": list_cursors(tenant)}

@app.post("/tenants/{tenant}/connectors/d365/cursors:reset")
async def reset_cursors_route(tenant: str, body: ResetBody):
    """
    Resets cursors for the provided logical tables, or for all registered tables if none provided.
    We resolve logical -> set name to match the poller’s resource_key.
    """
    # 1) Determine which logical tables to operate on
    logicals = body.tables or register_tables(tenant)
    if not logicals:
        raise HTTPException(status_code=400, detail=f"No tables provided and no registered tables for tenant '{tenant}'.")

    # 2) Resolve logical -> set names (cursor keys)
    sets: List[str] = []
    for logical in logicals:
        meta = await get_table(logical)  # {"logical","set",...}
        if not meta or not meta.get("set"):
            raise HTTPException(status_code=400, detail=f"Unknown table '{logical}'")
        sets.append(meta["set"])

    # 3) Clear each cursor (None or "" depending on your storage)
    cleared: Dict[str, bool] = {}
    for set_name in sets:
        set_cursor(tenant, set_name, None)  # use "" if your impl requires str
        cleared[set_name] = True

    return {"ok": True, "reset": len(sets), "resources": cleared}