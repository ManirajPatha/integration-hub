# apps/gateway/main.py
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from pathlib import Path
import json
import os

# ---- Try to import your helpers; provide light fallbacks for dev ----
try:
    from common.settings import settings  # loads D365_ORG_URL, TENANT_ID, CLIENT_ID, SECRET
except Exception:
    class _S:
        d365_org_url = os.getenv("D365_ORG_URL", "https://example.crm.dynamics.com")
        d365_tenant_id = os.getenv("D365_TENANT_ID", "TENANT")
        d365_client_id = os.getenv("D365_CLIENT_ID", "CLIENT")
        d365_client_secret = os.getenv("D365_CLIENT_SECRET", "SECRET")
        hub_port = int(os.getenv("HUB_PORT", "8080"))
    settings = _S()

try:
    from connectors.d365.client import d365_whoami, d365_get
except Exception:
    import httpx, asyncio
    _TOKEN: Dict[str, str] = {}
    async def _get_token() -> str:
        if "token" in _TOKEN:
            return _TOKEN["token"]
        url = f"https://login.microsoftonline.com/{settings.d365_tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": settings.d365_client_id,
            "client_secret": settings.d365_client_secret,
            "grant_type": "client_credentials",
            "scope": f"{settings.d365_org_url}/.default",
        }
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.post(url, data=data, headers={"Content-Type":"application/x-www-form-urlencoded"})
            r.raise_for_status()
            j = r.json()
            _TOKEN["token"] = j["access_token"]
            return j["access_token"]

    async def d365_get(path: str, params: Dict[str, Any] | None = None):
        token = await _get_token()
        base = f"{settings.d365_org_url}/api/data/v9.2"
        async with httpx.AsyncClient(timeout=30) as cli:
            r = await cli.get(f"{base}{path}", params=params or {},
                              headers={"Authorization": f"Bearer {token}", "Accept":"application/json"})
            r.raise_for_status()
            return r.json()

    async def d365_whoami():
        try:
            j = await d365_get("/WhoAmI")
            return True, j
        except Exception as e:
            return False, {"error": str(e)}

try:
    from connectors.d365.ingest import poll_sourcing_events
except Exception:
    async def poll_sourcing_events(tenant_id: str) -> int:
        # Minimal demo poller: list Accounts (replace with your custom table)
        data = await d365_get("/accounts", params={"$select":"name&$top=5"})
        return len(data.get("value", []))

# ---- Models for /submit ----
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

# ---- FastAPI app ----
app = FastAPI(title="integration-hub", version="0.1.0")

@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "integration-hub",
        "d365_org_url": settings.d365_org_url
    }

@app.post("/tenants/{tenant_id}/connectors/d365:test")
async def test_d365(tenant_id: str):
    ok, info = await d365_whoami()
    if not ok:
        return {"ok": False, "error": info.get("error", "unknown")}
    return {"ok": True, "whoami": info}

@app.post("/tenants/{tenant_id}/connectors/d365:poll")
async def poll_now(tenant_id: str):
    try:
        count = await poll_sourcing_events(tenant_id)
        return {"ok": True, "count": int(count)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _validate_answers(answers: Dict[str, Any]) -> List[str]:
    errs = []
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

def _write_submission_zip(submission_id: str, answers: Dict[str, Any], attachments: List[AttachmentIn]) -> Path:
    from zipfile import ZipFile, ZIP_DEFLATED
    from io import BytesIO
    out_path = Path(f"/tmp/submission_{submission_id}.zip")
    # Build answers.json
    answers_bytes = json.dumps(answers, indent=2).encode("utf-8")
    # Write ZIP
    with ZipFile(out_path, "w", compression=ZIP_DEFLATED) as z:
        z.writestr("answers.json", answers_bytes)
        # For now we only record attachment metadata (no downloads in dryrun)
        if attachments:
            meta = [a.model_dump() for a in attachments]
            z.writestr("attachments_manifest.json", json.dumps(meta, indent=2).encode("utf-8"))
    return out_path

@app.post("/tenants/{tenant_id}/connectors/d365/submit")
async def submit_pack(tenant_id: str, req: SubmitRequest = Body(...)):
    # 1) Validate
    errors = _validate_answers(req.answers)
    if errors:
        return {"ok": False, "errors": errors}

    # 2) Build ZIP locally (dryrun)
    zip_path = _write_submission_zip(req.submission_package_id, req.answers, req.attachments)

    # 3) Route (later): email/sftp; for now keep dryrun
    if req.route == "dryrun":
        return {"ok": True, "location": f"local:{zip_path}"}
    elif req.route == "email":
        # TODO: integrate SMTP and send attachment
        return {"ok": True, "location": f"email:queued:{zip_path.name}"}
    elif req.route == "sftp":
        # TODO: integrate SFTP upload and return remote path
        return {"ok": True, "location": f"sftp:/inbound/{zip_path.name}"}
    else:
        raise HTTPException(status_code=400, detail="Unknown route; use dryrun|email|sftp")