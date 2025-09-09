from typing import Any
from common.files import build_submission_zip, save_zip_local, send_zip_via_email, upload_zip_via_sftp
from common.validators import validate_submission
import os

async def export_submission(tenant_id: str, payload: dict[str, Any]) -> dict:
    """
    payload:
    {
      "submission_package_id": "abc123",
      "route": "local" | "email" | "sftp",
      "answers": {...},
      "attachments": [{"name":"file1.pdf","content_base64":"..."}]
      // email config or sftp config can be provided per-tenant or env
    }
    """
    package_id = payload.get("submission_package_id") or "no-id"
    answers = payload.get("answers") or {}
    attachments = payload.get("attachments") or []

    # 1) validate
    errors = validate_submission(answers, attachments)
    if errors:
        return {"ok": False, "errors": errors}

    # 2) create zip
    content = build_submission_zip(answers, attachments)

    # 3) deliver
    route = (payload.get("route") or "local").lower()
    if route == "email":
        host = os.getenv("SMTP_HOST", "localhost")
        port = int(os.getenv("SMTP_PORT", "1025"))  # MailHog default
        sender = os.getenv("SMTP_SENDER", "noreply@example.com")
        to = payload.get("email_to") or os.getenv("SUBMIT_EMAIL_TO", "demo@example.com")
        location = send_zip_via_email(host, port, sender, to, f"Submission Pack {package_id}", content)
    elif route == "sftp":
        host = os.getenv("SFTP_HOST", "localhost")
        port = int(os.getenv("SFTP_PORT", "22"))
        user = os.getenv("SFTP_USER", "user")
        password = os.getenv("SFTP_PASSWORD", "pass")
        remote_path = f"/inbound/{tenant_id}/{package_id}.zip"
        location = upload_zip_via_sftp(host, port, user, password, remote_path, content)
    else:
        # local save (dev)
        location = save_zip_local(content, tenant_id, package_id)

    return {"ok": True, "location": location, "package_id": package_id}