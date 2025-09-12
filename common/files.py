import csv
import io
import os
from pathlib import Path
import tempfile
import zipfile
import base64
import smtplib
from email.message import EmailMessage
from typing import Iterable


def build_submission_zip(answers: dict, attachments: Iterable[dict]) -> bytes:
    """
    Build a ZIP in-memory containing:
      - answers.json
      - attachments/<files>

    attachments: iterable of dicts supporting any of:
      { "name": str, "bytes": bytes }
      { "name": str, "content_base64": str }
      { "name": str, "url": str }   # (ignored for now; add a downloader later)
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # answers.json
        import json
        z.writestr("answers.json", json.dumps(answers, ensure_ascii=False, indent=2))

        # attachments/
        for att in attachments or []:
            name = att.get("name") or "file.bin"
            if "bytes" in att and isinstance(att["bytes"], (bytes, bytearray)):
                z.writestr(f"attachments/{name}", att["bytes"])
            elif "content_base64" in att and att["content_base64"]:
                z.writestr(f"attachments/{name}", base64.b64decode(att["content_base64"]))
            # If only URL provided, you can later add a downloader/streamer here
    return buf.getvalue()

def save_rows_csv(rows: list[dict], tenant_id: str, logical: str) -> str:
    base = os.getenv("SUBMISSION_DIR") or tempfile.gettempdir()
    out_dir = Path(base) / tenant_id / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)

    # columns = union of keys across rows
    cols = sorted({k for r in rows for k in r.keys()})
    out = out_dir / f"{logical}.csv"

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    return str(out)



def save_zip_local(content: bytes, tenant_id: str, package_id: str) -> str:
    # Use SUBMISSION_DIR if set, else default temp
    base = os.getenv("SUBMISSION_DIR") or tempfile.gettempdir()
    out_dir = Path(base) / tenant_id   # optional: keep tenant subfolder
    out_dir.mkdir(parents=True, exist_ok=True)

    out = out_dir / f"submission_{package_id}.zip"
    out.write_bytes(content)
    return f"local:{out}"


def send_zip_via_email(smtp_host: str, smtp_port: int, sender: str, to: str, subject: str, content: bytes):
    """
    Send the ZIP bytes as an email attachment using a plain SMTP server.
    For dev, pair with MailHog (localhost:1025).
    """
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content("Submission pack attached.")
    msg.add_attachment(content, maintype="application", subtype="zip", filename="submission.zip")
    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.send_message(msg)
    return f"email:sent:{to}"


# -----------------------------
# SFTP upload (requires paramiko)
# -----------------------------

def _sftp_makedirs(sftp, remote_dir: str):
    """
    Best-effort recursive directory create on SFTP.
    Ignores 'already exists' errors.
    """
    if not remote_dir or remote_dir in ("/", ".", "./"):
        return
    parts = []
    drive, path = os.path.splitdrive(remote_dir.replace("\\", "/"))
    for part in path.split("/"):
        if part and part != ".":
            parts.append(part)
            current = "/" + "/".join(parts)
            try:
                sftp.stat(current)
            except FileNotFoundError:
                try:
                    sftp.mkdir(current)
                except Exception:
                    # concurrent create or permission issue; ignore here
                    pass


def upload_zip_via_sftp(host: str, port: int, user: str, password: str, remote_path: str, content: bytes) -> str:
    """
    Upload ZIP bytes to an SFTP server at remote_path.
    Example remote_path: /inbound/<tenant_id>/<package_id>.zip

    Requires:
        pip install paramiko
    """
    try:
        import paramiko
    except ImportError as e:
        raise RuntimeError(
            "SFTP upload requested but 'paramiko' is not installed. "
            "Run: pip install paramiko"
        ) from e

    # Normalize remote path and ensure parent directories exist
    remote_path = remote_path.replace("\\", "/")
    remote_dir = os.path.dirname(remote_path)

    transport = paramiko.Transport((host, port))
    try:
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        if remote_dir:
            _sftp_makedirs(sftp, remote_dir)

        # Write the file
        with sftp.open(remote_path, "wb") as f:
            f.write(content)

        sftp.close()
    finally:
        transport.close()

    return f"sftp://{host}{remote_path}"

def save_bytes_local(content: bytes, tenant_id: str, filename: str) -> str:
    base = os.getenv("SUBMISSION_DIR") or tempfile.gettempdir()
    out_dir = Path(base) / tenant_id / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    path.write_bytes(content)
    return f"local:{path}"

def send_bytes_via_email(host: str, port: int, sender: str, to: str,
                         subject: str, filename: str,
                         content: bytes, maintype: str, subtype: str) -> str:
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content("Export file attached.")
    msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)
    with smtplib.SMTP(host, port) as s:
        s.send_message(msg)
    return f"email:sent:{to}"