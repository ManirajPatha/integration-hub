import io
import os
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


def save_zip_local(content: bytes, tenant_id: str, package_id: str) -> str:
    """
    Save the ZIP bytes to ./out/<tenant_id>/<package_id>.zip and return the absolute path.
    """
    outdir = os.path.abspath(os.path.join(".", "out", tenant_id))
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, f"{package_id}.zip")
    with open(path, "wb") as f:
        f.write(content)
    return path


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