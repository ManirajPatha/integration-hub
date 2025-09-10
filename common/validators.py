import re
from typing import List, Dict, Any

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
GUID_RE  = re.compile(r"^[0-9a-fA-F-]{20,}$")  # lenient; Dataverse GUIDs are 36 chars

def validate_submission(answers: Dict[str, Any], attachments: List[Dict[str, Any]]) -> list[str]:
    errs: list[str] = []

    # requireds
    event_id       = (answers.get("event_id") or "").strip()
    supplier_name  = (answers.get("supplier_name") or "").strip()
    contact_email  = (answers.get("contact_email") or "").strip()
    proposal_title = (answers.get("proposal_title") or "").strip()

    if not event_id:       errs.append("answers.event_id is required")
    if not supplier_name:  errs.append("answers.supplier_name is required")
    if not contact_email:  errs.append("answers.contact_email is required")
    if not proposal_title: errs.append("answers.proposal_title is required")

    # formats / lengths
    if event_id and not GUID_RE.match(event_id):
        errs.append("answers.event_id must look like a GUID")
    if supplier_name and len(supplier_name) > 120:
        errs.append("answers.supplier_name too long (max 120)")
    if proposal_title and len(proposal_title) > 120:
        errs.append("answers.proposal_title too long (max 120)")
    if contact_email and not EMAIL_RE.match(contact_email):
        errs.append("answers.contact_email invalid")

    # attachments checks (optional)
    for a in attachments or []:
        name = (a.get("name") or "").strip()
        if name and len(name) > 200:
            errs.append(f"attachment name too long: {name}")
        bs = a.get("byte_size")
        if isinstance(bs, int) and bs > 25 * 1024 * 1024:
            errs.append(f"attachment too large (>25MB): {name}")

    return errs