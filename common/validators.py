def validate_submission(answers: dict, attachments: list[dict]) -> list[str]:
    errors: list[str] = []

    # Required fields (example – tweak to your real schema)
    required = ["event_id", "supplier_name", "contact_email"]
    for k in required:
        if not answers.get(k):
            errors.append(f"Missing required field: {k}")

    # Length checks (example)
    if ans := answers.get("proposal_title"):
        if len(ans) > 120:
            errors.append("proposal_title exceeds 120 chars")

    # Attachment checks (example)
    max_files = 20
    if attachments and len(attachments) > max_files:
        errors.append(f"Too many attachments (> {max_files})")

    # (You can add filename extension allowlist, max bytes, etc.)
    return errors