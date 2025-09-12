# Integration Hub (D365 Connector)

The **integration-hub** is a lightweight service that talks to third-party procurement systems (today: **Microsoft Dynamics 365 / Dataverse**) and exposes simple HTTP endpoints for:
- OAuth token acquisition (client credentials via Azure Entra ID)
- Table discovery + registration
- Incremental polling (cursor-based) and row retrieval
- Submission/Export (local/email/SFTP)

It’s designed to run **independently** of the product API so we can scale, hotfix, and isolate vendor secrets safely.

---

## Architecture (high level)

lotuspetal-api ──HTTP──► integration-hub ──OAuth──► Azure AD
│
└─OData v9.2──► D365/Dataverse


- **integration-hub** = vendor connectors, polling, submission export
- **lotuspetal-api** (or demo) = product-facing API that calls the hub

---

## Requirements

- Python 3.11+
- Windows/Unix shell (PowerShell on Windows)
- D365 environment URL (e.g., `https://<org>.crm.dynamics.com`)
- Azure Entra ID App Registration (**Tenant ID**, **Client ID**, **Client Secret**)
- (Optional) SMTP/SFTP if you want to test email/SFTP export

---

## Setup

1) Clone & create virtualenv

```powershell
git clone <your-repo-url> integration-hub
cd integration-hub
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt


2. Create .env in the repo root

HUB_PORT=8080
D365_ORG_URL=https://<yourorg>.crm.dynamics.com
D365_TENANT_ID=<tenant-guid>
D365_CLIENT_ID=<app-client-id>
D365_CLIENT_SECRET=<client-secret-value>

# Optional: where "route=local" exports are written
SUBMISSION_DIR=C:\Users\<You>\OneDrive\Documents\Git\integration-hub\out

# Optional: protect hub with an internal token (if your app sends it)
HUB_SHARED_TOKEN=<random-long-string>

3. Run

uvicorn apps.gateway.main:app --reload --port 8080

4. Verify

Open http://localhost:8080/ → should show service + endpoints
GET http://localhost:8080/health

Key Endpoints

Replace {tenant_id} with your tenant

Health & Auth

GET /health

POST /tenants/{tenant_id}/connectors/d365:test
Returns WhoAmI from D365 (confirms Azure/D365 auth).

Table Discovery

GET /connectors/d365/tables?prefix=cr83d_
Lists tables by prefix (logical + set + pk + display name).

GET /connectors/d365/tables/{logical}
Metadata for a specific logical table name (e.g., account, cr83d_school).

Register Tables (what to poll)

POST /tenants/{tenant_id}/connectors/d365/tables:register
{ "tables": ["cr83d_sourcingevent", "cr83d_school", "cr83d_employee"] }
Reposting overwrites the set (use to add/remove).

Poll (delta ingest)

POST /tenants/{tenant_id}/connectors/d365:poll
Query params you may have enabled:

force_full=true → ignore cursor & read all once

table=account → target a single table for this call

Read Rows (verify data)

GET /tenants/{tenant_id}/connectors/d365/tables/{logical}/rows
Example: /tenants/{tenant}/connectors/d365/tables/cr83d_sourcingevent/rows

Submission / Export (ZIP or via routes)

POST /tenants/{tenant_id}/connectors/d365/submit
{
  "submission_package_id": "mani123",
  "route": "local",  // local | email | sftp
  "answers": { "event_id": "....", "supplier_name": "..." },
  "attachments": []
}


local → ZIP saved under SUBMISSION_DIR/<tenant_id>/submission_<id>.zip
email → SMTP (configure SMTP_HOST, SMTP_PORT, SMTP_SENDER)
sftp → SFTP upload (configure SFTP_*)

Demo Flow (quick)

POST /tenants/{tenant}/connectors/d365:test → ok + whoami
GET /connectors/d365/tables?prefix=cr83d_ → see custom tables
POST /tenants/{tenant}/connectors/d365/tables:register → pick tables
POST /tenants/{tenant}/connectors/d365:poll?force_full=true
GET /tenants/{tenant}/connectors/d365/tables/<logical>/rows → verify
POST /tenants/{tenant}/connectors/d365/submit → build/export ZIP

Security

Keep secrets in .env or a secret manager; never commit them.
Optionally require X-Internal-Token header with HUB_SHARED_TOKEN.
For production: consider mTLS, private networking, and rate limits/backoff.

Troubleshooting

401/403: check Tenant ID / Client ID / Client Secret; ensure Application User exists in D365 and has roles.
400 (metadata): remove unsupported OData query params; stick to $select, $top and do prefix filtering in-app.
Count = 0: call poll with force_full=true or confirm recent modifications.
Export path: set SUBMISSION_DIR on the hub.

.gitignore (important)
Make sure these are ignored:

.venv/
__pycache__/
*.pyc
.env
out/
*.zip
*.log
