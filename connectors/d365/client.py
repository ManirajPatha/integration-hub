# connectors/d365/client.py
import httpx
from common.settings import settings

_TOKEN_CACHE: dict[str, str] = {}

async def _get_token() -> str:
    tok = _TOKEN_CACHE.get("token")
    if tok:
        return tok
    url = f"https://login.microsoftonline.com/{settings.d365_tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": settings.d365_client_id,
        "client_secret": settings.d365_client_secret,
        "grant_type": "client_credentials",
        "scope": f"{settings.d365_org_url}/.default",
    }
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        r.raise_for_status()
        j = r.json()
        _TOKEN_CACHE["token"] = j["access_token"]
        return j["access_token"]

async def d365_get(path: str, params: dict | None = None):
    token = await _get_token()
    base = f"{settings.d365_org_url}/api/data/v9.2"
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.get(
            f"{base}{path}",
            params=params or {},
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )
        r.raise_for_status()
        return r.json()

async def d365_whoami() -> tuple[bool, dict]:
    try:
        j = await d365_get("/WhoAmI")
        return True, j
    except Exception as e:
        return False, {"error": str(e)}