from fastapi import FastAPI, Request
import logging, json

log = logging.getLogger("integration_hub")
logging.basicConfig(level=logging.INFO)
app = FastAPI(title="integration-hub")

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/webhooks/coupa")
async def coupa_webhook(request: Request):
    body = await request.body()
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = {"raw": body.decode("utf-8", errors="ignore")}
    log.info("COUPA_WEBHOOK %s", payload)
    return {"ok": True}
