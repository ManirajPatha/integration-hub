import json, os
from typing import Optional

_PATH = ".cursors.json"

def _load():
    if not os.path.exists(_PATH): return {}
    with open(_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def _save(d):
    with open(_PATH, "w", encoding="utf-8") as f:
        json.dump(d, f)

def get_cursor(tenant_id: str, resource: str) -> Optional[str]:
    d = _load()
    return d.get(f"{tenant_id}:{resource}")

def set_cursor(tenant_id: str, resource: str, cursor: str):
    d = _load()
    d[f"{tenant_id}:{resource}"] = cursor
    _save(d)
