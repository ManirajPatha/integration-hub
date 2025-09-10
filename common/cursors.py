# common/cursors.py
from __future__ import annotations
from pathlib import Path
import json
from typing import Optional

_STORE = Path(".runtime/cursors.json")
_STORE.parent.mkdir(parents=True, exist_ok=True)

def _load() -> dict:
    if _STORE.exists():
        try:
            return json.loads(_STORE.read_text())
        except Exception:
            return {}
    return {}

def _save(d: dict) -> None:
    _STORE.write_text(json.dumps(d, indent=2))

def _key(tenant_id: str, table: str) -> str:
    return f"{tenant_id}:{table}"

def get_cursor(tenant_id: str, table: str) -> Optional[str]:
    data = _load()
    return data.get(_key(tenant_id, table))

def set_cursor(tenant_id: str, table: str, iso_z: str) -> None:
    data = _load()
    data[_key(tenant_id, table)] = iso_z
    _save(data)

def list_cursors(tenant: str) -> dict[str, str]:
    d = Path(".runtime") / "cursors" / tenant
    out = {}
    if d.exists():
        for p in d.glob("*.txt"):
            out[p.stem] = p.read_text().strip()
    return out

def reset_cursors(tenant: str, tables: list[str] | None = None) -> int:
    d = Path(".runtime") / "cursors" / tenant
    if not d.exists():
        return 0
    count = 0
    for p in d.glob("*.txt"):
        if (not tables) or (p.stem in tables):
            try:
                p.unlink()
                count += 1
            except Exception:
                pass
    return count
