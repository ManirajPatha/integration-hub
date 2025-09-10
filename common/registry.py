# common/registry.py
from __future__ import annotations
import json, os
from pathlib import Path
from typing import Dict, List

# You can override this in .env if you want
REGISTRY_PATH = Path(os.getenv("REGISTRY_PATH", "./data/registry.json"))

def _ensure_parent():
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)

def load_registry() -> Dict[str, List[str]]:
    """Return { tenant_id: [logical_table, ...], ... }."""
    if not REGISTRY_PATH.exists():
        return {}
    try:
        with REGISTRY_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # normalize to lists of strings
        return {k: [str(x) for x in v] for k, v in data.items()}
    except Exception:
        return {}

def save_registry(data: Dict[str, List[str]]) -> None:
    _ensure_parent()
    with REGISTRY_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ---------- Simple helpers used by main.py ----------

def get_tables(tenant_id: str) -> List[str]:
    """Return registered logical table names for a tenant."""
    reg = load_registry()
    return reg.get(tenant_id, [])

def set_tables(tenant_id: str, tables: List[str]) -> List[str]:
    """Replace the tenant's list (idempotent)."""
    reg = load_registry()
    reg[tenant_id] = sorted(set(tables or []))
    save_registry(reg)
    return reg[tenant_id]

def register_tables(tenant_id: str, tables: List[str]) -> List[str]:
    """Add new tables (keeps existing)."""
    reg = load_registry()
    existing = set(reg.get(tenant_id, []))
    for t in tables or []:
        if t:
            existing.add(str(t))
    reg[tenant_id] = sorted(existing)
    save_registry(reg)
    return reg[tenant_id]
