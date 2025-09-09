from __future__ import annotations
from pydantic import BaseModel
from typing import Optional, List, Literal, Dict
from datetime import datetime

Platform = Literal["d365","coupa","jaggaer","ivalua","oracle_fusion"]

class Attachment(BaseModel):
    id: str
    name: str
    byte_size: Optional[int] = None
    content_type: Optional[str] = None
    external_url: Optional[str] = None

class Requirement(BaseModel):
    id: str
    text: str
    type: Literal["text","number","file","choice"] = "text"
    required: bool = False
    meta: Dict[str, str] = {}

class Amendment(BaseModel):
    id: str
    note: Optional[str] = None
    created_at: Optional[datetime] = None

class SourcingEvent(BaseModel):
    id: str
    platform: Platform
    tenant_id: str
    title: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    due_at: Optional[datetime] = None
    requirements: List[Requirement] = []
    attachments: List[Attachment] = []
    amendments: List[Amendment] = []