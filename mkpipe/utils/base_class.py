from pydantic import BaseModel
from typing import Optional


class InputTask(BaseModel):
    extractor_variant: str
    current_table_conf: dict
    loader_variant: str
    loader_conf: dict
    priority: Optional[int] = None
    data: Optional[dict]
