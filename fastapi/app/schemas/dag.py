from typing import List, Dict, Any, Optional
from pydantic import BaseModel

class DAG(BaseModel):
    dag_id: str
    schedule_interval: str
    start_date: str
    catchup: bool
    owner: str
    tasks: List[Dict[str, Any]]

    class Config:
        orm_mode = True
