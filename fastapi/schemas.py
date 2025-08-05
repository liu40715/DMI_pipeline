from typing import List, Optional, Dict, Any
from pydantic import BaseModel

class DAG(BaseModel):
    dag_id: str
    schedule_interval: str
    start_date: str
    catchup: bool
    tasks: List[Dict[str, Any]]  # 任務用通用字典格式表示

    class Config:
        orm_mode = True
