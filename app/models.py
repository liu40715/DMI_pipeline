from sqlalchemy import Column, Integer, String, Boolean, JSON
from database import Base

class DAGModel(Base):
    __tablename__ = "dags_backend"
    id = Column(Integer, primary_key=True, index=True)
    dag_id = Column(String, unique=True, index=True, nullable=False)
    schedule_interval = Column(String, nullable=False)
    start_date = Column(String, nullable=False)
    catchup = Column(Boolean, nullable=False)
    tasks = Column(JSON, nullable=False)  # 任務清單存 JSON
