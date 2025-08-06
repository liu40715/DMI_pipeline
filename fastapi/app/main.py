import logging.config
import os

from fastapi import FastAPI
from app.db.base import Base
from app.db.session import engine
from app.models import dag, user
from app.api.routers import auth, dags

# 載入 logging 配置
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "../logging.conf"), disable_existing_loggers=False)

# 建立所有表格
Base.metadata.create_all(bind=engine)

app = FastAPI(title="DMI pipeline builder")

app.include_router(auth.router)
app.include_router(dags.router)
