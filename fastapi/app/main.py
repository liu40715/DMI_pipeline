import logging.config
import os

from fastapi import FastAPI
from app.db.base import Base
from app.db.session import engine
from app.models import dag, user
from app.api.routers import auth, dags
from app.db.session import SessionLocal
from app.models.user import User
from app.security import get_password_hash
from fastapi.middleware.cors import CORSMiddleware

def ensure_admin_user():
    db = SessionLocal()
    try:
        admin_username = "admin"
        admin_password = "admin" 
        user = db.query(User).filter(User.username == admin_username).first()
        if not user:
            admin = User(username=admin_username, hashed_password=get_password_hash(admin_password))
            db.add(admin)
            db.commit()
        else:
            pass
    finally:
        db.close()

# 載入 logging 配置
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "../logging.conf"), disable_existing_loggers=False)

# 建立所有表格
Base.metadata.create_all(bind=engine)
ensure_admin_user()

app = FastAPI(title="DMI pipeline builder")

app.include_router(auth.router)
app.include_router(dags.router)

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000",
    "http://192.168.0.20:3000"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
