from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from app.security import get_password_hash, verify_password, create_access_token, get_db
from app.models.user import User
from app.schemas.user import UserCreate, Token
import logging

logger = logging.getLogger("app")

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/register", response_model=Token)
def register(user: UserCreate, db: Session = Depends(get_db)):
    logger.info(f"Register attempt: {user.username}")
    if db.query(User).filter(User.username == user.username).first():
        logger.warning(f"Registration failed: User exists ({user.username})")
        raise HTTPException(status_code=400, detail="User exists")
    new = User(username=user.username, hashed_password=get_password_hash(user.password))
    db.add(new)
    db.commit()
    db.refresh(new)
    token = create_access_token(subject=new.username)
    logger.info(f"User registered: {user.username}")
    return {"access_token": token}

@router.post("/token", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    logger.info(f"Login attempt: {form_data.username}")
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        logger.warning(f"Login failed for: {form_data.username}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    token = create_access_token(subject=user.username)
    logger.info(f"User logged in: {form_data.username}")
    return {"access_token": token}
