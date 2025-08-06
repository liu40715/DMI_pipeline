from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.schemas.dag import DAG
from app.models.dag import DAGModel
from app.db.session import SessionLocal
from app.security import get_current_user
import logging
import os, json
logger = logging.getLogger("app")
router = APIRouter(prefix="/api/dags", tags=["dags"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/", response_model=DAG)
def create_dag(dag: DAG, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Create DAG {dag.dag_id}")
    if db.query(DAGModel).filter(DAGModel.dag_id == dag.dag_id).first():
        logger.warning(f"DAG exists: {dag.dag_id}")
        raise HTTPException(status_code=400, detail="DAG exists")
    obj = DAGModel(**dag.dict())
    db.add(obj); db.commit(); db.refresh(obj)
    logger.info(f"DAG created id={obj.id}")
    return obj

@router.get("/all_dags", response_model=List[DAG])
def read_dags(db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info("Read all DAGs")
    return db.query(DAGModel).all()

@router.get("/template_dags")
def get_template_dags(user=Depends(get_current_user)):
    folder = os.path.join(os.path.dirname(__file__), "../../default_dags")
    folder = os.path.abspath(folder)
    logger.info(f"Reading template dags from {folder}")

    if not os.path.exists(folder) or not os.path.isdir(folder):
        logger.warning(f"Template folder not found: {folder}")
        raise HTTPException(status_code=404, detail="default_dags 資料夾不存在")

    results = []
    for fname in os.listdir(folder):
        if fname.lower().endswith(".json"):
            fpath = os.path.join(folder, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    content = json.load(f)
                results.append(content)
                logger.info(f"Loaded template: {fname}")
            except Exception as e:
                logger.warning(f"Failed to load {fname}: {e}")

    if not results:
        logger.warning("No JSON templates found in default_dags.")
        raise HTTPException(status_code=404, detail="找不到任何模板 json")

    return results

@router.get("/{dag_id}", response_model=DAG)
def read_dag(dag_id: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Read DAG {dag_id}")
    dag = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not dag:
        logger.warning(f"DAG not found: {dag_id}")
        raise HTTPException(status_code=404, detail="Not found")
    return dag

@router.put("/{dag_id}", response_model=DAG)
def update_dag(dag_id: str, dag: DAG, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Update DAG {dag_id}")
    obj = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not obj:
        logger.warning(f"DAG not found for update: {dag_id}")
        raise HTTPException(status_code=404, detail="Not found")
    for k, v in dag.dict().items():
        setattr(obj, k, v)
    db.commit(); db.refresh(obj)
    logger.info(f"DAG updated {dag_id}")
    return obj

@router.delete("/{dag_id}")
def delete_dag(dag_id: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Delete DAG {dag_id}")
    obj = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not obj:
        logger.warning(f"DAG not found for delete: {dag_id}")
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(obj); db.commit()
    logger.info(f"DAG deleted {dag_id}")
    return {"message": "Deleted", "dag_id": dag_id}
