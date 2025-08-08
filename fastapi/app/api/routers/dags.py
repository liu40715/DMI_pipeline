from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.schemas.dag import DAG
from app.models.dag import DAGModel
from app.db.session import SessionLocal
from app.security import get_current_user
import logging
import os, json
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger("app")
router = APIRouter(prefix="/api/dags", tags=["dags"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/")
def create_dag(dag: DAG, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Create DAG {dag.dag_id}")
    if db.query(DAGModel).filter(DAGModel.dag_id == dag.dag_id).first():
        logger.warning(f"DAG exists: {dag.dag_id}")
        raise HTTPException(status_code=400, detail="DAG exists")
    obj = DAGModel(**dag.dict())
    db.add(obj); db.commit(); db.refresh(obj)
    logger.info(f"DAG created id={obj.id}")
    return {"message": "DAG created dag_id:"+dag.dag_id+" success"}

#@router.post("/", response_model=DAG)
#def create_dag(dag: DAG, db: Session = Depends(get_db), user=Depends(get_current_user)):
#    logger.info(f"Create DAG {dag.dag_id}")
#    if db.query(DAGModel).filter(DAGModel.dag_id == dag.dag_id).first():
#        logger.warning(f"DAG exists: {dag.dag_id}")
#        raise HTTPException(status_code=400, detail="DAG exists")
#    obj = DAGModel(**dag.dict())
#    db.add(obj); db.commit(); db.refresh(obj)
#    logger.info(f"DAG created id={obj.id}")
#    return obj

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

#@router.get("/{dag_id}", response_model=DAG)
@router.get("/{dag_id}")
def read_dag(dag_id: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Read DAG {dag_id}")
    dag = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not dag:
        logger.warning(f"DAG not found: {dag_id}")
        raise HTTPException(status_code=404, detail="Not found")

    headers = {"Content-Type": "application/json"}
    auth = HTTPBasicAuth("admin", "admin")
    # Step 1: 查詢所有 dagRuns，取得最後一筆 dag_run_id
    url_runs = f"http://172.20.10.10:8080/api/v1/dags/{dag_id}/dagRuns"
    resp = requests.get(url_runs, headers=headers, auth=auth, timeout=5)
    resp.raise_for_status()
    dag_runs = resp.json().get("dag_runs", [])
    if not dag_runs:
        return []  # 沒有就回空 list
    last_dag_run_id = dag_runs[-1]["dag_run_id"]
    # Step 2: 查詢最後這個 dag_run 的所有 task instance 狀態
    url_tasks = f"http://172.20.10.10:8080/api/v1/dags/{dag_id}/dagRuns/{last_dag_run_id}/taskInstances"
    resp = requests.get(url_tasks, headers=headers, auth=auth, timeout=5)
    resp.raise_for_status()
    task_instances = resp.json().get("task_instances", [])
    # 取出 state list
    statusList = [ti["state"] for ti in task_instances]
    dag_dict = {c.name: getattr(dag, c.name) for c in dag.__table__.columns}
    dag_dict['status'] = statusList
    return dag_dict

@router.put("/{dag_id}")
def update_dag(
    dag_id: str,
    dag: DAG,
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):
    obj = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not obj:
        logger.warning(f"DAG not found for update: {dag_id}")
        raise HTTPException(status_code=404, detail="DAG not found")

    try:
        for k, v in dag.dict().items():
            setattr(obj, k, v)
        db.commit()
        db.refresh(obj)
        logger.info(f"DAG {dag_id} updated")
    except Exception as e:
        db.rollback()
        logger.error(f"DB update error for DAG {dag_id}: {e}")
        raise HTTPException(status_code=500, detail="Update failed")

    airflow_url = f"http://172.20.10.10:8080/api/v1/dags/{dag_id}/dagRuns"
    try:
        resp = requests.post(
            airflow_url,
            headers={"Content-Type": "application/json"},
            json={},  # 若有參數請自行帶入
            auth=HTTPBasicAuth('admin', 'admin'),
            timeout=5
        )
        resp.raise_for_status()
        logger.info(f"Triggered Airflow DAG run for {dag_id}. Status: {resp.status_code}")
    except Exception as e:
        logger.error(f"Failed to trigger Airflow DAG run for {dag_id}: {e}")
        raise HTTPException(status_code=502, detail="Failed to update DAG in Airflow")
    return {"message": "DAG updated dag_id:"+dag_id+" success"}

@router.delete("/{dag_id}")
def delete_dag(
    dag_id: str,
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):

    obj = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not obj:
        logger.warning(f"DAG not found for delete: {dag_id}")
        raise HTTPException(status_code=404, detail="DAG not found")


    airflow_url = f"http://172.20.10.10:8080/api/v1/dags/{dag_id}?update_mask=is_paused"
    try:
        resp = requests.patch(
            airflow_url,
            headers={"Content-Type": "application/json"},
            json={"is_paused": True},
            auth=HTTPBasicAuth('admin', 'admin'),
            timeout=5
        )
        resp.raise_for_status()
        logger.info(f"Airflow DAG {dag_id} paused. Status: {resp.status_code}")
    except Exception as e:
        logger.error(f"Pause Airflow DAG Failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to pause DAG in Airflow")

    try:
        db.delete(obj)
        db.commit()
        logger.info(f"DAG {dag_id} deleted")
    except Exception as e:
        db.rollback()
        logger.error(f"DB error: {e}")
        raise HTTPException(status_code=500, detail="Database error")

    return {"message": "DAG deleted dag_id:"+dag_id+" success"}
