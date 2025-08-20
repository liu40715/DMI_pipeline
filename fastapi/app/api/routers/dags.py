import logging
import os, json
import requests
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, ConnectionError, Timeout

from app.schemas.dag import DAG
from app.models.dag import DAGModel
from app.db.session import SessionLocal
from app.security import get_current_user

logger = logging.getLogger("app")
router = APIRouter(prefix="/api/dags", tags=["dags"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


### Create DAG
@router.post("/")
def create_dag(dag: DAG, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Create DAG {dag.dag_id}")
    if db.query(DAGModel).filter(DAGModel.dag_id == dag.dag_id).first():
        logger.warning(f"DAG exists: {dag.dag_id}")
        return {"success": False, "message": f"DAG already exists: {dag.dag_id}", "data": []}
    try:
        obj = DAGModel(**dag.dict())
        db.add(obj)
        db.commit()
        db.refresh(obj)
        logger.info(f"DAG created id={obj.id}")
        return {
            "success": True,
            "message": f"DAG created dag_id:{dag.dag_id} successfully",
            "data": [obj.__dict__]
        }
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create DAG: {e}")
        return {"success": False, "message": "DAG creation failed", "data": []}


### Read all DAGs
@router.get("/all_dags")
def read_dags(db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info("Read all DAGs")
    try:
        dags = db.query(DAGModel).all()
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        return {"success": False, "message": "Failed to retrieve DAG list", "data": []}

    result = []
    for dag in dags:
        dag_dict = {c.name: getattr(dag, c.name) for c in dag.__table__.columns}
        dag_id = dag_dict.get("dag_id")
        last_state = ""
        if dag_id:
            api_url = f"http://airflow:8080/api/v1/dags/{dag_id}/dagRuns"
            headers = {"Content-Type": "application/json"}
            auth = HTTPBasicAuth("admin", "admin")
            try:
                response = requests.get(api_url, auth=auth, headers=headers, timeout=5)
                response.raise_for_status()
                data = response.json()
                if "dag_runs" in data and len(data["dag_runs"]) > 0:
                    last_state = data["dag_runs"][-1].get("state", "")
            except Exception as e:
                logger.error(f"Failed to fetch Airflow dag_run state ({dag_id}): {e}")                
        dag_dict["state"] = last_state
        dag_dict.pop("id", None)
        dag_dict.pop("tasks", None)
        result.append(dag_dict)

    return {"success": True, "message": "Retrieve successful", "data": result}


### Get template DAGs
@router.get("/template_dags")
def get_template_dags(user=Depends(get_current_user)):
    folder = os.path.join(os.path.dirname(__file__), "../../default_dags")
    folder = os.path.abspath(folder)
    logger.info(f"Reading template dags from {folder}")
    if not os.path.exists(folder) or not os.path.isdir(folder):
        logger.warning(f"Template folder not found: {folder}")
        return {"success": False, "message": "default_dags folder not found", "data": []}

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
                logger.warning(f"Failed to read template {fname}: {e}")
    if not results:
        return {"success": False, "message": "No JSON templates found in default_dags", "data": []}

    return {"success": True, "message": "Retrieve successful", "data": results}


### Get single DAG with state
@router.get("/{dag_id}")
def read_dag(dag_id: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Read DAG {dag_id}")
    dag = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not dag:
        return {"success": False, "message": f"DAG not found: {dag_id}", "data": []}

    headers = {"Content-Type": "application/json"}
    auth = HTTPBasicAuth("admin", "admin")
    try:
        url_runs = f"http://airflow:8080/api/v1/dags/{dag_id}/dagRuns"
        resp = requests.get(url_runs, headers=headers, auth=auth, timeout=5)
        resp.raise_for_status()
        dag_runs = resp.json().get("dag_runs", [])
        if not dag_runs:
            return {"success": True, "message": f"{dag_id} has no dag runs", "data": []}
        last_dag_run_id = dag_runs[-1]["dag_run_id"]
        url_tasks = f"http://airflow:8080/api/v1/dags/{dag_id}/dagRuns/{last_dag_run_id}/taskInstances"
        resp = requests.get(url_tasks, headers=headers, auth=auth, timeout=5)
        resp.raise_for_status()
        task_instances = resp.json().get("task_instances", [])
        state_map = {ti["task_id"]: ti["state"] for ti in task_instances}
    except Exception as e:
        logger.error(f"Failed to fetch Airflow DAG task states ({dag_id}): {e}")
        return {"success": False, "message": "Failed to fetch DAG task states from Airflow", "data": []}

    dag_dict = {c.name: getattr(dag, c.name) for c in dag.__table__.columns}
    dag_dict.pop("id", None)
    tasks = dag_dict.get("tasks", [])
    for task in tasks:
        task_id = task.get("task_id")
        task["state"] = state_map.get(task_id, None)
    dag_dict["tasks"] = tasks
    return {"success": True, "message": "Retrieve successful", "data": [dag_dict]}


### Update DAG
@router.put("/{dag_id}")
def update_dag(dag_id: str, dag: DAG, db: Session = Depends(get_db), user=Depends(get_current_user)):
    obj = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not obj:
        return {"success": False, "message": f"DAG not found: {dag_id}", "data": []}
    try:
        for k, v in dag.dict().items():
            setattr(obj, k, v)
        db.commit()
        db.refresh(obj)
        logger.info(f"DAG {dag_id} updated")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to update DAG: {e}")
        return {"success": False, "message": "Database DAG update failed", "data": []}

    airflow_url = f"http://airflow:8080/api/v1/dags/{dag_id}/dagRuns"
    try:
        resp = requests.post(
            airflow_url,
            headers={"Content-Type": "application/json"},
            json={},
            auth=HTTPBasicAuth('admin', 'admin'),
            timeout=5
        )
        resp.raise_for_status()
        logger.info(f"Triggered Airflow DAG run for {dag_id}. Status: {resp.status_code}")
    except Exception as e:
        logger.error(f"Failed to trigger Airflow DAG run: {e}")
        return {"success": False, "message": "Failed to trigger DAG run in Airflow", "data": []}

    return {"success": True, "message": f"DAG updated dag_id:{dag_id} successfully", "data": [obj.__dict__]}


### Delete DAG
@router.delete("/{dag_id}")
def delete_dag(dag_id: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    obj = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not obj:
        return {"success": False, "message": f"DAG not found: {dag_id}", "data": []}

    airflow_url = f"http://airflow:8080/api/v1/dags/{dag_id}?update_mask=is_paused"
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
        logger.error(f"Failed to pause Airflow DAG: {e}")
        return {"success": False, "message": "Failed to pause DAG", "data": []}

    try:
        db.delete(obj)
        db.commit()
        logger.info(f"DAG {dag_id} deleted")
    except Exception as e:
        db.rollback()
        logger.error(f"Database deletion failed: {e}")
        return {"success": False, "message": "Database DAG deletion failed", "data": []}

    return {"success": True, "message": f"DAG deleted dag_id:{dag_id} successfully", "data": []}
