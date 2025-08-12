import logging
import os, json
import requests
from fastapi import APIRouter, Depends, HTTPException
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

@router.post("/")
def create_dag(dag: DAG, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Create DAG {dag.dag_id}")
    if db.query(DAGModel).filter(DAGModel.dag_id == dag.dag_id).first():
        logger.warning(f"DAG exists: {dag.dag_id}")
        raise HTTPException(status_code=400, detail="DAG已存在")
    try:
        obj = DAGModel(**dag.dict())
        db.add(obj)
        db.commit()
        db.refresh(obj)
        logger.info(f"DAG created id={obj.id}")
        return {"message": f"DAG created dag_id:{dag.dag_id} success"}
    except Exception as e:
        db.rollback()
        logger.error(f"建立 DAG 失敗: {e}")
        raise HTTPException(status_code=500, detail="建立DAG失敗")

@router.get("/all_dags")
def read_dags(db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info("Read all DAGs")
    try:
        dags = db.query(DAGModel).all()
    except Exception as e:
        logger.error(f"資料庫查詢失敗: {e}")
        raise HTTPException(status_code=500, detail="讀取 DAG 清單失敗")
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
                response = requests.get(
                    api_url, auth=auth, headers=headers, timeout=5
                )
                response.raise_for_status()
                data = response.json()
                if "dag_runs" in data and len(data["dag_runs"]) > 0:
                    last_state = data["dag_runs"][-1].get("state", "")
            except Timeout:
                logger.error("Airflow API逾時")
            except ConnectionError:
                logger.error("Airflow連線失敗")
            except RequestException as e:
                logger.error(f"Airflow API錯誤: {e}")
            except Exception as e:
                logger.error(f"取得 Airflow dag_run 狀態失敗({dag_id}): {e}")
        dag_dict["state"] = last_state
        dag_dict.pop("id", None)
        dag_dict.pop("tasks", None)
        result.append(dag_dict)
    return result

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
                logger.warning(f"讀取模板失敗{fname}: {e}")
    if not results:
        logger.warning("default_dags 沒有任何json模板.")
        raise HTTPException(status_code=404, detail="找不到任何模板 json")
    return results

@router.get("/{dag_id}")
def read_dag(dag_id: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    logger.info(f"Read DAG {dag_id}")
    dag = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not dag:
        logger.warning(f"DAG not found: {dag_id}")
        raise HTTPException(status_code=404, detail="DAG不存在")
    headers = {"Content-Type": "application/json"}
    auth = HTTPBasicAuth("admin", "admin")
    try:
        url_runs = f"http://airflow:8080/api/v1/dags/{dag_id}/dagRuns"
        resp = requests.get(url_runs, headers=headers, auth=auth, timeout=5)
        resp.raise_for_status()
        dag_runs = resp.json().get("dag_runs", [])
        if not dag_runs:
            logger.info(f"{dag_id} 沒有任何dag run")
            return []
        last_dag_run_id = dag_runs[-1]["dag_run_id"]
        url_tasks = f"http://airflow:8080/api/v1/dags/{dag_id}/dagRuns/{last_dag_run_id}/taskInstances"
        resp = requests.get(url_tasks, headers=headers, auth=auth, timeout=5)
        resp.raise_for_status()
        task_instances = resp.json().get("task_instances", [])
        state_map = {ti["task_id"]: ti["state"] for ti in task_instances}
    except Timeout:
        logger.error("Airflow API逾時")
        raise HTTPException(status_code=504, detail="Airflow API Timeout")
    except ConnectionError:
        logger.error("Airflow連線失敗")
        raise HTTPException(status_code=502, detail="Airflow API Connection Failed")
    except RequestException as e:
        logger.error(f"Airflow API錯誤: {e}")
        raise HTTPException(status_code=502, detail="Airflow API失敗")
    except Exception as e:
        logger.error(f"取得 Airflow DAG任務狀態失敗 ({dag_id}): {e}")
        raise HTTPException(status_code=502, detail="Airflow取得DAG任務失敗")
    dag_dict = {c.name: getattr(dag, c.name) for c in dag.__table__.columns}
    dag_dict.pop("id", None)
    tasks = dag_dict.get("tasks", [])
    for task in tasks:
        task_id = task.get("task_id")
        task["state"] = state_map.get(task_id, None)
    dag_dict["tasks"] = tasks
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
        raise HTTPException(status_code=404, detail="DAG不存在")
    try:
        for k, v in dag.dict().items():
            setattr(obj, k, v)
        db.commit()
        db.refresh(obj)
        logger.info(f"DAG {dag_id} updated")
    except Exception as e:
        db.rollback()
        logger.error(f"DAG更新失敗: {e}")
        raise HTTPException(status_code=500, detail="資料庫DAG更新失敗")
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
    except Timeout:
        logger.error("Airflow API逾時")
        raise HTTPException(status_code=504, detail="Airflow API Timeout")
    except ConnectionError:
        logger.error("Airflow連線失敗")
        raise HTTPException(status_code=502, detail="Airflow API Connection Failed")
    except RequestException as e:
        logger.error(f"Airflow API失敗: {e}")
        raise HTTPException(status_code=502, detail="Airflow觸發DAG run失敗")
    except Exception as e:
        logger.error(f"觸發 Airflow DAG run 失敗: {e}")
        raise HTTPException(status_code=502, detail="Airflow觸發DAG run失敗")
    return {"message": f"DAG updated dag_id:{dag_id} success"}

@router.delete("/{dag_id}")
def delete_dag(
    dag_id: str,
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):
    obj = db.query(DAGModel).filter(DAGModel.dag_id == dag_id).first()
    if not obj:
        logger.warning(f"DAG not found for delete: {dag_id}")
        raise HTTPException(status_code=404, detail="DAG不存在")
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
    except Timeout:
        logger.error("Airflow API逾時")
        raise HTTPException(status_code=504, detail="Airflow API Timeout")
    except ConnectionError:
        logger.error("Airflow連線失敗")
        raise HTTPException(status_code=502, detail="Airflow API Connection Failed")
    except RequestException as e:
        logger.error(f"Airflow API失敗: {e}")
        raise HTTPException(status_code=502, detail="暫停DAG失敗")
    except Exception as e:
        logger.error(f"暫停 Airflow DAG 失敗: {e}")
        raise HTTPException(status_code=502, detail="暫停DAG失敗")
    try:
        db.delete(obj)
        db.commit()
        logger.info(f"DAG {dag_id} deleted")
    except Exception as e:
        db.rollback()
        logger.error(f"DB刪除失敗: {e}")
        raise HTTPException(status_code=500, detail="資料庫DAG刪除失敗")
    return {"message": f"DAG deleted dag_id:{dag_id} success"}
