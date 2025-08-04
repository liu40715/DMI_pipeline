from datetime import datetime, timedelta
import logging
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagModel
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session
import requests

# 預設參數
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 建立 DAG
dag = DAG(
    'cleanup_paused_dags',
    default_args=default_args,
    description='刪除暫停的 DAG 並同步刪除 API 資料庫資料',
    schedule_interval='* * * * *',
    catchup=False,
    tags=['maintenance'],
)

def call_delete_api(dag_id: str) -> bool:
    """
    調用 API 刪除資料庫中的 DAG 資料
    """
    logger = logging.getLogger(__name__)
    
    try:
        # API 端點 URL
        api_url = f"http://172.20.10.10:8000/dags/{dag_id}"
        
        # 發送 DELETE 請求
        response = requests.delete(
            api_url,
            headers={'accept': 'application/json'},
            timeout=30
        )
        
        if response.status_code == 200:
            logger.info(f"成功調用 API 刪除 DAG 資料: {dag_id}")
            return True
        elif response.status_code == 404:
            logger.warning(f" API 中找不到 DAG: {dag_id}（可能已刪除）")
            return True  # 404 也視為成功，因為資料已經不存在
        else:
            logger.error(f"API 刪除失敗 {dag_id}: HTTP {response.status_code}, {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error(f"API 請求逾時: {dag_id}")
        return False
    except requests.exceptions.ConnectionError:
        logger.error(f"無法連接到 API: {dag_id}")
        return False
    except Exception as e:
        logger.error(f"調用 API 發生錯誤 {dag_id}: {e}")
        return False

@provide_session
def delete_paused_dags_with_api(session: Session = None, **context):
    """
    刪除暫停的 DAG 並同步刪除 API 資料庫資料
    """
    logger = logging.getLogger(__name__)
    
    # 排除清理程式本身
    excluded_dags = ['cleanup_paused_dags']
    
    try:
        # 查詢暫停的 DAG
        paused_dags = session.query(DagModel).filter(
            DagModel.is_paused == True,
            ~DagModel.dag_id.in_(excluded_dags)
        ).all()
        
        logger.info(f"找到 {len(paused_dags)} 個暫停的 DAG")
        
        deleted_count = 0
        api_deleted_count = 0
        
        for dag_model in paused_dags:
            dag_id = dag_model.dag_id
            
            try:
                # 步驟 1: 使用 CLI 刪除 Airflow DAG
                result = subprocess.run(
                    ['airflow', 'dags', 'delete', '-y', dag_id],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    logger.info(f"成功刪除 Airflow DAG: {dag_id}")
                    deleted_count += 1
                    
                    # 步驟 2: 調用 API 刪除資料庫資料
                    api_success = call_delete_api(dag_id)
                    if api_success:
                        api_deleted_count += 1
                    else:
                        logger.warning(f"DAG {dag_id} 已從 Airflow 刪除，但 API 刪除失敗")
                        
                else:
                    logger.error(f"Airflow DAG 刪除失敗 {dag_id}: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                logger.error(f"刪除 {dag_id} 逾時")
            except Exception as e:
                logger.error(f"刪除 {dag_id} 發生錯誤: {e}")
        
        result_message = (
            f"完成！成功刪除 {deleted_count} 個 Airflow DAG，"
            f"API 成功刪除 {api_deleted_count} 個資料庫記錄"
        )
        logger.info(result_message)
        return result_message
        
    except Exception as e:
        logger.error(f"清理程式發生錯誤: {e}")
        raise

# 建立任務
cleanup_task = PythonOperator(
    task_id='delete_paused_dags_with_api',
    python_callable=delete_paused_dags_with_api,
    dag=dag,
)

