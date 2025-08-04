from datetime import datetime, timedelta
import logging
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagModel
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

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
    description='使用 CLI 簡單刪除暫停的 DAG',
    schedule_interval='* * * * *',
    catchup=False,
    tags=['cleanup'],
)

@provide_session
def delete_paused_dags_simple(session: Session = None, **context):
    """
    使用 CLI 刪除暫停的 DAG（簡化版）
    """
    logger = logging.getLogger(__name__)
    
    # 排除清理程式本身
    excluded_dags = ['simple_cleanup_paused_dags']
    
    try:
        # 查詢暫停的 DAG
        paused_dags = session.query(DagModel).filter(
            DagModel.is_paused == True,
            ~DagModel.dag_id.in_(excluded_dags)
        ).all()
        
        logger.info(f"找到 {len(paused_dags)} 個暫停的 DAG")
        
        deleted_count = 0
        
        for dag_model in paused_dags:
            dag_id = dag_model.dag_id
            
            try:
                # 使用 CLI 刪除
                result = subprocess.run(
                    ['airflow', 'dags', 'delete', '-y', dag_id],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    logger.info(f"✅ 成功刪除 DAG: {dag_id}")
                    deleted_count += 1
                else:
                    logger.error(f"❌ 刪除失敗 {dag_id}: {result.stderr}")
                    
            except Exception as e:
                logger.error(f"❌ 刪除 {dag_id} 發生錯誤: {e}")
        
        result_message = f"完成！成功刪除 {deleted_count} 個暫停的 DAG"
        logger.info(result_message)
        return result_message
        
    except Exception as e:
        logger.error(f"清理程式發生錯誤: {e}")
        raise

# 建立任務
cleanup_task = PythonOperator(
    task_id='delete_paused_dags',
    python_callable=delete_paused_dags_simple,
    dag=dag,
)
