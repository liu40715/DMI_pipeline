from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil

# 設定你 Airflow log 路徑，根據你的環境調整
AIRFLOW_LOG_DIR = "/opt/airflow/logs"  # 或 '/home/airflow/airflow/logs' 等

# 保留日誌的天數，超過天數的日誌會被刪除
LOG_RETENTION_DAYS = 7

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def clean_old_logs():
    """清理超過保留期限的日誌資料夾"""
    cutoff_time = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)
    # 遍歷 Airflow log 目錄下的所有 DAG 日誌資料夾
    for subdir, dirs, files in os.walk(AIRFLOW_LOG_DIR):
        # 只處理目錄，Airflow log 以目錄分 DAG及任務執行
        for dirname in dirs:
            dirpath = os.path.join(subdir, dirname)
            # 取得此資料夾最後修改時間
            dir_mtime = datetime.fromtimestamp(os.path.getmtime(dirpath))
            if dir_mtime < cutoff_time:
                # 使用 shutil.rmtree 递归刪除整個舊資料夾
                shutil.rmtree(dirpath)
                print(f"刪除日誌資料夾: {dirpath}")

with DAG(
    'cleanup_dag_logs',
    default_args=default_args,
    description='定期清除舊的 Airflow 日誌',
    schedule_interval='@daily',  # 每日執行一次，可改成您需要的排程
    catchup=False,
    tags=['maintenance'],
) as dag:
    
    clean_logs_task = PythonOperator(
        task_id='clean_old_logs',
        python_callable=clean_old_logs,
    )
