import requests
import importlib
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def create_dag_from_config(config: dict) -> DAG:
    """
    由API傳回的config字典建立DAG。
    """
    # 處理start_date，API傳回是字串，需要轉成datetime物件
    start_date_str = config.get('start_date', '2023-01-01')
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')

    with DAG(
        dag_id=config['dag_id'],
        schedule_interval=config['schedule_interval'],
        start_date=start_date,
        catchup=config.get('catchup', False),
        doc_md="從Web API動態生成的DAG"
    ) as dag:
        tasks = {}

        for task_config in config['tasks']:
            task_id = task_config['task_id']
            if task_config.get('operator') == 'PythonOperator':
                module_path = task_config['python_callable_file']
                function_name = task_config['python_callable_name']

                module = importlib.import_module(module_path)
                callable_function = getattr(module, function_name)

                task = PythonOperator(
                    task_id=task_id,
                    python_callable=callable_function,
                    op_kwargs=task_config.get('op_kwargs', {})
                )
                tasks[task_id] = task

        # 設定任務依賴關係
        for task_config in config['tasks']:
            if 'dependencies' in task_config:
                for dep_task_id in task_config['dependencies']:
                    tasks[dep_task_id] >> tasks[task_config['task_id']]
    return dag


# 呼叫API取得DAG設定清單
api_url = "http://172.20.10.10:8000/dags/"
response = requests.get(api_url)
dag_configs = response.json()  # 這裡會是一個list

# 依API回傳的設定，動態產生多個DAG，並註冊到 globals()
for config in dag_configs:
    dag_id = config['dag_id']
    globals()[dag_id] = create_dag_from_config(config)

