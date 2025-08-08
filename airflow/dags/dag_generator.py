# dag_generator.py
import requests
import importlib
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def create_dag_from_config(config: dict) -> DAG:
    """由API傳回的config字典建立DAG - 只支援新的配置格式"""
    
    # 處理start_date
    start_date_str = config.get('start_date', '2023-01-01')
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    owner = config.get('owner', 'default_owner')
    
    with DAG(
        dag_id=config['dag_id'],
        schedule_interval=config['schedule_interval'],
        start_date=start_date,
        catchup=config.get('catchup', False),
        doc_md="從Web API動態生成的DAG",
        tags=['workflow'],
        default_args={"owner": owner},
    ) as dag:
        
        tasks = {}
        
        for task_config in config['tasks']:
            task_id = task_config['task_id']
            
            if task_config.get('operator') == 'PythonOperator':
                # 只支援新格式：processor_stage 和 processor_method
                if 'processor_stage' not in task_config or 'processor_method' not in task_config:
                    raise ValueError(f"任務 {task_id} 必須包含 'processor_stage' 和 'processor_method' 參數")
                
                stage = task_config['processor_stage']
                method = task_config['processor_method']
                
                # 驗證處理階段
                valid_stages = ['extract', 'transform', 'load']
                if stage not in valid_stages:
                    raise ValueError(f"不支援的處理階段: {stage}，有效階段: {valid_stages}")
                
                # 動態導入處理器模組
                module = importlib.import_module('processor')
                
                # 根據階段選擇對應的函數
                stage_function_map = {
                    'extract': 'extract_data',
                    'transform': 'transform_data',
                    'load': 'load_data'
                }
                
                function_name = stage_function_map[stage]
                callable_function = getattr(module, function_name)
                
                # 合併參數
                op_kwargs = task_config.get('op_kwargs', {})
                op_kwargs.update({
                    'algorithm_module': f'processor.{stage}',
                    'algorithm_func': method
                })
                
                task = PythonOperator(
                    task_id=task_id,
                    python_callable=callable_function,
                    op_kwargs=op_kwargs
                )
                
                tasks[task_id] = task
            else:
                raise ValueError(f"任務 {task_id} 不支援的操作器類型: {task_config.get('operator')}")
        
        # 設定任務依賴關係
        for task_config in config['tasks']:
            if 'dependencies' in task_config:
                for dep_task_id in task_config['dependencies']:
                    if dep_task_id not in tasks:
                        raise ValueError(f"依賴任務 {dep_task_id} 不存在")
                    tasks[dep_task_id] >> tasks[task_config['task_id']]
    
    return dag

def load_dag_configs():
    dag_configs = []
    try:
        conn = psycopg2.connect(
            database="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port=5432
        )
        cur = conn.cursor()
        cur.execute("SELECT dag_id, schedule_interval, start_date, catchup, owner, tasks FROM dags_backend;")
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        for row in rows:
            config = dict(zip(columns, row))
            # 若 tasks 是字串(JSON)，載入為 list
            if isinstance(config['tasks'], str):
                config['tasks'] = json.loads(config['tasks'])
            dag_configs.append(config)
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error loading DAG configs: {e}")
    return dag_configs

dag_configs = load_dag_configs()
# 依API回傳的設定，動態產生多個DAG
for config in dag_configs:
    dag_id = config['dag_id']
    globals()[dag_id] = create_dag_from_config(config)

