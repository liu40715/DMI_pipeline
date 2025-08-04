# processor/__init__.py
import pandas as pd
import redis
import io
import importlib
from pathlib import Path
import logging
import numpy as np

# 設定 logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis 連線設定
redis_client = redis.StrictRedis(host='172.20.10.10', port=6379, db=0, decode_responses=False)
REDIS_PREFIX = 'airflow_data:'

def _get_redis_key(ti_or_key) -> str:
    """支援 TaskInstance 或字串輸入，回傳加前綴的 Redis Key"""
    if hasattr(ti_or_key, 'dag_id') and hasattr(ti_or_key, 'task_id'):
        key_part = f"{ti_or_key.dag_id}.{ti_or_key.task_id}"
    elif isinstance(ti_or_key, str):
        key_part = ti_or_key
    else:
        raise ValueError("參數 ti_or_key 必須是 TaskInstance 或字串")
    return REDIS_PREFIX + key_part

def _get_algorithm_function(algorithm_module: str, algorithm_func: str):
    """只支援資料夾結構格式 'category.method'"""
    try:
        if '.' not in algorithm_func:
            raise ValueError(f"algorithm_func 必須使用資料夾結構格式 'category.method'，目前值: {algorithm_func}")
        
        # 強制要求資料夾結構格式
        folder, method = algorithm_func.rsplit('.', 1)
        module_path = f"{algorithm_module}.{folder}.{method}"
        
        # 直接載入資料夾結構模組
        method_module = importlib.import_module(module_path)
        
        if hasattr(method_module, 'execute'):
            func = method_module.execute
            logger.info(f"使用資料夾結構方法: {algorithm_func}")
        else:
            raise AttributeError(f"模組 {module_path} 必須有 'execute' 函數")
        
        return func
        
    except Exception as e:
        logger.error(f"載入算法失敗: {e}")
        raise

OUTPUT_PATH_XCOM_KEY = 'redis_key'

def _get_upstream_output_path(ti):
    """獲取上游任務的輸出路徑"""
    upstream_task_ids = ti.task.upstream_task_ids
    logger.info(f"_get_upstream_output_path 上游任務: {upstream_task_ids}")
    
    if not upstream_task_ids:
        logger.warning(f"任務 {ti.task_id} 沒有上游任務")
        return None
    
    if len(upstream_task_ids) > 1:
        pulled_list = []
        for t_id in upstream_task_ids:
            val = ti.xcom_pull(key=OUTPUT_PATH_XCOM_KEY, task_ids=t_id)
            if val is not None:
                pulled_list.append(val)
        logger.info(f"多上游任務拉到的 XCom 清單: {pulled_list}")
        return pulled_list
    
    upstream_task_id = list(upstream_task_ids)[0]
    logger.info(f"任務 '{ti.task_id}' 從單一上游 '{upstream_task_id}' 拉取資料")
    pulled_str = ti.xcom_pull(key=OUTPUT_PATH_XCOM_KEY, task_ids=upstream_task_id)
    logger.info(f"拉取到的 redis_key: {pulled_str}")
    return pulled_str

def extract_data(ti, algorithm_module: str = "processor.extract", algorithm_func: str = "csv_reader.standard", **algo_kwargs):
    """數據提取函數 - 強制要求資料夾結構格式"""
    try:
        logger.info("執行 extract_data (資料載入層)")
        
        # 直接使用資料夾結構，不做降級處理
        func = _get_algorithm_function(algorithm_module, algorithm_func)
        logger.info(f"呼叫方法 '{algorithm_func}' 來自模組 '{algorithm_module}'")
        
        data = func(**algo_kwargs)
        
        # 序列化數據
        if isinstance(data, np.ndarray):
            logger.info("資料是 numpy.ndarray")
            buf = io.BytesIO()
            np.save(buf, data)
            data_bytes = buf.getvalue()
        elif isinstance(data, pd.DataFrame):
            logger.info("資料是 pd.DataFrame")
            buf = io.BytesIO()
            data.to_parquet(buf, index=False)
            data_bytes = buf.getvalue()
        else:
            logger.error("無法辨識資料格式，無法轉存")
            raise TypeError("Unsupported data type")
        
        redis_key = _get_redis_key(ti)
        redis_client.set(redis_key, data_bytes)
        logger.info(f"資料已存入 Redis，key={redis_key}")
        
        ti.xcom_push(key=OUTPUT_PATH_XCOM_KEY, value=redis_key)
        
    except Exception as e:
        logger.error(f"extract_data 發生例外: {e}")
        raise

def transform_data(ti, algorithm_module="processor.transform", algorithm_func="product_analysis.aggregation", **algo_kwargs):
    """數據轉換函數 - 強制要求資料夾結構格式"""
    try:
        logger.info("執行 transform_data (資料轉換層)")
        
        # 獲取上游數據
        upstream_redis_keys = _get_upstream_output_path(ti)
        if not upstream_redis_keys:
            raise RuntimeError("無法取得上游任務的 redis_key")
        
        data_list = []
        if isinstance(upstream_redis_keys, list):
            for key in upstream_redis_keys:
                data_bytes = redis_client.get(key)
                if not data_bytes:
                    raise RuntimeError(f"Redis 無資料，key={key}")
                data = io.BytesIO(data_bytes)
                data_list.append(data)
            logger.info(f"已從多個上游合併資料，共 {len(data_list)} 筆")
        else:
            data_bytes = redis_client.get(upstream_redis_keys)
            if not data_bytes:
                raise RuntimeError(f"Redis 無資料，key={upstream_redis_keys}")
            data_list = [io.BytesIO(data_bytes)]
        
        # 直接使用資料夾結構
        func = _get_algorithm_function(algorithm_module, algorithm_func)
        logger.info(f"呼叫方法 '{algorithm_func}' 來自模組 '{algorithm_module}'")
        
        processed_data = func(data_list, **algo_kwargs)
        
        # 序列化處理後的數據
        if isinstance(processed_data, np.ndarray):
            logger.info("資料是 numpy.ndarray")
            buf = io.BytesIO()
            np.save(buf, processed_data)
            data_bytes = buf.getvalue()
        elif isinstance(processed_data, pd.DataFrame):
            logger.info("資料是 pd.DataFrame")
            buf = io.BytesIO()
            processed_data.to_parquet(buf, index=False)
            data_bytes = buf.getvalue()
        else:
            logger.error("無法辨識資料格式，無法轉存")
            raise TypeError("Unsupported data type")
        
        redis_key_out = _get_redis_key(ti)
        redis_client.set(redis_key_out, data_bytes)
        logger.info(f"轉換後資料已存入 Redis，key={redis_key_out}")
        
        # 清理上游數據
        if isinstance(upstream_redis_keys, list):
            for key in upstream_redis_keys:
                redis_client.delete(key)
                logger.info(f"刪除上游 Redis key: {key}")
        else:
            redis_client.delete(upstream_redis_keys)
            logger.info(f"刪除上游 Redis key: {upstream_redis_keys}")
        
        ti.xcom_push(key=OUTPUT_PATH_XCOM_KEY, value=redis_key_out)
        
    except Exception as e:
        logger.error(f"transform_data 發生例外: {e}")
        raise

def load_data(ti, algorithm_module: str = "processor.load", algorithm_func: str = "csv_writer.standard", **algo_kwargs):
    """數據載入函數 - 強制要求資料夾結構格式"""
    try:
        logger.info("執行 load_data (資料匯入層)")
        
        # 獲取上游數據
        upstream_redis_key = _get_upstream_output_path(ti)
        if isinstance(upstream_redis_key, list):
            if len(upstream_redis_key) == 0:
                raise RuntimeError("load_data 中上游無 redis_key")
            upstream_redis_key = upstream_redis_key[0]
            logger.warning(f"load_data 有多個上游 redis_key，暫用第一筆: {upstream_redis_key}")
        
        if not upstream_redis_key:
            raise RuntimeError("load_data 無法從上游取得 redis_key")
        
        logger.info(f"從 Redis 讀取資料，key={upstream_redis_key}")
        data_bytes = redis_client.get(upstream_redis_key)
        if not data_bytes:
            raise RuntimeError(f"Redis 無資料，key={upstream_redis_key}")
        
        data = io.BytesIO(data_bytes)
        
        # 直接使用資料夾結構
        func = _get_algorithm_function(algorithm_module, algorithm_func)
        logger.info(f"呼叫方法 '{algorithm_func}' 來自模組 '{algorithm_module}'")
        
        func(data, **algo_kwargs)
        
        # 清理上游數據
        redis_client.delete(upstream_redis_key)
        logger.info(f"刪除上游 Redis key: {upstream_redis_key}")
        
    except Exception as e:
        logger.error(f"load_data 發生例外: {e}")
        raise
