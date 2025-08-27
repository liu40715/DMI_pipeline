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
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=False)
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

def _serialize_value(v):
    buf = io.BytesIO()
    if isinstance(v, np.ndarray):
        logger.info("儲存資料格式為 numpy.ndarray")
        np.save(buf, v)
    elif isinstance(v, pd.DataFrame):
        logger.info("儲存資料格式為 pd.DataFrame")
        v.to_parquet(buf, index=False)
    else:
        raise TypeError("不支援的資料格式")
    return buf.getvalue()

def _store_dict_to_redis_hash(redis_key, data_dict):
    try:
        mapping = {k: _serialize_value(v) for k, v in data_dict.items()}
        redis_client.hset(redis_key, mapping=mapping)
        redis_client.expire(redis_key, 60)
        logger.info(f"資料已存入 Redis，key={redis_key}")
    except Exception as e:
        logger.error(f"儲存錯誤: {e}")

def _load_dict_from_redis_hash(redis_key):
    logger.info(f"從 Redis 讀取資料:key={redis_key}")
    stored = redis_client.hgetall(redis_key)
    data_dict = {}
    for k, v_bytes in stored.items():
        buf = io.BytesIO(v_bytes)
        try:
            buf.seek(0)
            data_dict[k] = np.load(buf, allow_pickle=True)
        except Exception:
            buf.seek(0)
            data_dict[k] = pd.read_parquet(buf)
    return data_dict

def input_data(ti, algorithm_module: str = "processor.extract", algorithm_func: str = "csv_reader.standard", **algo_kwargs):
    """數據提取函數 - 強制要求資料夾結構格式"""
    try:
        logger.info("執行 extract_data (資料載入層)")
        
        # 直接使用資料夾結構，不做降級處理
        func = _get_algorithm_function(algorithm_module, algorithm_func)
        logger.info(f"呼叫方法 '{algorithm_func}' 來自模組 '{algorithm_module}'")
        
        dict_data = func(**algo_kwargs)
        redis_key = _get_redis_key(ti)
        _store_dict_to_redis_hash(redis_key,dict_data)
        
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
        
        # 直接使用資料夾結構
        func = _get_algorithm_function(algorithm_module, algorithm_func)
        logger.info(f"呼叫方法 '{algorithm_func}' 來自模組 '{algorithm_module}'")

        data_list = []
        if isinstance(upstream_redis_keys, list):
            pass
            # for key in upstream_redis_keys:
            #     dict_data = _load_dict_from_redis_hash(key)
            #     data_list.append(dict_data)
            
            # proc_dict_data  = {k: func(v, **algo_kwargs) for k, v in dict_data.items()}
        else:
            dict_data = _load_dict_from_redis_hash(upstream_redis_keys)
            logger.info(f"Tags 名稱: {dict_data.keys()}")
            proc_dict_data  = {k: func(v, **algo_kwargs) for k, v in dict_data.items()}
        redis_key_out = _get_redis_key(ti)
        _store_dict_to_redis_hash(redis_key_out,proc_dict_data)
        logger.info(f"轉換後資料已存入 Redis，key={redis_key_out}")
        
        # 清理上游數據
        #if isinstance(upstream_redis_keys, list):
        #    for key in upstream_redis_keys:
        #        #redis_client.delete(key)
        #        logger.info(f"刪除上游 Redis key: {key}")
        #else:
        #    #redis_client.delete(upstream_redis_keys)
        #    logger.info(f"刪除上游 Redis key: {upstream_redis_keys}")
        
        ti.xcom_push(key=OUTPUT_PATH_XCOM_KEY, value=redis_key_out)
        
    except Exception as e:
        logger.error(f"transform_data 發生例外: {e}")
        raise

def output_data(ti, algorithm_module: str = "processor.load", algorithm_func: str = "csv_writer.standard", **algo_kwargs):
    """數據載入函數 - 強制要求資料夾結構格式"""
    try:
        logger.info("執行 load_data (資料匯入層)")
        
        # 獲取上游數據
        upstream_redis_keys = _get_upstream_output_path(ti)
        if not upstream_redis_keys:
            raise RuntimeError("load_data 無法從上游取得 redis_key")

        # 直接使用資料夾結構
        func = _get_algorithm_function(algorithm_module, algorithm_func)
        logger.info(f"呼叫方法 '{algorithm_func}' 來自模組 '{algorithm_module}'")
        
        if isinstance(upstream_redis_keys, list):
            data_list = []
            for key in upstream_redis_keys:
                dict_data = _load_dict_from_redis_hash(key)
                if not isinstance(dict_data, dict):
                    logger.error(f"取得的資料不是 dict，實際型態 {type(dict_data)}")
                data_list.append(dict_data)
   
            all_keys = set().union(*(d.keys() for d in data_list))
            result_dict = {
                key: pd.concat([d[key] for d in data_list if key in d], ignore_index=True)
                for key in all_keys
            }
            logger.info(f"合併結果欄位: {list(result_dict.keys())}")
            func(result_dict, **algo_kwargs)
        else:
            dict_data = _load_dict_from_redis_hash(upstream_redis_keys)
            func(dict_data, **algo_kwargs)

        # # 清理上游數據
        # redis_client.delete(upstream_redis_key)
        # logger.info(f"刪除上游 Redis key: {upstream_redis_key}")
        
    except Exception as e:
        logger.error(f"load_data 發生例外: {e}")
        raise

