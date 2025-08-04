# processor/extract/np_reader/standard.py
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def execute(input_filename: str, **kwargs):
    """標準 NumPy 讀取方法"""
    try:
        input_path = Path('/opt/airflow/data/input') / input_filename
        data = np.load(input_path)
        logger.info(f"成功讀取 NumPy: {input_filename}, 資料形狀: {data.shape}")
        return data
    except Exception as e:
        logger.error(f"NumPy 讀取失敗: {e}")
        raise
