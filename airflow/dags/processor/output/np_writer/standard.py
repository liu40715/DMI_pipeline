# processor/load/np_writer/standard.py
import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def execute(data, output_filename, **kwargs):
    """標準 NumPy 寫出方法"""
    try:
        # 如果輸入是 parquet 格式，先轉換為 numpy
        #if hasattr(data, 'read'):
        #    df = pd.read_parquet(data)
        #    np_data = df.to_numpy()
        #else:
        #    np_data = np.load(data, allow_pickle=True)
        np_data = np.load(data, allow_pickle=True)
        output_path = Path('/opt/airflow/data/output') / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(output_path, np_data)
        logger.info(f"成功寫出 NumPy: {output_path}, 資料形狀: {np_data.shape}")
    except Exception as e:
        logger.error(f"NumPy 寫出失敗: {e}")
        raise
