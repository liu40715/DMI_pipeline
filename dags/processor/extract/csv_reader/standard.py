# processor/extract/csv_reader/standard.py
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def execute(input_filename: str, **kwargs) -> pd.DataFrame:
    """標準 CSV 讀取方法"""
    try:
        input_path = Path('/opt/airflow/data/input') / input_filename
        df = pd.read_csv(input_path)
        logger.info(f"成功讀取 CSV: {input_filename}, 資料筆數: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"CSV 讀取失敗: {e}")
        raise
