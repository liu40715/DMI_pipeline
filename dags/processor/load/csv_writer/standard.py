# processor/load/csv_writer/standard.py
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def execute(data, output_filename, **kwargs):
    """標準 CSV 寫出方法"""
    try:
        df = pd.read_parquet(data)
        output_path = Path('/opt/airflow/data/output') / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"成功寫出 CSV: {output_path}, 資料筆數: {len(df)}")
    except Exception as e:
        logger.error(f"CSV 寫出失敗: {e}")
        raise
