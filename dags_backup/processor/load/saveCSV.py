import pandas as pd
from pathlib import Path
import logging
# 設定 logging
logging.basicConfig(
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def test_csv(data,output_filename,**kwargs):
    try:    
        df = pd.read_parquet(data)
        output_path = Path('/opt/airflow/data/output') / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"Saved output CSV to: {output_path}")
    except Exception as e:
        logger.error(f"extract_data 發生例外: {e}")
        raise
        