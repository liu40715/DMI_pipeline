import pandas as pd
from pathlib import Path
import logging
# 設定 logging
logging.basicConfig(
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def test_csv(input_filename: str,**kwargs) -> pd.DataFrame:
    try:    
        input_path = Path('/opt/airflow/data/input') / input_filename
        df = pd.read_csv(input_path)
        return df
    except Exception as e:
        logger.error(f"extract_data 發生例外: {e}")
        raise
        

