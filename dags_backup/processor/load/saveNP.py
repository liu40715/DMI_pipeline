import numpy as np
from pathlib import Path
import logging
# 設定 logging
logging.basicConfig(
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def test_csv(data,output_filename,**kwargs):
    try:
        data = np.load(data, allow_pickle=True)
        output_path = Path('/opt/airflow/data/output') / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(output_path, data)
        print(f"Saved output npy to: {output_path}")
    except Exception as e:
        logger.error(f"extract_data 發生例外: {e}")
        raise
        