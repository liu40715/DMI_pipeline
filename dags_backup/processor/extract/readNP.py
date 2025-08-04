import numpy as np
from pathlib import Path
import logging
# 設定 logging
logging.basicConfig(
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def test_csv(input_filename,**kwargs):
    try:
        input_path = Path('/opt/airflow/data/input') / input_filename
        data = np.load(input_path)
        return data
    except Exception as e:
        logger.error(f"extract_data 發生例外: {e}")
        raise