import numpy as np
from pathlib import Path
import logging
# 設定 logging
logging.basicConfig(
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def add_test(data,number,**kwargs):
    try:
        buffer = data[0]              # data[0] 已經是 BytesIO 物件，不用包
        buffer.seek(0)                # 確保指標從開頭開始
        arr = np.load(buffer)         # 獲得array格式
        new_data = arr + number
        logger.info("資料處理完成")
        return new_data
    except Exception as e:
        logger.error(f"extract_data 發生例外: {e}")
        raise
