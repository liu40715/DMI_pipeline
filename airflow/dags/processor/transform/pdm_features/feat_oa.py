import numpy as np
import pandas as pd
import logging
logger = logging.getLogger(__name__)


def execute(data: np.ndarray, **kwargs) -> pd.DataFrame:
    try:
        # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if data.size == 0:
            raise ValueError("data 不能為空陣列")
        features = {}
        value = float(np.sqrt(np.sum(data**2))*0.8165)
        features['OA'] = value
        df = pd.DataFrame(list(features.items()), columns=["Feature", "Value"])
        return df
    except Exception as e:
        logger.error(f"feat_sta 發生例外: {e}")
        raise
