# processor/transform/data_processing/add_column.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def execute(data: list, titlename: str='test111', **kwargs) -> pd.DataFrame:
    """新增欄位方法"""
    try:
        data = data[0]
        df = pd.read_parquet(data)
        # 新增欄位邏輯
        df[titlename] = [i for i in range(len(df))]
        logger.info(f"欄位新增完成, 新增欄位: {titlename}")
        return df

    except Exception as e:
        logger.error(f"欄位新增失敗: {e}")
        raise
