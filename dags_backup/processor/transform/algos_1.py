import pandas as pd
import logging
# 設定 logging
logging.basicConfig(
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def add_test(data, titlename='test111',**kwargs):
    try:
        data = data[0]               # data[0] 已經是 BytesIO 物件，不用包
        df = pd.read_parquet(data)         # 獲得dataframe格式
        df[titlename] = [i for i in range(len(df['quantity']))]
        logger.info("資料處理完成")
        return df
    except Exception as e:
        logger.error(f"extract_data 發生例外: {e}")
        raise
           
def aggregate_product_data(data, multiplier=1.0,**kwargs):
    try:
        data = data[0]    
        df = pd.read_parquet(data)
        df['total_price'] = df['quantity'] * df['price'] * multiplier
        grouped = df.groupby(['product_id', 'product_name']).agg(
            total_quantity=('quantity', 'sum'),
            average_price=('price', 'mean')
        ).reset_index()
        logger.info("資料處理完成")
        return grouped
    except Exception as e:
        logger.error(f"extract_data 發生例外: {e}")
        raise
    
    
