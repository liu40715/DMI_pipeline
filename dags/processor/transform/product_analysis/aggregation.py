# processor/transform/product_analysis/aggregation.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def execute(data, multiplier=1.0, **kwargs):
    """產品聚合分析方法"""
    try:
        data = data[0]
        df = pd.read_parquet(data)
        
        # 基礎聚合邏輯
        df['total_price'] = df['quantity'] * df['price'] * multiplier
        grouped = df.groupby(['product_id', 'product_name']).agg(
            total_quantity=('quantity', 'sum'),
            average_price=('price', 'mean'),
            total_revenue=('total_price', 'sum'),
            order_count=('product_id', 'count')
        ).reset_index()
        
        logger.info(f"產品聚合分析完成, 產品數量: {len(grouped)}")
        return grouped
        
    except Exception as e:
        logger.error(f"產品聚合分析失敗: {e}")
        raise
