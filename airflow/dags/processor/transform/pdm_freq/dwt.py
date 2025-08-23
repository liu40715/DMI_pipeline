import numpy as np
import logging
import pywt
logger = logging.getLogger(__name__)


def execute(data: list, **kwargs) -> np.ndarray:
    """
    離散小波變換函數：將時域信號進行離散小波變換

    Args:
        data (np.ndarray): 連續一段時間的時域資料

    Returns:
        - coefficient (np.ndarray): 小波係數矩陣，每一行代表一個子帶的係數
        
    Raises:
        ValueError: 當輸入參數不正確時
        Exception: 當處理過程發生錯誤時
    """
    try:
        buffer = data[0]              # data[0] 已經是 BytesIO 物件，不用包
        buffer.seek(0)                # 確保指標從開頭開始
        data = np.load(buffer)         # 獲得array格式
        # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if len(data) == 0:
            raise ValueError("data 不能為空")
        
        # 執行離散小波變換
        wp = pywt.WaveletPacket(data, 'db1', 'symmetric', maxlevel=5)
        # 提取所有葉節點的係數
        nodes = [node.path for node in wp.get_level(5, 'natural')]
        coefficient= np.array([wp[node].data for node in nodes], dtype=object)

        # 返回係數列表
        logger.info(f"離散小波變換完成 - 子帶數量: {len(coefficient)}")
        return coefficient
        
    except Exception as e:
        logger.error(f"dwt 發生例外: {e}")
        raise
