import numpy as np
import logging
logger = logging.getLogger(__name__)


def execute(data: np.ndarray, type: str ="hanning", **kwargs) -> np.ndarray:
    """
    窗函數：將時域訊號套上窗函數以減少頻譜洩漏，其在訊號的開始和結束處平滑地降低訊號強度
    
    Args:
        data (np.ndarray): 連續一段時間的時域資料
        type (str): 窗函數類型，支持 'hanning', 'flattop', 'rectangular'

    Returns:
        - processed_signal (np.ndarray): 套用窗函數後的訊號
    Raises:
        ValueError: 當輸入參數不正確時
        Exception: 當處理過程發生錯誤時
    """
    try:
        logger.info(f" 窗函數類型: {type}")
        # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if len(data) == 0:
            raise ValueError("data 不能為空")

        # 窗函數處理
        if type == "hanning":
            window = np.hanning(len(data))
        elif type == "flattop":
            # flattop 窗的係數
            a0 = 0.21557895
            a1 = 0.41663158
            a2 = 0.277263158
            a3 = 0.083578947
            a4 = 0.006947368
            N = len(data)
            n = np.arange(N)
            # flattop 窗函數公式
            window = (a0 - a1*np.cos(2*np.pi*n/(N-1)) + 
                     a2*np.cos(4*np.pi*n/(N-1)) - 
                     a3*np.cos(6*np.pi*n/(N-1)) + 
                     a4*np.cos(8*np.pi*n/(N-1)))
        elif type == "rectangular":
            window = np.ones(len(data))
        else:
            raise ValueError(f"不支持的窗函數類型: {type}")
        processed_signal = data * window

        return processed_signal

    except Exception as e:
        logger.error(f"windows 發生例外: {e}")
        raise
