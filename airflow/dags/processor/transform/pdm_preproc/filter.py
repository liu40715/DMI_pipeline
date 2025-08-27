import numpy as np
import logging
from scipy.signal import firwin, filtfilt
logger = logging.getLogger(__name__)


def execute(data: np.ndarray, fs: float, lowcut: float, highcut: float, **kwargs) -> np.ndarray:
    """
    濾波函數：將時域訊號進行帶通濾波處理
    
    Args:
        data (np.ndarray): 連續一段時間的時域資料
        fs (float): 採樣頻率 (Hz)
        lowcut (float): 低頻截止頻率 (Hz)
        highcut (float): 高頻截止頻率 (Hz)

    Returns:
        - processed_signal (np.ndarray): 帶通濾波後的時域訊號
    Raises:
        ValueError: 當輸入參數不正確時
        Exception: 當處理過程發生錯誤時
    """
    try:
        logger.info(f" 採樣頻率: {fs},  低頻截止頻率: {lowcut}, 高頻截止頻率: {highcut}")
        # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if len(data) == 0:
            raise ValueError("data 不能為空")
        if lowcut <= 0 or highcut <= 0:
            raise ValueError("截止頻率必須大於 0")
        if lowcut >= highcut:
            raise ValueError("低頻截止頻率必須小於高頻截止頻率")
        # 確保頻率在有效範圍內
        nyq = 0.5 * fs
        low = max(1e-6, min(lowcut / nyq, 1-(1e-6)))
        high = max(1e-6, min(highcut / nyq, 1-(1e-6)))
        # 設計FIR濾波器
        taps = firwin(101, [low, high], window='hamming', pass_zero=False)
        processed_signal = filtfilt(taps, 1.0, data)

        return processed_signal

    except Exception as e:
        logger.error(f"filter 發生例外: {e}")
        raise
