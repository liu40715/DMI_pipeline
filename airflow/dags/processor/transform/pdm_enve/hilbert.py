import numpy as np
import logging
from scipy import signal
from scipy.signal import firwin, filtfilt
logger = logging.getLogger(__name__)


def execute(data: np.ndarray, fs: float, rpm: float, **kwargs) -> np.ndarray:
    """
    包絡轉換函數：將訊號進行希爾伯特變換以獲得包絡線

    Args:
        data (np.ndarray): 連續一段時間的時域資料
        fs (float): 採樣頻率 (Hz)
        rpm (float): 轉速 (RPM)

    Returns:
        - envelope (np.ndarray): 訊號的包絡線
    Raises:
        ValueError: 當輸入參數不正確時
        Exception: 當處理過程發生錯誤時
    """
    try:
        # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if len(data) == 0:
            raise ValueError("data 不能為空")
        if not isinstance(fs, (int, float)):
            raise ValueError("fs 必須是數值類型")
        if fs <= 0:
            raise ValueError("取樣頻率 fs 必須大於 0")
        if not isinstance(rpm, (int, float)):
            raise ValueError("rpm 必須是數值類型")
        if rpm <= 0:
            raise ValueError("轉速 rpm 必須大於 0")
        
        # 設定參數，不同轉速對應的包絡頻譜範圍
        if rpm <= 600:
            lowcut, highcut = 50, 2000
        elif 600 < rpm <= 2400:
            lowcut, highcut = 100, 10000
        else:
            lowcut, highcut = 1000, 40000

        # 過濾信號
        filtered_signal = __filter(data, fs, lowcut, highcut) # 根據ISO-10816選取過濾頻率範圍
        # 計算希爾伯特變換進行包絡檢測
        envelope = np.abs(signal.hilbert(filtered_signal))
        # 在[2, 800]Hz範圍內解調
        envelope_filtered = __filter(envelope, fs, 2, 800)
        logger.info("包絡線計算完成")
        return envelope_filtered
    except Exception as e:
        logger.error(f"hilbert 發生例外: {e}")
        raise

def __filter(data: np.ndarray, fs: float, lowcut: float, highcut: float, **kwargs) -> np.ndarray:
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
