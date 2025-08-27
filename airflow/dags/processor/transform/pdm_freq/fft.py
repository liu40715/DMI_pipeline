import numpy as np
import logging
logger = logging.getLogger(__name__)


def execute(data: np.ndarray, fs: float, **kwargs) -> np.ndarray:
    """
    快速傅立葉變換函數：將時域信號轉換為頻域表示

    Args:
        data (np.ndarray): 連續一段時間的時域資料
        fs (float): 採樣頻率 (Hz)

    Returns:
        - spectrum (np.ndarray): 頻域幅度
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

        # 計算FFT
        FFT_X = np.fft.fft(data)
        fft_result  = 1.41421 * abs(FFT_X) * 2 / len(FFT_X)
        # 計算頻率軸
        freq = np.fft.fftfreq(len(data), d=1/fs)
        # 只保留正頻率部分
        spectrum = fft_result[freq >= 0]
        logger.info("FFT 計算完成")
        return spectrum
    except Exception as e:
        logger.error(f"fft 發生例外: {e}")
        raise
