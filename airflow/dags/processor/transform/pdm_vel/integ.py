import numpy as np
import logging
logger = logging.getLogger(__name__)

def execute(data: np.ndarray, fs: float, **kwargs) -> np.ndarray:
    """
    積分函數：使用精確梯形法將加速度訊號積分轉換為速度訊號，並處理漂移
    
    Args:
        data (np.ndarray): 加速度信號，單位為g
        fs (float): 取樣頻率（Hz）

    Returns:
        - velocity_mms (np.ndarray): 轉換後的速度訊號，單位為mm/s
    
    Raises:
        ValueError: 當輸入參數不正確時
        Exception: 當處理過程發生錯誤時
    """
    try:
        # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if data.size == 0:
            raise ValueError("data 不能為空陣列")
        if not isinstance(fs, (int, float)):
            raise ValueError("fs 必須是數值類型")
        if fs <= 0:
            raise ValueError("取樣頻率 fs 必須大於 0")
        if data.size < 2:
            raise ValueError("資料長度必須至少為 2 個樣本點才能進行積分")

        # 強制轉為 1D float64，將無法解析的元素視為 NaN
        a = np.asarray(data, dtype=object).ravel()
        if a.dtype == object:
            a_conv = np.empty(a.size, dtype=np.float64)
            for i, v in enumerate(a):
                try:
                    if isinstance(v, (bytes, bytearray)):
                        v = v.decode('utf-8', 'ignore')
                    a_conv[i] = float(v)
                except Exception:
                    a_conv[i] = np.nan
            a = a_conv
        else:
            a = a.astype(np.float64, copy=False)

        # 過濾非有限值
        finite_mask = np.isfinite(a)
        if not np.all(finite_mask):
            dropped = int(np.sum(~finite_mask))
            logger.warning(f"發現非有限值 {dropped} 個，將予以過濾")
            a = a[finite_mask]
        if a.size < 2:
            raise ValueError("有效數據點少於 2，無法積分")

        # 移除直流分量（去除偏移）
        data_mean = np.mean(a)
        acceleration_processed = a - data_mean

        # 將加速度從 g 轉換為 m/s²
        acceleration_ms2 = acceleration_processed * 9.81

        # 計算時間間隔
        dt = 1.0 / float(fs)

        # 使用精確的梯形法積分（向量化）
        # v[n] = v[n-1] + 0.5*(a[n-1] + a[n])*dt
        # 等價於 v[0]=0, v[1:] = cumsum(0.5*(a[:-1]+a[1:])*dt)
        incr = 0.5 * (acceleration_ms2[:-1] + acceleration_ms2[1:]) * dt
        velocity_ms = np.empty(acceleration_ms2.size, dtype=np.float64)
        velocity_ms[0] = 0.0
        velocity_ms[1:] = np.cumsum(incr)

        # 移除線性漂移（去趨勢化）
        # 使用物理時間軸（秒）且零基準化，數值條件較佳
        N = velocity_ms.size
        time_points = np.arange(N, dtype=np.float64) * dt
        t0 = time_points - time_points[0]

        # 避免 dtype 問題，並確保樣本足夠
        mask = np.isfinite(t0) & np.isfinite(velocity_ms)
        if np.count_nonzero(mask) >= 2:
            coeffs = np.polyfit(t0[mask], velocity_ms[mask], 1)
            linear_trend = np.polyval(coeffs, t0)
            velocity_ms = velocity_ms - linear_trend
        else:
            logger.warning("有效樣本不足以去趨勢，跳過線性漂移移除")

        # 轉換為 mm/s
        velocity_mms = velocity_ms * 1000.0

        logger.info("加速度積分轉換速度")
        return velocity_mms

    except Exception as e:
        logger.error(f"integ 發生例外: {e}")
        raise
