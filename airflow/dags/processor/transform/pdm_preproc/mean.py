import numpy as np
import logging
logger = logging.getLogger(__name__)


def execute(data: np.ndarray, fs: float, type: str, **kwargs) -> np.ndarray:

    """
    訊號處理函數：將時域訊號切成三等份，做混疊or平均處理

    Args:
        data (np.ndarray): 連續一段時間的時域資料
        fs (float): 採樣頻率 (Hz)
        type (str): 處理類型，支持 '線性平均', 'RMS平均', '重疊平均', '峰值保持'
    Returns:
        - 處理後的時域訊號 (np.ndarray): 3個區段處理後的訊號
    Raises:
        ValueError: 當輸入參數不正確時
        Exception: 當處理過程發生錯誤時
    """

    try:
        logger.info(f" 採樣頻率: {fs},  處理類型: {type}")
        # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if len(data) == 0:
            raise ValueError("data 不能為空")
        if fs <= 0:
            raise ValueError("採樣頻率 fs 必須大於 0")
        
        # 計算每個區段的長度
        segment_length = len(data) // 3
        # 如果資料長度不能被3整除，取最大可能的3等分長度
        if len(data) % 3 != 0:
            total_length = segment_length * 3
            data = data[:total_length]
        # 將資料分成3個區段
        segment1 = data[:segment_length]
        segment2 = data[segment_length:2*segment_length]
        segment3 = data[2*segment_length:3*segment_length]
        # 將3個區段疊加並處理
        if type == '線性平均':
            processed_signal = (segment1 + segment2 + segment3) / 3.0
        elif type == 'RMS平均':
            processed_signal = np.sqrt((segment1**2 + segment2**2 + segment3**2) / 3.0)
        elif type == '重疊平均':
            overlap_percent = 20.0
            # 計算重疊長度
            overlap_length = int(segment_length * overlap_percent / 100)
            # 計算步長（相鄰區段的起始位置差）
            step = segment_length - overlap_length
            # 重新定義區段以包含重疊
            segment1 = data[:segment_length]
            segment2 = data[step:step + segment_length]
            # 確保第三個區段不超出範圍
            end_start = min(2*step, len(data) - segment_length)
            segment3 = data[end_start:end_start + segment_length]

            # 建立輸出訊號容器
            output_length = 2*step + segment_length
            processed_signal = np.zeros(output_length)
            overlap_count = np.zeros(output_length)  # 記錄每個位置的重疊次數
            # 累加每個區段
            # 區段1
            processed_signal[:segment_length] += segment1
            overlap_count[:segment_length] += 1
            # 區段2
            end_idx2 = min(output_length, step + segment_length)
            processed_signal[step:end_idx2] += segment2[:end_idx2-step]
            overlap_count[step:end_idx2] += 1
            # 區段3
            start_idx3 = 2*step
            end_idx3 = min(output_length, start_idx3 + len(segment3))
            processed_signal[start_idx3:end_idx3] += segment3[:end_idx3-start_idx3]
            overlap_count[start_idx3:end_idx3] += 1
            # 對重疊區域取平均
            processed_signal = processed_signal / np.maximum(overlap_count, 1)
        elif type == '峰值保持':
            processed_signal = np.maximum(np.maximum(segment1, segment2), segment3)
        else:
            raise ValueError(f"不支持的處理類型: {type}")
    
        # 計算處理後資料的時長(秒)
        duration = len(processed_signal) / fs
        logger.info(f"訊號處理完成 - 原始長度: {len(data)}, 時長: {duration:.3f}秒, 解析度: {1/duration:.3f}Hz")

        return processed_signal
        
    except Exception as e:
        logger.error(f"signal_segment_average 發生例外: {e}")
        raise
