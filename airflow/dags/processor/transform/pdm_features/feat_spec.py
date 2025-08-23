import numpy as np
import logging
import pandas as pd
logger = logging.getLogger(__name__)


def execute(data: list, fs: float, rpm: float, Bearing: dict = None, **kwargs) -> dict:
    """
    頻域特徵函數：將頻域信號進行缺陷頻域特徵提取
    
    注意：不包含基本統計特徵（平均值、標準差、偏度、峰態），
    這些特徵由 feat_sta() 函數提供，避免重複。

    Args:
        data (np.ndarray): 頻域陣列
        fs (float): 採樣頻率 (Hz)
        rpm (float): 轉速 (RPM)
        Bearing (dict): 軸承缺陷頻率字典 {component_name: frequency}

    Returns:
        - features (dict): 提取的頻域特徵字典
    """
    try:
        buffer = data[0]              # data[0] 已經是 BytesIO 物件，不用包
        buffer.seek(0)                # 確保指標從開頭開始
        data = np.load(buffer)         # 獲得array格式
       # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if data.size == 0:
            raise ValueError("data 不能為空陣列")
        if not isinstance(fs, (int, float)):
            raise ValueError("fs 必須是數值類型")
        if fs <= 0:
            raise ValueError("採樣頻率 fs 必須大於 0")
        if data.ndim != 1:
            raise ValueError("data 必須是一維陣列")
        if not isinstance(rpm, (int, float)):
            raise ValueError("rpm 必須是數值類型")
        if rpm <= 0:
            raise ValueError("轉速 rpm 必須大於 0")
        if Bearing is not None and not isinstance(Bearing, dict):
            raise ValueError("Bearing 必須是字典類型")
        # 建立頻率軸
        frequencies = np.linspace(0, fs/2, len(data))
        freq_ratio = [0.25, 0.5, 1, 2, 3, 4, 8, 16]
        
        logger.info(f"開始頻域特徵提取 - 資料長度: {len(data)}, 採樣頻率: {fs} Hz, 轉速: {rpm} RPM")
        # 計算頻域特徵
        features = {}
        # OA (Overall Amplitude) - 總振幅
        features["OA"] = float(np.sqrt(np.sum(data**2)) * 0.8165)
        # 轉速相關頻率特徵
        for order in freq_ratio:
            target_freq = rpm * order / 60  # 轉換 RPM 到 Hz
            harmonic_freq = _get_harmonics_frequency(target_freq, frequencies, data)
            idx = np.where(np.isclose(frequencies, harmonic_freq, atol=fs/(2*len(data))))[0]
            features[f"FFT_{order}X"] = float(data[idx[0]]) if len(idx) > 0 else 0.0
        # 軸承缺陷頻率特徵（僅當 Bearing 有傳入時才計算）
        if Bearing:
            for component, base_freq in Bearing.items():
                if not isinstance(base_freq, (int, float)):
                    logger.warning(f"軸承頻率 {component} 不是數值類型，跳過")
                    continue
                vel_freq = _get_harmonics_frequency(base_freq, frequencies, data)
                vel_idx = np.where(np.isclose(frequencies, vel_freq, atol=fs/(2*len(data))))[0]
                features[f"spectrum_{component}"] = float(data[vel_idx[0]]) if len(vel_idx) > 0 else 0.0
                for harmonic in [2, 3]:
                    harmonic_target = base_freq * harmonic
                    if harmonic_target <= fs/2:  # 確保不超過Nyquist頻率
                        h_freq = _get_harmonics_frequency(harmonic_target, frequencies, data)
                        h_idx = np.where(np.isclose(frequencies, h_freq, atol=fs/(2*len(data))))[0]
                        features[f"spectrum_{component}_{harmonic}X"] = float(data[h_idx[0]]) if len(h_idx) > 0 else 0.0
                # 計算諧波總能量
                base_energy = features[f"spectrum_{component}"]**2
                harmonic_energy = 0
                for harmonic in [2, 3]:
                    harmonic_key = f"spectrum_{component}_{harmonic}X"
                    if harmonic_key in features:
                        harmonic_energy += features[harmonic_key]**2
                features[f"spectrum_{component}_total_energy"] = base_energy + harmonic_energy
        # 計算頻率加權統計量
        total_power = np.sum(data)
        if total_power == 0:
            logger.warning("資料總功率為零，某些頻域特徵將設為零")
            mean_val = 0.0
            std_val = 0.0
        else:
            mean_val = np.sum(data * frequencies) / total_power  # 重心頻率
            std_val = np.sqrt(np.sum(data * (frequencies - mean_val) ** 2) / total_power) # 頻率標準差        
        # 頻域專有特徵
        features["freq_centroid"] = float(mean_val) # 重心頻率
        features["freq_spread"] = float(std_val) # 頻率標準差
        # 避免除零錯誤的高階特徵
        if total_power > 0:
            features["freq_rms"] = float(np.sqrt(np.sum(data * frequencies ** 2) / total_power)) # RMS 頻率
            sum_freq2 = np.sum(data * frequencies ** 2)
            if sum_freq2 > 0:
                features["freq_variance"] = float(np.sqrt(np.sum(data * frequencies ** 4) / sum_freq2)) # 變異頻率
                features["freq_irregularity"] = float(sum_freq2 / np.sqrt(total_power * np.sum(data * frequencies ** 4))) # 頻率不規則性
            else:
                features["freq_variance"] = 0.0
                features["freq_irregularity"] = 0.0
        else:
            features["freq_rms"] = 0.0
            features["freq_variance"] = 0.0
            features["freq_irregularity"] = 0.0
        # 比值特徵（避免除零）
        features["freq_cov"] = float(std_val / mean_val) if mean_val != 0 else 0.0 # 頻率變異係數
        if std_val != 0:
            features["freq_skewness"] = float(np.sum(((frequencies - mean_val) ** 3) * data) / (total_power * std_val ** 3)) # 頻率偏度
            features["freq_kurtosis"] = float(np.sum(((frequencies - mean_val) ** 4) * data) / (total_power * std_val ** 4)) # 頻率峰態
            features["freq_roughness"] = float(np.sum(data * np.sqrt(np.abs(frequencies - mean_val))) / (total_power * np.sqrt(std_val))) # 頻率粗糙度
        else:
            features["freq_skewness"] = 0.0
            features["freq_kurtosis"] = 0.0
            features["freq_roughness"] = 0.0
        features["freq_rolloff"] = float(np.sqrt(np.sum(data * (frequencies - mean_val) ** 2) / total_power)) if total_power > 0 else 0.0 # 頻率滾降
        logger.info(f"頻域特徵提取完成 - 提取了 {len(features)} 個特徵")
        df = pd.DataFrame(list(features.items()), columns=["Feature", "Value"])
        return df

    except Exception as e:
        logger.error(f"feat_spec 發生例外: {e}")
        raise


def _get_harmonics_frequency(target_freq: float, spectrum_hz: np.ndarray, spectrum_velocity: np.ndarray) -> float:
    """
    計算目標頻率的諧波頻率，找到目標頻率附近的最大振幅對應頻率

    Args:
        target_freq (float): 目標頻率 (Hz)
        spectrum_hz (np.ndarray): 頻率軸 (Hz)
        spectrum_velocity (np.ndarray): 頻域振幅陣列

    Returns:
        float: 最接近目標頻率且振幅最大的頻率
    """
    try:
        # 參數檢查
        if target_freq <= 0:
            logger.warning(f"目標頻率必須大於零，實際值: {target_freq}")
            return target_freq
        if len(spectrum_hz) != len(spectrum_velocity):
            logger.error("頻率軸和振幅陣列長度不匹配")
            return target_freq
        # 定義搜尋範圍 (目標頻率的±5%) 
        tolerance = max(0.05, 1.0 / target_freq)  # 針對低頻的容差
        lower_bound = target_freq * (1 - tolerance)
        upper_bound = target_freq * (1 + tolerance)
        # 在搜尋範圍內找到頻率
        mask = (spectrum_hz >= lower_bound) & (spectrum_hz <= upper_bound)
        filtered_freq = spectrum_hz[mask]
        filtered_amp = spectrum_velocity[mask]
        # 如果沒有找到符合條件的頻率，返回目標頻率
        if len(filtered_amp) == 0:
            logger.debug(f"在範圍 [{lower_bound:.2f}, {upper_bound:.2f}] Hz 內沒有找到頻率點，返回目標頻率 {target_freq:.2f} Hz")
            return target_freq
        # 找到振幅最大的頻率
        max_amp_idx = np.argmax(filtered_amp)
        harmonic_freq = filtered_freq[max_amp_idx]      
        logger.debug(f"目標頻率 {target_freq:.2f} Hz，找到諧波頻率 {harmonic_freq:.2f} Hz，振幅 {filtered_amp[max_amp_idx]:.4f}")
        
        return float(harmonic_freq)

    except Exception as e:
        logger.error(f"_get_harmonics_frequency 發生例外: {e}")
        return target_freq  # 出錯時返回原始目標頻率
