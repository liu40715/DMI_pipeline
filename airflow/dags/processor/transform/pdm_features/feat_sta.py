import numpy as np
import pandas as pd
from scipy import stats
import logging
logger = logging.getLogger(__name__)


def execute(data: list, **kwargs) -> pd.DataFrame:
    """
    統計特徵函數：將信號進行統計特徵提取

    Args:
        data (np.ndarray): 可以是以下格式之一：
            - dtype=object: DWT係數陣列，包含多個子帶
            - dtype=float64: FFT頻域陣列或時域訊號陣列

    Returns:
        - features (dict): 提取的統計特徵字典

    Raises:
        ValueError: 當輸入參數不正確時
        Exception: 當處理過程發生錯誤時
    """
    try:
        buffer = data[0]              # data[0] 已經是 BytesIO 物件，不用包
        buffer.seek(0)                # 確保指標從開頭開始
        data = np.load(buffer, allow_pickle=True)         # 獲得array格式
        # 參數檢查
        if not isinstance(data, np.ndarray):
            raise ValueError("data 必須是 numpy 陣列")
        if data.size == 0:
            raise ValueError("data 不能為空陣列")
        logger.info(f"開始統計特徵提取 - 資料長度: {len(data)}")
        # 根據 dtype 區分處理方式
        if data.dtype == object:
            # 處理DWT係數陣列（numpy object 陣列）
            logger.info(f"輸入為DWT係數陣列，子帶數量: {len(data)}")
            
            features = {}
            processed_subbands = 0
            # 對每個子帶分別提取特徵
            for i, subband_coeffs in enumerate(data):
                if not isinstance(subband_coeffs, np.ndarray):
                    subband_coeffs = np.array(subband_coeffs)
                if len(subband_coeffs) == 0:
                    logger.warning(f"子帶 {i} 沒有係數資料，跳過")
                    continue
                # 檢查是否有非零係數
                nonzero_coeffs = subband_coeffs[subband_coeffs != 0]
                if len(nonzero_coeffs) == 0:
                    logger.debug(f"子帶 {i} 所有係數為零，跳過")
                    continue
                logger.debug(f"子帶 {i}: 總係數 {len(subband_coeffs)}, 非零係數 {len(nonzero_coeffs)}")
                subband_coeffs = np.concatenate([np.asarray(x, dtype=np.float64).ravel() for x in subband_coeffs])
                # 計算該子帶的統計特徵
                subband_features = _calculate_statistical_features(subband_coeffs)
                # 為特徵名稱添加子帶前綴
                for key, value in subband_features.items():
                    features[f"subband_{i}_{key}"] = value
                
                processed_subbands += 1
            
            if processed_subbands == 0:
                raise ValueError("所有子帶都沒有有效的係數資料")
            logger.info(f"DWT陣列特徵提取完成 - 成功處理了 {processed_subbands}/{len(data)} 個子帶")

            df = pd.DataFrame(list(features.items()), columns=["Feature", "Value"])
            return df

        elif np.issubdtype(data.dtype, np.floating) or np.issubdtype(data.dtype, np.integer):
            # 處理數值陣列（FFT頻域或時域訊號）
            logger.info(f"輸入為數值陣列，形狀: {data.shape}")
            if not np.isfinite(data).all():
                raise ValueError("data 包含無限值或 NaN，請先清理資料")
            # 展平多維陣列
            if data.ndim > 1:
                logger.info(f"多維陣列展平處理，原形狀: {data.shape}")
                data = data.flatten()
            # 計算統計特徵
            features = _calculate_statistical_features(data)
            df = pd.DataFrame(list(features.items()), columns=["Feature", "Value"])
            return df
        else:
            raise ValueError(f"不支援的資料類型: {data.dtype}，必須是 object 或數值類型")
        
    except Exception as e:
        logger.error(f"feat_sta 發生例外: {e}")
        raise

def _calculate_statistical_features(data: np.ndarray) -> dict:
    """
    計算單一維度資料的統計特徵
    
    Args:
        data (np.ndarray): 一維資料陣列
        
    Returns:
        - features (dict): 統計特徵字典
    """
    
    # 基本統計量
    data_abs = np.abs(data)
    data_square = np.square(data)
    # 計算統計特徵
    features = {}
    # 基本統計特徵
    features["max"] = float(np.max(data))
    features["min"] = float(np.min(data))
    features["peak"] = float(np.max(data_abs))
    features["peak2peak"] = features["max"] - features["min"]
    features["mean"] = float(np.mean(data))
    features["std"] = float(np.std(data, ddof=1))
    features["var"] = float(np.var(data, ddof=1))
    features["q75"], features["q25"] = np.percentile(data, [75, 25])
    features["iqr"] = float(features["q75"] - features["q25"])
    features["cv"] = float(features["std"] / features["mean"]) if features["mean"] != 0 else 0.0
    # RMS 和相關特徵
    mean_abs = np.mean(data_abs)
    features["mean_abs"] = float(mean_abs)
    features["rms"] = float(np.sqrt(np.mean(data_square)))
    # 避免除零錯誤的保護
    if features["rms"] == 0:
        logger.warning("RMS 值為零，某些比值特徵將設為零")
        features["crest"] = 0.0
        features["clearance"] = 0.0
        features["shape"] = 0.0
        features["impulse"] = 0.0
    else:
        # 波峰因子 (Crest Factor)
        features["crest"] = features["peak"] / features["rms"]
        # 間隙因子 (Clearance Factor)
        mean_sqrt_abs = np.mean(np.sqrt(data_abs))
        if mean_sqrt_abs == 0:
            features["clearance"] = 0.0
        else:
            features["clearance"] = features["peak"] / (mean_sqrt_abs ** 2)
        # 形狀因子 (Shape Factor) & 脈衝因子 (Impulse Factor)
        if mean_abs == 0:
            features["shape"] = 0.0
            features["impulse"] = 0.0
        else:
            features["shape"] = features["rms"] / mean_abs 
            features["impulse"] = features["peak"] / mean_abs
    # 高階統計量
    try:
        features["skewness"] = float(stats.skew(data))
        features["kurtosis"] = float(stats.kurtosis(data, fisher=True))
    except Exception as e:
        logger.warning(f"計算偏度或峰態時發生警告: {e}")
        features["skewness"] = 0.0
        features["kurtosis"] = 0.0
    # 零交越率 (Zero Crossing Rate)
    if len(data) > 1:
        zero_crossings = np.sum(np.abs(np.diff(np.sign(data))))
        features["zero_crossing_rate"] = float(zero_crossings / (2 * (len(data) - 1)))
    else:
        features["zero_crossing_rate"] = 0.0
    # 額外有用的統計特徵
    features["median"] = float(np.median(data))
    features["mad"] = float(np.median(np.abs(data - features["median"])))
    features["energy"] = float(np.sum(data_square))
    features["entropy"] = _calculate_entropy(data)
    # 計算完成統計
    num_features = len(features)
    logger.debug(f"統計特徵計算完成 - 提取了 {num_features} 個特徵")
    return features

def _calculate_entropy(data: np.ndarray, bins: int = 50) -> float:
    """
    計算信號的熵

    Args:
        data (np.ndarray): 一維資料陣列
        bins (int): 直方圖的箱數

    Returns:
        - float: 訊號的熵值
    """
    try:
        # 將連續信號離散化
        hist, _ = np.histogram(data, bins=bins, density=True)
        hist = hist + 1e-12  # 避免log(0)
        # 正規化為機率分布
        prob = hist / np.sum(hist)
        # 計算熵
        entropy = -np.sum(prob * np.log2(prob + 1e-12))
        return float(entropy)
    except:
        return 0.0
