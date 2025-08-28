import numpy as np
from pathlib import Path
import logging
import requests
import json
from datetime import datetime, timedelta, timezone

AUTH_URL = "http://192.168.0.101:5290/api/auth/server-token"
DATA_URL = "http://192.168.0.101:5280/api/raw-data"
logger = logging.getLogger(__name__)

def execute(tags: list, buffer: int, **kwargs):
    try:
        dict_data = {}
        token = _get_access_token() 
        for tag in tags:
            data = _fetch_raw_data(token,buffer,tag)
            dict_data[tag] = data
        logger.info(f"成功讀取")
        return dict_data
    except Exception as e:
        logger.error(f"讀取失敗: {e}")
        raise

def _get_access_token():
    files = {
        "ClientId": (None, "6638e2f9-516d-434f-951b-da739d1bd649"),
        "ClientSecret": (None, "admin123"),
        "Scope": (None, "string"),
        "GrantType": (None, "2"),
    }
    headers = {"accept": "application/json"}
    r = requests.post(AUTH_URL, files=files, headers=headers, timeout=10)
    r.raise_for_status()
    data = r.json()  # 使用 requests 內建 JSON 解析較為便利 [web:78][web:76]
    return data["data"]["accessToken"]  # 直接取出 accessToken [web:78]

def _fetch_raw_data(token: str, buffer: int, tagID: str):
    """
    buffer: 以毫秒為單位。
    - EndTime = 現在時間
    - StartTime = EndTime - buffer
    - 時間格式：YYYY-MM-DD HH:MM:SS
    - 回傳：將回應中的 data.list[*].arrayValue 全部解析並攤平成同一個 list
    """
    if buffer < 0:
        raise ValueError("buffer 必須是非負整數（毫秒）")

    now = datetime.now()
    start_dt = now - timedelta(milliseconds=buffer)

    params = {
        "DataTagIds": tagID,
        "StartTime": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "EndTime": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    headers = {
        "accept": "application/json",  # 若資料端回 text/plain 也可行，這裡用 JSON 解析更方便 [web:78]
        "Authorization": f"Bearer {token}",
    }
    r = requests.get(DATA_URL, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    obj = r.json()  # 直接轉成 dict 操作 [web:78][web:67]

    items = obj.get("data", {}).get("list", [])
    out = []
    for it in items:
        s = it.get("arrayValue")
        if s:
            out.extend(json.loads(s))  # 版本B：arrayValue 是像 "[1,2,3]" 的 JSON 字串 [web:67]
    return out
