# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from typing import Optional
from utils.logger import Logger

logger = Logger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _meta_path(name: str) -> str:
    return os.path.join(CACHE_DIR, f"{name}.meta")


def _data_path(name: str) -> str:
    return os.path.join(CACHE_DIR, f"{name}.csv")


def save_cache(name: str, df: pd.DataFrame) -> None:
    _ensure_cache_dir()
    data_file = _data_path(name)
    meta_file = _meta_path(name)
    df.to_csv(data_file, index=False, encoding="utf-8-sig")
    with open(meta_file, "w") as f:
        f.write(str(time.time()))
    logger.info(f"[缓存] 已保存 {name}，共 {len(df)} 条记录")


def load_cache(
    name: str,
    max_age_hours: float = 24.0,
    allow_stale: bool = False,
    stale_max_age_hours: Optional[float] = None,
) -> Optional[pd.DataFrame]:
    data_file = _data_path(name)
    meta_file = _meta_path(name)

    if not os.path.exists(data_file) or not os.path.exists(meta_file):
        return None

    with open(meta_file, "r") as f:
        try:
            saved_ts = float(f.read().strip())
        except (ValueError, OSError):
            return None

    age_hours = (time.time() - saved_ts) / 3600

    if age_hours > max_age_hours:
        if allow_stale and (stale_max_age_hours is None or age_hours <= stale_max_age_hours):
            logger.warning(f"[缓存] {name} 已过期（{age_hours:.1f}h），使用陈旧缓存降级")
        else:
            logger.info(f"[缓存] {name} 已过期（{age_hours:.1f}h > {max_age_hours}h）")
            return None

    try:
        df = pd.read_csv(data_file, encoding="utf-8-sig", dtype={"code": str})
        logger.info(f"[缓存] 命中 {name}，共 {len(df)} 条，缓存年龄 {age_hours:.1f}h")
        return df
    except Exception as e:
        logger.warning(f"[缓存] 读取 {name} 失败: {e}")
        return None
