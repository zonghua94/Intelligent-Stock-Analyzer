# -*- coding: utf-8 -*-
"""
验证 base_info_filter 和 income_filter 的数据获取是否正常
"""

import sys
import time
from utils.config import FetcherArgs, FilterArgs
from framework.data_fetch_manager import DataFetcherManager

def test_all_realtime_quote(manager):
    print("=" * 60)
    print("测试1: get_all_realtime_quote（全市场行情）")
    print("=" * 60)
    start = time.time()
    df = manager.get_all_realtime_quote()
    elapsed = time.time() - start

    if df is None or df.empty:
        print(f"[FAIL] 获取失败，返回 None 或空 DataFrame，耗时 {elapsed:.2f}s")
        return False

    print(f"[OK] 获取成功，共 {len(df)} 条，耗时 {elapsed:.2f}s")
    print(f"  列名: {df.columns.tolist()}")

    for col in ['code', 'name', 'total_mv']:
        if col not in df.columns:
            print(f"  [WARN] 缺少必需列: {col}")

    sample = df[['code', 'name', 'total_mv']].head(5) if all(c in df.columns for c in ['code', 'name', 'total_mv']) else df.head(5)
    print(f"  示例数据:\n{sample.to_string(index=False)}")

    # 模拟 base_info_filter 逻辑
    BASE_MARKET_VALUE = 5000000000
    df_filtered = df[~df['name'].str.contains("ST", na=False)]
    df_filtered = df_filtered[df_filtered['total_mv'] > BASE_MARKET_VALUE]
    print(f"  模拟 base_info_filter: 非ST且市值>50亿 → {len(df_filtered)} 只股票")
    return True


def test_batch_income_data(manager):
    print()
    print("=" * 60)
    print("测试2: get_batch_income_data（批量业绩数据）")
    print("=" * 60)
    quarter = manager._get_latest_quarter_date()
    print(f"  最新季报日期: {quarter}")

    start = time.time()
    df = manager.get_batch_income_data()
    elapsed = time.time() - start

    if df is None or df.empty:
        print(f"[FAIL] 获取失败，返回 None 或空 DataFrame，耗时 {elapsed:.2f}s")
        return False

    print(f"[OK] 获取成功，共 {len(df)} 条，耗时 {elapsed:.2f}s")
    print(f"  列名: {df.columns.tolist()}")

    for col in ['code', 'income_inc']:
        if col not in df.columns:
            print(f"  [WARN] 缺少必需列: {col}")

    sample = df.head(5)
    print(f"  示例数据:\n{sample.to_string(index=False)}")

    # 模拟 income_filter 逻辑
    BASE_INCOME_INCREASE = 10
    df['income_inc'] = df['income_inc'].astype(float, errors='ignore')
    df_filtered = df[df['income_inc'] >= BASE_INCOME_INCREASE]
    print(f"  模拟 income_filter: 收入增速>={BASE_INCOME_INCREASE}% → {len(df_filtered)} 只股票")
    return True


def test_cache_stale():
    print()
    print("=" * 60)
    print("测试3: load_cache stale 参数")
    print("=" * 60)
    from utils.cache import load_cache
    result = load_cache("nonexistent_cache", max_age_hours=24.0, allow_stale=True, stale_max_age_hours=72.0)
    if result is None:
        print("[OK] 不存在的缓存正确返回 None")
    else:
        print("[FAIL] 不存在的缓存应该返回 None")
        return False
    return True


if __name__ == "__main__":
    fetcher_args = FetcherArgs()
    manager = DataFetcherManager(fetcher_args)

    results = []
    results.append(("cache stale 参数", test_cache_stale()))
    results.append(("全市场行情", test_all_realtime_quote(manager)))
    results.append(("批量业绩数据", test_batch_income_data(manager)))

    print()
    print("=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    all_pass = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name}")

    sys.exit(0 if all_pass else 1)
