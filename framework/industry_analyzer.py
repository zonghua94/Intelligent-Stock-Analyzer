# -*- coding: utf-8 -*-

from datetime import date
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class IndustryAnalyzer:
    def __init__(self, fetcher_manager):
        self.fetcher_manager = fetcher_manager

    def get_industry_ranks(self):
        pass

    def get_industry_infos(self, stock_codes, code_infos):
        results = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(
                    self._get_industry_info,
                    code
                ): (code, info)
                for code, info in zip(stock_codes, code_infos)
            }
            # 收集结果
            for idx, future in enumerate(as_completed(future_to_code)):
                code, info = future_to_code[future]
                try:
                    result = future.result()
                    info['行业'] = result
                    results.append(info)

                except Exception as e:
                    logger.error(f"[{code}] 任务执行失败: {e}")
        return results

    def _get_industry_info(self, stock_code):
        industry = self.fetcher_manager.get_industry_info(stock_code)
        return industry