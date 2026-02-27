# -*- coding: utf-8 -*-

from datetime import date
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class IndustryAnalyzer:
    def __init__(self, fetcher_manager):
        self.fetcher_manager = fetcher_manager

    def get_industry_infos(self, stock_codes, code_infos):
        results = []
        for code, info in zip(stock_codes, code_infos):
            results.append(self._get_industry_info(code, info))
        return results

    def _get_industry_info(self, stock_code, stock_info):
        industry = self.fetcher_manager.get_industry_info(stock_code)
        stock_info['行业'] = industry
        return stock_info