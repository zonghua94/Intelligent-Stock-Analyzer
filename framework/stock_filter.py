# -*- coding: utf-8 -*-

import logging
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import List, Dict, Any, Optional, Tuple

from .data_fetch_manager import DataFetcherManager
from utils.config import FilterArgs
from utils.logger import Logger

logger = Logger(__name__)

BASE_DAILY_AMOUNT = 200000000
BASE_MARKET_VALUE = 5000000000
BASE_INCOME_INCREASE = 0.1

MA20_AMOUNT = 300000000
MA20_AMOUNT_FACTOR = 1.3
EMA200_SLOP = -0.05
GLODEN_CROSS_DAYS = 1
CROSS_COUNT_10D = 1
MACD_HISTOGRAM = 0

EMA200_DEVIATION_RATE_HIGH = 25
EMA200_DEVIATION_RATE_LOW = 15
INCOME_INCREASE_20D = 25
INCOME_INCREASE_60D = 50
ATR_RATE = 4.0
BOLL_PCT_B = 0.9

RISK_SCORE_THRESHOLD = 1

class StockFilter:
    
    def __init__(
        self,
        args: Optional[FilterArgs] = None,
        max_workers: Optional[int] = None,
    ):
        if args is None:
            args = FilterArgs()
        self.args = args
        self.max_workers = max_workers or self.args.max_workers
        # 初始化各模块
        self.fetcher_manager = DataFetcherManager(args.fetcher_args)
        logger.info(f"调度器初始化完成，最大并发数: {self.max_workers}")

    def filter_stocks(self) -> List[str]:
        stock_codes = self.base_info_filter()
        # stock_codes = ['300251', '688147', '688702', '688409', '300486', '688280', '920808', '002498']
        stock_codes = self.income_filter(stock_codes)
        print(len(stock_codes))
        stock_codes = self.history_info_filter(stock_codes)
        print(stock_codes)
        return stock_codes

    def base_info_filter(self) -> List[str]:
        df_all = self.fetcher_manager.get_all_realtime_quote()
        df_filtered = df_all[~df_all['name'].str.contains("ST")]
        df_filtered = df_filtered[df_filtered['amount'] > BASE_DAILY_AMOUNT] # 单日成交额
        df_filtered = df_filtered[df_filtered['total_mv'] > BASE_MARKET_VALUE] # 总市值
        stock_list = df_filtered['code'].tolist()
        return stock_list

    def income_filter(self, stock_list:List[str]) -> List[str]:
        print(stock_list)
        all_income_data = []
        with ThreadPoolExecutor(max_workers=self.args.max_workers) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(
                    self.fetcher_manager.get_income_data,
                    code
                ): code
                for code in stock_list
            }
            
            # 收集结果
            for idx, future in enumerate(as_completed(future_to_code)):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result:
                        all_income_data.append(result)

                    # Issue #128: 分析间隔 - 在个股分析和大盘分析之间添加延迟
                    if idx < len(stock_list) - 1 and self.args.analysis_delay > 0:
                        logger.debug(f"等待 {self.args.analysis_delay} 秒后继续下一只股票...")
                        time.sleep(self.args.analysis_delay)

                except Exception as e:
                    logger.error(f"[{code}] 任务执行失败: {e}")

        df_filtered = pd.DataFrame(all_income_data)
        print(df_filtered)
        df_filtered = df_filtered[df_filtered['income_inc'] >= BASE_INCOME_INCREASE]
        return df_filtered['code'].tolist()

    def history_info_filter(self, stock_list:List[str]) -> List[str]:
        logger.info(f"开始处理{len(stock_list)}只股票: {stock_list}")
        filted_stocks = []
        with ThreadPoolExecutor(max_workers=self.args.max_workers) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(
                    self._history_info_filter,
                    code
                ): code
                for code in stock_list
            }
            
            # 收集结果
            for idx, future in enumerate(as_completed(future_to_code)):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result:
                        filted_stocks.append(code)

                    # Issue #128: 分析间隔 - 在个股分析和大盘分析之间添加延迟
                    if idx < len(stock_list) - 1 and self.args.analysis_delay > 0:
                        logger.debug(f"等待 {self.args.analysis_delay} 秒后继续下一只股票...")
                        time.sleep(self.args.analysis_delay)

                except Exception as e:
                    logger.error(f"[{code}] 任务执行失败: {e}")
        logger.info(f"处理完成，剩余{len(filted_stocks)}只股票: {filted_stocks}")
        return filted_stocks

    def _history_info_filter(self, stock_code: str):
        stock_data = self.fetcher_manager.get_daily_analyzed_data(stock_code)
        if stock_data is None:
            return False
        trend_result = self._single_trend_filter(stock_data)
        risk_result = self._single_risk_filter(stock_data)
        additional_result = self._single_additional_filter(stock_data)
        return trend_result and risk_result and additional_result

    def _single_trend_filter(self, stock_data):
        # 20日成交额 > 3亿 && 当日成交量 》= 1.3 x 20日均量
        if stock_data['amount_ma20'] < MA20_AMOUNT:
            return False
        if stock_data['amount'] < stock_data['amount_ma20'] * MA20_AMOUNT_FACTOR:
            return False
        # ema200 > 收盘
        if stock_data['ema200'] < stock_data['close']:
            return False
        # ema200斜率 < -0.05
        if stock_data['ema200_slop'] < EMA200_SLOP:
            return False
        # EMA5上穿EMA10≥1日 && 10日内EMA5/10交叉≤1次
        if stock_data['gloden_cross_days'] < GLODEN_CROSS_DAYS:
            return False
        if stock_data['cross_count_10d'] > CROSS_COUNT_10D:
            return False
        # macd histogram > 0
        if stock_data['macd_histogram'] < MACD_HISTOGRAM:
            return False
        return True
    
    def _single_risk_filter(self, stock_data):
        risk_score = 0
        # 长期偏离EMA200
        if abs(stock_data['ema200_deviation_rate']) > EMA200_DEVIATION_RATE_HIGH:
            return False
        elif abs(stock_data['ema200_deviation_rate']) > EMA200_DEVIATION_RATE_LOW:
            risk_score += 1
        # 20日涨幅是否超过25%，60日涨幅是否超过50%
        if stock_data['20d_inc'] > INCOME_INCREASE_20D:
            risk_score += 1
        if stock_data['60d_inc'] > INCOME_INCREASE_60D:
            risk_score += 1
        # ATR/收盘价比值是否>4%
        if stock_data['atr_rate'] > ATR_RATE:
            risk_score += 1
        # 布林带%B是否连续2日>0.9
        if stock_data['bb_percent_b1'] is not None and stock_data['bb_percent_b1'] > BOLL_PCT_B and \
            stock_data['bb_percent_b1'] is not None and stock_data['bb_percent_b2'] > BOLL_PCT_B:
            risk_score += 1
        return risk_score <= RISK_SCORE_THRESHOLD

    def _single_additional_filter(self, stock_data):
        # 可选 ema50 > ema200
        if stock_data['ema50'] < stock_data['ema200']:
           return False
        return True
