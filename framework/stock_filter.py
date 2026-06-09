# -*- coding: utf-8 -*-

import logging
import time
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import List, Dict, Any, Optional, Tuple
import os

from .data_fetch_manager import DataFetcherManager
from .notification import NotificationService
from .industry_analyzer import IndustryAnalyzer
from utils.config import FilterArgs
from utils.logger import Logger

logger = Logger(__name__)

BASE_MARKET_VALUE = 5000000000
BASE_INCOME_INCREASE = 10
INCOME_SLOWDOWN_THRESHOLD = 20

MA20_AMOUNT = 300000000
MA20_AMOUNT_FACTOR = 1.3
EMA200_SLOPE = -0.25
GOLDEN_CROSS_DAYS = 1
CROSS_COUNT_10D = 1
MACD_HISTOGRAM = 0
MIN_TURNOVER_RATE_MA5 = 1.0
MAX_TURNOVER_RATE_5D = 15.0

EMA200_DEVIATION_MODERATE = 20
EMA200_DEVIATION_EXTREME = 35
PRICE_INCREASE_20D = 25
PRICE_INCREASE_60D = 50
ATR_RATE = 4.0
BOLL_PCT_B = 0.9

RISK_SCORE_THRESHOLD = 2

SECTOR_WEAK_PERCENTILE = 0.3
MAX_STOCKS_PER_SECTOR = 3

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
        self.notifier = NotificationService(args.notifier_args)
        self.industry_analyzer = IndustryAnalyzer(self.fetcher_manager)
    
    def process(self):
        logger.info("开始筛选股票")
        self._sector_cache = {}
        self._funnel_stats = {}
        self._market_env = None
        stock_codes, code_infos = self.filter_stocks()
        logger.info(f"共找到 {len(stock_codes)} 只股票")
        code_infos = self.industry_analyzer.get_industry_infos(stock_codes, code_infos, sector_cache=self._sector_cache)
        code_infos = pd.DataFrame(code_infos)
        logger.info(f"股票信息: {code_infos.to_string()}")
        industry_analyzer_infos = []
        self.send_report(code_infos, industry_analyzer_infos)
        return

    def send_report(self, code_infos, industry_analyzer_infos):
        report = []
        code_report = self.notifier.generate_filter_report(code_infos)
        report.extend(code_report)
        self.notifier.send_filter_report(
            report,
            market_env=self._market_env,
            funnel_stats=self._funnel_stats,
        )

    def market_env_filter(self) -> bool:
        """大盘环境过滤：检查指数是否在EMA20之上"""
        if not self.args.enable_market_env_filter:
            return True

        result = self.fetcher_manager.get_index_ema_status(
            index_code=self.args.market_index_code, ema_period=20)
        if result is None:
            logger.warning("market_env_filter: 无法获取大盘数据，默认继续筛选")
            return True

        self._market_env = result
        is_above = result['is_above_ema']
        status = '站上EMA20' if is_above else '跌破EMA20'
        logger.info(f"market_env_filter: {result['index_name']} 当前 {result['current_price']:.2f}, "
                    f"EMA20 {result['ema_value']:.2f}, {status}")
        return is_above

    def filter_stocks(self) -> List[str]:
        market_ok = self.market_env_filter()
        if not market_ok:
            logger.warning("大盘环境不佳（指数在EMA20下方），本次跳过筛选")
            return [], []

        stock_codes = self.base_info_filter()
        self._funnel_stats['base_info'] = len(stock_codes)
        logger.info(f"base_info_filter return size: {len(stock_codes)}")

        if not stock_codes:
            logger.warning("base_info_filter 返回空列表，跳过后续筛选")
            return [], []

        stock_codes = self.income_filter(stock_codes)
        self._funnel_stats['income'] = len(stock_codes)
        logger.info(f"income_filter return size: {len(stock_codes)}")

        stock_codes = self.sector_filter(stock_codes)
        self._funnel_stats['sector'] = len(stock_codes)
        logger.info(f"sector_filter return size: {len(stock_codes)}")

        stock_codes, code_infos = self.history_info_filter(stock_codes)
        self._funnel_stats['technical'] = len(stock_codes)
        logger.info(f"history_info_filter return size: {len(stock_codes)}")
        return stock_codes, code_infos

    def base_info_filter(self) -> List[str]:
        df_all = self.fetcher_manager.get_all_realtime_quote()
        if df_all is not None and not df_all.empty:
            df_filtered = df_all[~df_all['name'].str.contains("ST", na=False)]
            df_filtered = df_filtered[df_filtered['total_mv'] > BASE_MARKET_VALUE]
            stock_list = df_filtered['code'].tolist()
            logger.info(f"base_info_filter: 筛选出 {len(stock_list)} 只股票（非ST，市值>{BASE_MARKET_VALUE/1e8:.0f}亿）")
            return stock_list

        logger.warning("base_info_filter: 实时行情不可用，使用股票列表兜底（跳过市值筛选）")
        df_list = self.fetcher_manager.get_all_stock_list()
        if df_list is None or df_list.empty:
            logger.error("base_info_filter: 股票列表也无法获取，返回空列表")
            return []
        df_filtered = df_list[~df_list['name'].str.contains("ST", na=False)]
        stock_list = df_filtered['code'].tolist()
        logger.info(f"base_info_filter: 兜底筛选出 {len(stock_list)} 只（非ST，无市值筛选）")
        return stock_list

    def income_filter(self, stock_list: List[str]) -> List[str]:
        from .data_fetch_manager import DataFetcherManager

        current_quarter = DataFetcherManager._get_latest_quarter_date()
        df_income = self.fetcher_manager.get_batch_income_data(current_quarter)
        if df_income is None or df_income.empty:
            logger.warning("income_filter: 无法获取批量业绩数据，跳过收入筛选，返回原列表")
            return stock_list

        df_income['code'] = df_income['code'].astype(str)
        df_in_list = df_income[df_income['code'].isin(stock_list)].copy()
        df_in_list['income_inc'] = pd.to_numeric(df_in_list['income_inc'], errors='coerce')
        df_filtered = df_in_list[df_in_list['income_inc'] >= BASE_INCOME_INCREASE]

        prev_quarter = DataFetcherManager._get_previous_quarter_date(current_quarter)
        df_prev = self.fetcher_manager.get_batch_income_data(prev_quarter)
        if df_prev is not None and not df_prev.empty:
            df_prev['code'] = df_prev['code'].astype(str)
            df_prev['prev_income_inc'] = pd.to_numeric(df_prev['income_inc'], errors='coerce')
            df_merged = df_filtered.merge(df_prev[['code', 'prev_income_inc']], on='code', how='left')
            slowdown = df_merged['prev_income_inc'] - df_merged['income_inc']
            df_slowdown = df_merged[slowdown >= INCOME_SLOWDOWN_THRESHOLD]
            if len(df_slowdown) > 0:
                logger.info(f"income_filter: 排除 {len(df_slowdown)} 只增速大幅放缓股票（上季增速-本季增速>={INCOME_SLOWDOWN_THRESHOLD}%）")
            df_filtered = df_merged[~(slowdown >= INCOME_SLOWDOWN_THRESHOLD)]
        else:
            logger.warning("income_filter: 无法获取上季度业绩数据，跳过增速趋势判断")

        result = df_filtered['code'].tolist()
        logger.info(f"income_filter: {len(stock_list)} -> {len(result)} 只股票（收入增速>={BASE_INCOME_INCREASE}%，排除增速放缓）")
        return result

    def sector_filter(self, stock_list: List[str]) -> List[str]:
        mapping = self.fetcher_manager.get_sector_stock_mapping()
        if mapping is None:
            logger.warning("sector_filter: 板块数据不可用，跳过板块筛选")
            return stock_list

        sectors = mapping['sectors']
        stock_to_sector = mapping['stock_to_sector']

        sorted_sectors = sorted(sectors, key=lambda s: s['change_pct'])
        weak_count = max(1, int(len(sorted_sectors) * SECTOR_WEAK_PERCENTILE))
        weak_threshold = sorted_sectors[weak_count - 1]['change_pct']
        weak_sectors = {s['name'] for s in sorted_sectors[:weak_count]}

        logger.info(f"sector_filter: 弱势板块阈值(5日) {weak_threshold:.2f}%，共 {len(weak_sectors)} 个: {weak_sectors}")

        sector_count = {}
        result = []
        excluded_weak = 0
        excluded_cap = 0

        for code in stock_list:
            sector_name = stock_to_sector.get(code)
            if sector_name is None:
                result.append(code)
                continue

            if not hasattr(self, '_sector_cache'):
                self._sector_cache = {}
            self._sector_cache[code] = sector_name

            if sector_name in weak_sectors:
                excluded_weak += 1
                continue

            count = sector_count.get(sector_name, 0)
            if count >= MAX_STOCKS_PER_SECTOR:
                excluded_cap += 1
                continue

            sector_count[sector_name] = count + 1
            result.append(code)

        logger.info(f"sector_filter: {len(stock_list)} -> {len(result)} 只（排除弱势板块 {excluded_weak}，板块集中度 {excluded_cap}）")
        return result

    def history_info_filter(self, stock_list:List[str], report_type='short') -> List[str]:
        logger.info(f"开始处理{len(stock_list)}只股票: {stock_list}")
        filted_stocks = []
        filtered_infos = []
        batch = self.args.request_batch
        total_iter = (len(stock_list) - 1) // batch + 1
        for step in range(total_iter):
            start = batch * step
            end = min(batch * (step + 1), len(stock_list))
            batch_stock_list = stock_list[start:end]
            with ThreadPoolExecutor(max_workers=self.args.max_workers) as executor:
                # 提交任务
                future_to_code = {
                    executor.submit(
                        self._history_info_filter,
                        code,
                        report_type=report_type
                    ): code
                    for code in batch_stock_list
                }
                
                # 收集结果
                for idx, future in enumerate(as_completed(future_to_code)):
                    code = future_to_code[future]
                    try:
                        result = future.result()
                        if result and result[0]:
                            filted_stocks.append(code)
                            filtered_infos.append(result[1])

                        # Issue #128: 分析间隔 - 在个股分析和大盘分析之间添加延迟
                        if idx < len(batch_stock_list) - 1 and self.args.analysis_delay > 0:
                            logger.debug(f"等待 {self.args.analysis_delay} 秒后继续下一只股票...")
                            time.sleep(self.args.analysis_delay)

                    except Exception as e:
                        logger.error(f"[{code}] 任务执行失败: {e}")
            if not step == total_iter - 1:
                logger.info(f"处理完成 {batch * (step + 1)} 只股票，剩余{len(stock_list) - batch * (step + 1)}只股票")
                time.sleep(random.uniform(1, 2) * 60)
        logger.info(f"处理完成，剩余{len(filted_stocks)}只股票: {filted_stocks}")
        #filtered_infos = pd.DataFrame(filtered_infos)
        return filted_stocks, filtered_infos

    def _history_info_filter(self, stock_code: str, report_type='short'):
        analyzed_data = {"code": stock_code}
        stock_data = self.fetcher_manager.get_daily_analyzed_data(stock_code, end_date=self.args.analyze_date)
        if stock_data is None:
            return False, analyzed_data
        trend_result = self._single_trend_filter(stock_data, analyzed_data)
        risk_result = self._single_risk_filter(stock_data, analyzed_data)
        additional_result = self._single_additional_filter(stock_data, analyzed_data)
        if not report_type == 'short':
            analyzed_data['origin_data'] = stock_data
        logger.info(f"history info analyze info: {analyzed_data}")
        return trend_result and risk_result and additional_result, analyzed_data

    def _single_trend_filter(self, stock_data, analyzed_data):
        result = True
        stock_data['20日成交额>3亿'] = (stock_data['amount_ma20'] >= MA20_AMOUNT)
        stock_data['近5日有放量'] = stock_data['has_volume_expansion_5d']
        result = result and stock_data['20日成交额>3亿'] and stock_data['近5日有放量']
        if stock_data.get('turnover_rate_ma5') is not None:
            stock_data['5日均换手率>=1%'] = (stock_data['turnover_rate_ma5'] >= MIN_TURNOVER_RATE_MA5)
            result = result and stock_data['5日均换手率>=1%']
        stock_data['收盘>ema200'] = (stock_data['close'] > stock_data['ema200'])
        result = result and stock_data['收盘>ema200']
        stock_data['ema200斜率>=-0.25'] = (stock_data['ema200_slope'] >= EMA200_SLOPE)
        result = result and stock_data['ema200斜率>=-0.25']
        stock_data['EMA5上穿EMA10≥1日'] = (stock_data['golden_cross_days'] >= GOLDEN_CROSS_DAYS)
        stock_data['10日内EMA5/10交叉≤1次'] = (stock_data['cross_count_10d'] <= CROSS_COUNT_10D)
        result = result and stock_data['EMA5上穿EMA10≥1日'] and stock_data['10日内EMA5/10交叉≤1次']
        stock_data['MACD Histogram≥0'] = (stock_data['macd_histogram'] >= MACD_HISTOGRAM)
        result = result and stock_data['MACD Histogram≥0']
        analyzed_data['回踩支撑位'] = stock_data.get('near_support', False)
        return result
    
    def _single_risk_filter(self, stock_data, analyzed_data):
        risk_score = 0
        dev = abs(stock_data['ema200_deviation_rate'])
        if dev > EMA200_DEVIATION_EXTREME:
            analyzed_data['偏离EMA200'] = f'{dev:.1f}%(极端)'
            risk_score += 2
        elif dev > EMA200_DEVIATION_MODERATE:
            analyzed_data['偏离EMA200'] = f'{dev:.1f}%(偏高)'
            risk_score += 1
        else:
            analyzed_data['偏离EMA200'] = f'{dev:.1f}%'
        analyzed_data['20日涨幅>25%'] = (stock_data['20d_inc'] > PRICE_INCREASE_20D)
        analyzed_data['60日涨幅>50%'] = (stock_data['60d_inc'] > PRICE_INCREASE_60D)
        risk_score = risk_score + 1 if analyzed_data['20日涨幅>25%'] else risk_score
        risk_score = risk_score + 1 if analyzed_data['60日涨幅>50%'] else risk_score
        analyzed_data['ATR/收盘价比值>4%'] = (stock_data['atr_rate'] > ATR_RATE)
        risk_score = risk_score + 1 if analyzed_data['ATR/收盘价比值>4%'] else risk_score
        analyzed_data['布林带%B连续2日>0.9'] = (stock_data['bb_percent_b1'] is not None and stock_data['bb_percent_b1'] > BOLL_PCT_B and \
            stock_data['bb_percent_b2'] is not None and stock_data['bb_percent_b2'] > BOLL_PCT_B)
        risk_score = risk_score + 1 if analyzed_data['布林带%B连续2日>0.9'] else risk_score
        analyzed_data['量价背离'] = stock_data.get('volume_price_divergence', False)
        risk_score = risk_score + 1 if analyzed_data['量价背离'] else risk_score
        tr_max5 = stock_data.get('turnover_rate_max5')
        analyzed_data['5日换手率过高'] = tr_max5 is not None and tr_max5 > MAX_TURNOVER_RATE_5D
        risk_score = risk_score + 1 if analyzed_data['5日换手率过高'] else risk_score
        analyzed_data['风险分'] = risk_score
        return risk_score <= RISK_SCORE_THRESHOLD

    def _single_additional_filter(self, stock_data, analyzed_data):
        # 可选 ema50 > ema120
        result = True
        stock_data['ema50>=ema200'] = (stock_data['ema50'] >= stock_data['ema200'])
        result = result and stock_data['ema50>=ema200']
        return result
