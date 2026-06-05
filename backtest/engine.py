# -*- coding: utf-8 -*-

import time
import random
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from framework.data_fetch_manager import DataFetcherManager
from utils.cache import save_cache, load_cache
from utils.logger import Logger
from .config import BacktestArgs
from .indicators import compute_all_indicators, compute_filter_masks

logger = Logger(__name__)


@dataclass
class SingleStockResult:
    stock_code: str
    signal_count: int
    signals_df: pd.DataFrame


@dataclass
class BacktestResult:
    args: BacktestArgs
    stock_results: List[SingleStockResult]
    total_signals: int
    metrics: Dict[str, Any]
    summary_df: pd.DataFrame


class BacktestEngine:
    def __init__(self, args: BacktestArgs):
        self.args = args
        self.fetcher_manager = DataFetcherManager(args.fetcher_args)

        if args.end_date is None:
            self.end_date = datetime.now().strftime('%Y-%m-%d')
        else:
            self.end_date = args.end_date

        dt_start = datetime.strptime(args.start_date, '%Y-%m-%d')
        fetch_dt = dt_start - timedelta(days=int(args.warmup_days * 1.5))
        self.fetch_start = fetch_dt.strftime('%Y-%m-%d')

    def run(self) -> BacktestResult:
        logger.info(f"开始回测: {self.args.start_date} ~ {self.end_date}, "
                     f"共 {len(self.args.stock_codes)} 只股票")

        all_results = []
        codes = self.args.stock_codes
        batch_size = 50

        for i in range(0, len(codes), batch_size):
            batch = codes[i:i + batch_size]
            with ThreadPoolExecutor(max_workers=self.args.max_workers) as executor:
                futures = {
                    executor.submit(self._backtest_single, code): code
                    for code in batch
                }
                for future in as_completed(futures):
                    code = futures[future]
                    try:
                        result = future.result()
                        if result is not None:
                            all_results.append(result)
                            logger.info(f"[{code}] 回测完成: {result.signal_count} 个信号")
                    except Exception as e:
                        logger.error(f"[{code}] 回测失败: {e}")

            if i + batch_size < len(codes):
                sleep_time = random.uniform(30, 60)
                logger.info(f"批次完成 ({i + batch_size}/{len(codes)})，等待 {sleep_time:.0f}s")
                time.sleep(sleep_time)

        result = self._aggregate_results(all_results)
        logger.info(f"回测完成: 共 {result.total_signals} 个买入信号, "
                     f"涉及 {len(result.stock_results)} 只股票")
        return result

    def _fetch_data(self, stock_code: str) -> Optional[pd.DataFrame]:
        cache_name = f"bt_{stock_code}_{self.fetch_start}_{self.end_date}"
        cached = load_cache(cache_name, max_age_hours=72.0)
        if cached is not None:
            logger.debug(f"[{stock_code}] 命中回测数据缓存")
            return cached

        try:
            df, source = self.fetcher_manager.get_daily_data(
                stock_code,
                start_date=self.fetch_start,
                end_date=self.end_date,
                days=1000,
            )
            if df is not None and not df.empty:
                save_cache(cache_name, df)
                return df
        except Exception as e:
            logger.warning(f"[{stock_code}] 数据获取失败: {e}")
        return None

    def _backtest_single(self, stock_code: str) -> Optional[SingleStockResult]:
        df = self._fetch_data(stock_code)
        if df is None:
            return None

        min_rows = self.args.warmup_days + 20
        if len(df) < min_rows:
            logger.warning(f"[{stock_code}] 数据不足 ({len(df)} < {min_rows})，跳过")
            return None

        df = compute_all_indicators(df)
        df = compute_filter_masks(df)

        for n in self.args.forward_return_days:
            df[f'forward_{n}d'] = (df['close'].shift(-n) / df['close'] - 1) * 100

        backtest_start = pd.to_datetime(self.args.start_date)
        df_bt = df[df['date'] >= backtest_start].copy()

        signals = df_bt[df_bt['buy_signal']].copy()

        if self.args.min_hold_days > 0 and not signals.empty:
            signals = self._apply_min_hold_filter(signals)

        forward_cols = [f'forward_{n}d' for n in self.args.forward_return_days]
        keep_cols = ['date', 'close', 'risk_score'] + forward_cols
        existing = [c for c in keep_cols if c in signals.columns]

        return SingleStockResult(
            stock_code=stock_code,
            signal_count=len(signals),
            signals_df=signals[existing].reset_index(drop=True),
        )

    def _apply_min_hold_filter(self, signals: pd.DataFrame) -> pd.DataFrame:
        keep = []
        last_idx = -self.args.min_hold_days - 1
        for idx in signals.index:
            if idx - last_idx >= self.args.min_hold_days:
                keep.append(idx)
                last_idx = idx
        return signals.loc[keep]

    def _aggregate_results(self, results: List[SingleStockResult]) -> BacktestResult:
        if not results:
            return BacktestResult(
                args=self.args, stock_results=[], total_signals=0,
                metrics={}, summary_df=pd.DataFrame(),
            )

        all_signals = []
        for r in results:
            s = r.signals_df.copy()
            s['stock_code'] = r.stock_code
            all_signals.append(s)
        summary_df = pd.concat(all_signals, ignore_index=True)

        metrics = self._compute_metrics(summary_df)

        return BacktestResult(
            args=self.args,
            stock_results=results,
            total_signals=len(summary_df),
            metrics=metrics,
            summary_df=summary_df,
        )

    def _compute_metrics(self, summary_df: pd.DataFrame) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        metrics['total_signals'] = len(summary_df)
        metrics['unique_stocks'] = summary_df['stock_code'].nunique() if 'stock_code' in summary_df.columns else 0

        if 'date' in summary_df.columns and len(summary_df) > 0:
            date_range = (summary_df['date'].max() - summary_df['date'].min()).days
            months = max(date_range / 30.0, 1)
            metrics['avg_signals_per_month'] = round(len(summary_df) / months, 1)

        for n in self.args.forward_return_days:
            col = f'forward_{n}d'
            if col not in summary_df.columns:
                continue
            returns = summary_df[col].dropna()
            if len(returns) == 0:
                continue

            prefix = f'{n}d'
            metrics[f'win_rate_{prefix}'] = round((returns > 0).mean() * 100, 2)
            metrics[f'avg_return_{prefix}'] = round(returns.mean(), 2)
            metrics[f'median_return_{prefix}'] = round(returns.median(), 2)
            metrics[f'max_return_{prefix}'] = round(returns.max(), 2)
            metrics[f'min_return_{prefix}'] = round(returns.min(), 2)
            metrics[f'std_return_{prefix}'] = round(returns.std(), 2)

            if returns.std() > 0:
                metrics[f'sharpe_{prefix}'] = round(
                    returns.mean() / returns.std() * np.sqrt(252 / n), 2
                )
            else:
                metrics[f'sharpe_{prefix}'] = 0.0

            cum_ret, max_dd = self._compute_cumulative_metrics(returns)
            metrics[f'cumulative_return_{prefix}'] = round(cum_ret, 2)
            metrics[f'max_drawdown_{prefix}'] = round(max_dd, 2)

            pos = returns[returns > 0].sum()
            neg = abs(returns[returns < 0].sum())
            metrics[f'profit_factor_{prefix}'] = round(pos / neg, 2) if neg > 0 else float('inf')

            losses = (returns <= 0).astype(int)
            if len(losses) > 0:
                streaks = losses.groupby((losses != losses.shift()).cumsum())
                max_loss_streak = max(
                    (g.sum() for _, g in streaks if g.iloc[0] == 1), default=0
                )
                metrics[f'max_consecutive_loss_{prefix}'] = int(max_loss_streak)

        return metrics

    @staticmethod
    def _compute_cumulative_metrics(returns: pd.Series):
        r = returns / 100
        cumulative = (1 + r).cumprod()
        peak = cumulative.expanding().max()
        drawdown = (cumulative - peak) / peak
        max_dd = drawdown.min() * 100
        cum_ret = (cumulative.iloc[-1] - 1) * 100 if len(cumulative) > 0 else 0
        return cum_ret, max_dd
