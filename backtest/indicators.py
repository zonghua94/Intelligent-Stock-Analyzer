# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from ta.trend import MACD
from ta.volatility import BollingerBands, AverageTrueRange

from framework.stock_filter import (
    MA20_AMOUNT, MA20_AMOUNT_FACTOR, EMA200_SLOPE, GOLDEN_CROSS_DAYS,
    CROSS_COUNT_10D, MACD_HISTOGRAM, MIN_TURNOVER_RATE_MA5,
    EMA200_DEVIATION_MODERATE, EMA200_DEVIATION_EXTREME,
    PRICE_INCREASE_20D, PRICE_INCREASE_60D, ATR_RATE, BOLL_PCT_B,
    RISK_SCORE_THRESHOLD,
)


def compute_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values("date").reset_index(drop=True)

    df['amount_ma20'] = df['amount'].rolling(window=20).mean()
    volume_expansion = df['amount'] >= df['amount_ma20'] * MA20_AMOUNT_FACTOR
    df['has_volume_expansion_5d'] = volume_expansion.rolling(window=5).max().fillna(0).astype(bool)

    if 'turnover_rate' in df.columns and df['turnover_rate'].notna().any():
        df['turnover_rate_ma5'] = df['turnover_rate'].rolling(window=5).mean()
        df['has_turnover'] = True
    else:
        df['turnover_rate_ma5'] = np.nan
        df['has_turnover'] = False

    df['ema5'] = df['close'].ewm(span=5, adjust=False).mean()
    df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    df['ema200_slope'] = (df['ema200'] - df['ema200'].shift(5)) / df['ema200'].shift(5) * 100

    is_golden_cross = (df['ema5'] > df['ema10']) & (df['ema5'].shift(1) <= df['ema10'].shift(1))
    is_death_cross = (df['ema5'] < df['ema10']) & (df['ema5'].shift(1) >= df['ema10'].shift(1))

    row_idx = pd.Series(range(len(df)), index=df.index, dtype=float)
    gc_pos = row_idx.where(is_golden_cross).ffill()
    df['golden_cross_days'] = (row_idx - gc_pos).fillna(-1).astype(int)

    is_cross = (is_golden_cross | is_death_cross).astype(int)
    df['cross_count_10d'] = is_cross.rolling(window=10, min_periods=1).sum().astype(int)

    macd_calc = MACD(close=df['close'], window_fast=12, window_slow=26, window_sign=9)
    df['macd_histogram'] = macd_calc.macd_diff()

    df['ema200_deviation_rate'] = (df['ema200'] - df['close']) / df['ema200'] * 100

    df['price_inc_20d'] = (df['close'] - df['close'].shift(20)) / df['close'].shift(20) * 100
    df['price_inc_60d'] = (df['close'] - df['close'].shift(60)) / df['close'].shift(60) * 100

    atr_calc = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14)
    df['atr_rate'] = atr_calc.average_true_range() / df['close'] * 100

    bb = BollingerBands(close=df['close'], window=20, window_dev=2)
    bb_high = bb.bollinger_hband()
    bb_low = bb.bollinger_lband()
    bb_width = bb_high - bb_low
    df['bb_percent_b'] = (df['close'] - bb_low) / bb_width.replace(0, np.nan)
    df['bb_pct_b_prev'] = df['bb_percent_b'].shift(1)

    return df


def compute_filter_masks(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    trend_pass = (
        (df['amount_ma20'] >= MA20_AMOUNT) &
        (df['has_volume_expansion_5d']) &
        (df['close'] > df['ema200']) &
        (df['ema200_slope'] >= EMA200_SLOPE) &
        (df['golden_cross_days'] >= GOLDEN_CROSS_DAYS) &
        (df['cross_count_10d'] <= CROSS_COUNT_10D) &
        (df['macd_histogram'] >= MACD_HISTOGRAM)
    )

    if df['has_turnover'].any():
        trend_pass = trend_pass & (df['turnover_rate_ma5'] >= MIN_TURNOVER_RATE_MA5)

    dev = df['ema200_deviation_rate'].abs()
    risk_score = pd.Series(0, index=df.index)
    risk_score = risk_score + np.where(dev > EMA200_DEVIATION_EXTREME, 2,
                             np.where(dev > EMA200_DEVIATION_MODERATE, 1, 0))
    risk_score = risk_score + (df['price_inc_20d'] > PRICE_INCREASE_20D).astype(int)
    risk_score = risk_score + (df['price_inc_60d'] > PRICE_INCREASE_60D).astype(int)
    risk_score = risk_score + (df['atr_rate'] > ATR_RATE).astype(int)
    risk_score = risk_score + (
        (df['bb_percent_b'] > BOLL_PCT_B) & (df['bb_pct_b_prev'] > BOLL_PCT_B)
    ).astype(int)

    df['risk_score'] = risk_score
    risk_pass = risk_score <= RISK_SCORE_THRESHOLD

    additional_pass = df['ema50'] >= df['ema200']

    df['trend_pass'] = trend_pass
    df['risk_pass'] = risk_pass
    df['additional_pass'] = additional_pass
    df['buy_signal'] = trend_pass & risk_pass & additional_pass

    return df
