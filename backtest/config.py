# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from typing import List, Optional
from utils.config import FetcherArgs


@dataclass
class BacktestArgs:
    fetcher_args: FetcherArgs = field(default_factory=FetcherArgs)
    stock_codes: List[str] = field(default_factory=list)
    start_date: str = "2024-01-01"
    end_date: Optional[str] = None
    warmup_days: int = 250
    forward_return_days: List[int] = field(default_factory=lambda: [5, 10, 20])
    min_hold_days: int = 0
    max_workers: int = 3
    output_dir: str = "backtest_results"
    enable_chart: bool = True
