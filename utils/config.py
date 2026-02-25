# -*- coding: utf-8 -*-

import os
from typing import List, Optional
from dataclasses import dataclass, field


@dataclass
class FetcherArgs:
    # === 数据源 API Token ===
    tushare_token: Optional[str] = None
    # Tushare 每分钟最大请求数（免费配额）
    tushare_rate_limit_per_minute: int = 80
    # 实时行情数据源优先级（逗号分隔）
    # 推荐顺序：tencent > akshare_sina > efinance > akshare_em > tushare
    # - tencent: 腾讯财经，有量比/换手率/市盈率等，单股查询稳定（推荐）
    # - akshare_sina: 新浪财经，基本行情稳定，但无量比
    # - efinance/akshare_em: 东财全量接口，数据最全但容易被封
    # - tushare: Tushare Pro，需要2000积分，数据全面（付费用户可优先使用）
    realtime_source_priority: str = "tencent,akshare_sina,efinance,akshare_em"
    

@dataclass
class FilterArgs:
    max_workers: int = 4
    fetcher_args: FetcherArgs = field(default_factory=FetcherArgs)
    analysis_delay: float = 0.0  # 个股分析与大盘分析之间的延迟
    max_workers: int = 3  # 低并发防封禁
    request_batch: int = 50  # 以batch方式请求，一次请求最多50只股票
