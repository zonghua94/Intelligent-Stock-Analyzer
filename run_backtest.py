# -*- coding: utf-8 -*-
"""
回测入口脚本

环境变量:
  BT_START_DATE  回测起始日期 (默认 2024-01-01)
  BT_END_DATE    回测结束日期 (默认 今天)
  BT_STOCK_FILE  股票列表文件路径 (每行一个代码)
"""

import os
from datetime import datetime
from backtest.config import BacktestArgs
from backtest.engine import BacktestEngine
from backtest.report import BacktestReporter
from utils.config import FetcherArgs
from utils.logger import Logger

logger = Logger(__name__)

start_date = os.getenv("BT_START_DATE", "2024-01-01")
end_date = os.getenv("BT_END_DATE", datetime.now().strftime('%Y-%m-%d'))

stock_codes = [
    "600519", "000858", "601318", "600036",
    "000333", "600276", "601166", "000001",
    "600900", "000651", "601888", "300750",
    "002475", "600031", "000568", "601012",
    "603259", "300059", "002049", "600809",
]

stock_file = os.getenv("BT_STOCK_FILE")
if stock_file and os.path.exists(stock_file):
    with open(stock_file, 'r') as f:
        stock_codes = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    logger.info(f"从文件加载 {len(stock_codes)} 只股票: {stock_file}")

backtest_args = BacktestArgs(
    stock_codes=stock_codes,
    start_date=start_date,
    end_date=end_date,
    forward_return_days=[5, 10, 20],
    min_hold_days=5,
    enable_chart=True,
)

logger.info(f"回测参数: {start_date} ~ {end_date}, {len(stock_codes)} 只股票")
engine = BacktestEngine(backtest_args)
result = engine.run()

reporter = BacktestReporter(result, backtest_args)
reporter.save_report()
logger.info(f"回测完成，报告已保存至 {backtest_args.output_dir}/")
