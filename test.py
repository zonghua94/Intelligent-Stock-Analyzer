import os
import time
from datetime import datetime

from framework.stock_filter import StockFilter
from utils.config import FilterArgs, NotificationArgs
from utils.logger import Logger

logger = Logger(__name__)

analyze_date = os.getenv("ANALYZE_DATE")
if not analyze_date or len(analyze_date) == 0:
    analyze_date = datetime.now().strftime('%Y-%m-%d')
notifier_args = NotificationArgs(serverchan3_sendkey=os.getenv("SERVERCHAN3_SENDKEY"))
filter_args = FilterArgs(notifier_args=notifier_args, analyze_date=analyze_date)
sf = StockFilter(filter_args)
filtered_stock_codes = sf.process()
#logger.info(filtered_stock_codes)