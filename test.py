import os

from framework.stock_filter import StockFilter
from utils.config import FilterArgs, NotificationArgs
from utils.logger import Logger

logger = Logger(__name__)

notifier_args = NotificationArgs(serverchan3_sendkey=os.getenv("SERVERCHAN3_SENDKEY"))
filter_args = FilterArgs(notifier_args=notifier_args)
sf = StockFilter(filter_args)
filtered_stock_codes = sf.process()
#logger.info(filtered_stock_codes)