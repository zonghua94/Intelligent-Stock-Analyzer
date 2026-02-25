from framework.stock_filter import StockFilter
from utils.logger import Logger

logger = Logger(__name__)

sf = StockFilter()
filtered_stock_codes = sf.filter_stocks()
logger.info(filtered_stock_codes)