import logging
import os
import sys


class SingleLevelFilter(logging.Filter):
    def __init__(self, passlevel, reject):
        self.passlevel = passlevel
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return record.levelno != self.passlevel
        else:
            return record.levelno == self.passlevel


class Logger:
    def __init__(self, name=__file__, level=logging.INFO):
        stdout_handler = logging.StreamHandler(sys.stdout)
        stderr_handler = logging.StreamHandler(sys.stderr)
        stdout_handler.addFilter(SingleLevelFilter(logging.INFO, False))
        stderr_handler.addFilter(SingleLevelFilter(logging.INFO, True))
        stderr_handler.setLevel(level)
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
        )
        logger = logging.getLogger(name)
        logger.setLevel(level)
        stdout_handler.setFormatter(formatter)
        stderr_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)
        logger.addHandler(stderr_handler)
        logger.propagate = False
        self._logger = logger

    def info(self, *args, **kwargs):
        self._logger.info(*args, **kwargs)

    def warning(self, *args, **kwargs):
        self._logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        self._logger.error(*args, **kwargs)

    def debug(self, *args, **kwargs):
        self._logger.debug(*args, **kwargs)