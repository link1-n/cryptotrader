"""Centralized logging configuration."""

import logging
import sys
from datetime import datetime

from .config import Config


class MicrosecondFormatter(logging.Formatter):
    """Custom formatter that includes microsecond precision in timestamps.

    Example output:
        2024-01-15 14:23:45.123456 - crypt - INFO - [logger.py:42:setup_logger] - Message here
    """

    def formatTime(self, record, datefmt=None):
        """Override formatTime to include microseconds."""
        ct = datetime.fromtimestamp(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime("%Y-%m-%d %H:%M:%S")
        # Add microseconds
        s = f"{s}.{int(record.created % 1 * 1_000_000):06d}"
        return s


def setup_logger(name: str = "crypt") -> logging.Logger:
    """Set up and return a configured logger."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = MicrosecondFormatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d:%(funcName)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))

    return logger


logger = setup_logger()
