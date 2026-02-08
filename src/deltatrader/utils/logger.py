"""Centralized logging configuration."""

import logging
import sys

from .config import Config


def setup_logger(name: str = "crypt") -> logging.Logger:
    """Set up and return a configured logger."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))

    return logger


logger = setup_logger()
