"""Central logging setup."""
from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.utils.config_loader import config


def setup_logger(name: str = "scalping_bot") -> logging.Logger:
    """Setup a configured logger with console + rotating file handlers."""
    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured
        return logger

    console_level = getattr(
        logging, config.get("logging.console_level", "INFO").upper(), logging.INFO
    )
    file_level = getattr(
        logging, config.get("logging.file_level", "DEBUG").upper(), logging.DEBUG
    )

    logger.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (rotating, 10MB, 5 backups)
    log_file = config.project_root / config.get("logging.system_log_file", "logs/system.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(file_level)
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(funcName)s:%(lineno)d | %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "scalping_bot") -> logging.Logger:
    """Get an already-configured logger (or set up a new one)."""
    return setup_logger(name)
