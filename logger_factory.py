import logging
import sys
import datetime
from pathlib import Path

LOG_DIR_PATH = Path(__file__).resolve().parent / 'logs'
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

def get_default_logger(name : str | None) -> logging.Logger:
    """
    get logger with default setting
    """

    # Setting logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(get_default_file_handler())
    logger.addHandler(get_default_stream_handler())

    return logger

def get_custom_handlers_logger(
        name: str | None, handlers: list[logging.Handler]
        ) -> logging.Logger:
    """
    get logger with custom handler
    """

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    for handler in handlers:
        logger.addHandler(handler) 

    return logger

def get_default_file_handler(log_format : str = LOG_FORMAT) -> logging.FileHandler:
    file_formatter = logging.Formatter(log_format)
    file_handler = logging.FileHandler(
        f"{LOG_DIR_PATH}/{datetime.datetime.now().date()}.log",
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    return file_handler

def get_file_handler(log_format: str = LOG_FORMAT, log_prefix: str | None = None) -> logging.FileHandler:
    file_formatter = logging.Formatter(log_format)
    file_handler = logging.FileHandler(
        f"{LOG_DIR_PATH}/{log_prefix if log_prefix else ""}_{datetime.datetime.now().date()}.log",
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    return file_handler

def get_default_stream_handler(log_format : str = LOG_FORMAT) -> logging.StreamHandler:
    stream_formatter = logging.Formatter(log_format)
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(stream_formatter)

    return stream_handler
