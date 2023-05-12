# treemotion/utilities/log.py
import logging
from datetime import datetime
from pathlib import Path

from treemotion import configuration


class ColorfulFormatter(logging.Formatter):
    COLOR_CODES = {
        logging.CRITICAL: '\033[91m',  # Red
        logging.ERROR: '\033[91m',     # Red
        logging.WARNING: '\033[93m',   # Yellow
        logging.INFO: '\033[92m',      # Green
        logging.DEBUG: '\033[94m',     # Blue
    }

    RESET_CODE = '\033[0m'

    def format(self, record):
        color_code = self.COLOR_CODES.get(record.levelno, self.RESET_CODE)
        record.levelname = f"{color_code}{record.levelname}{self.RESET_CODE}"
        return super().format(record)


def configure_logger(log_level=configuration.log_level, log_directory=configuration.log_directory, log_format=None, date_format=None, log_file=None):
    log_level = LOG_LEVELS.get(log_level.lower(), logging.INFO)
    log_directory = Path(log_directory)
    log_directory.mkdir(exist_ok=True)

    if log_format is None:
        log_format = "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s: %(message)s"
    if date_format is None:
        date_format = "%Y-%m-%d %H:%M:%S"
    if log_file is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = log_directory / f"treemotion_log_{timestamp}.txt"

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorfulFormatter(log_format, datefmt=date_format))

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))

    handlers = [console_handler, file_handler]

    logging.basicConfig(
        level=log_level,
        handlers=handlers,
    )
    return



def get_logger(name):
    return logging.getLogger(name)

LOG_LEVELS = {
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
}
