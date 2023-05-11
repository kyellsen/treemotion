import logging
from datetime import datetime
from pathlib import Path

class ColorfulConsoleHandler(logging.StreamHandler):
    COLOR_CODES = {
        logging.CRITICAL: '\033[91m',  # Red
        logging.ERROR: '\033[91m',     # Red
        logging.WARNING: '\033[93m',   # Yellow
        logging.INFO: '\033[92m',      # Green
        logging.DEBUG: '\033[94m',     # Blue
    }

    RESET_CODE = '\033[0m'

    def emit(self, record):
        color_code = self.COLOR_CODES.get(record.levelno, self.RESET_CODE)
        record.msg = f"{color_code}{record.msg}{self.RESET_CODE}"
        super().emit(record)

def configure_logger(log_level=logging.DEBUG, log_directory="log", log_format=None, date_format=None, log_file=None):
    log_directory = Path(log_directory)
    log_directory.mkdir(exist_ok=True)

    if log_format is None:
        log_format = "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s: %(message)s"
    if date_format is None:
        date_format = "%Y-%m-%d %H:%M:%S"
    if log_file is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = log_directory / f"treemotion_log_{timestamp}.txt"

    handlers = [
        ColorfulConsoleHandler(),
        logging.FileHandler(log_file)
    ]

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
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
