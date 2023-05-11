# treemotion/utilities/logging_config.py
import logging
import os


def configure_logger():
    log_directory = "log"
    os.makedirs(log_directory, exist_ok=True)

    log_format = "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s: %(message)s"
    date_format = "%H:%M:%S"  # "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"{log_directory}/treemotion.log")
        ],
    )


LOG_LEVELS = {
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
}
