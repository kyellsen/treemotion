# treemotion/utilities/timing.py
import time
import logging

logger = logging.getLogger(__name__)

ENABLE_TIMING = True


def timing_decorator(func):
    if not ENABLE_TIMING is None:
        return func

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"[TIMING] Laufzeit von {func.__name__}: {end_time - start_time} Sekunden")
        return result

    return wrapper
