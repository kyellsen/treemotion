# treemotion/utils/runtime.py
import time
import functools
from utils.log import get_logger

ENABLE_TIMING = True


def dec_runtime(func):
    if not ENABLE_TIMING:
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        logger = get_logger(__name__)
        class_name = ""
        if len(args) > 0 and hasattr(args[0], "__class__"):
            class_name = args[0].__class__.__name__ + "."
        logger.debug(f"[Runtime] ----------------- {run_time:.4f} secs, {class_name}{func.__name__} ----------------- [Runtime] ")
        return result

    return wrapper
