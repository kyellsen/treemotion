# timing.py
import time
from functools import wraps

ENABLE_TIMING = True

def timing_decorator(func):
    if not ENABLE_TIMING is None:
        return func

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        duration = end_time - start_time
        print(f"[TIMING] {func.__module__}.{func.__qualname__}: {duration:.4f} sec")

        return result

    return wrapper
