# timing.py
import time
from functools import wraps

ENABLE_TIMING = True

def timing_decorator(func):
    if not ENABLE_TIMING:
        return func

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        duration = end_time - start_time
        class_name = ""
        if len(args) > 0 and hasattr(args[0], "__class__"):
            class_name = args[0].__class__.__name__ + "."
        print(f"{duration:.4f} sec: {class_name}{func.__name__}")
        return result
    return wrapper
