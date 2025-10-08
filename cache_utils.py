import functools
import time
from flask import request, make_response, jsonify

def cache_with_timeout(timeout_seconds):
    def decorator(func):
        cache = {} 
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache_key_args = tuple(sorted(request.args.items()))
            cache_key_view_args = tuple(sorted(request.view_args.items()))
            cache_key = (cache_key_args, cache_key_view_args)

            current_time = time.time()

            if cache_key in cache:
                cached_time, cached_result = cache[cache_key]
                if (current_time - cached_time) < timeout_seconds:
                    return cached_result
            result = func(*args, **kwargs)
            cache[cache_key] = (current_time, result)
            return result
        return wrapper
    return decorator
    