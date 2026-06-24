"""Cross-cutting decorators (retry, timing).

Migrated verbatim from ``mirror_url.py`` (orig. lines 660-738).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from functools import wraps
from typing import Tuple, Type, Union


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on: Union[Type[BaseException], Tuple[Type[BaseException], ...]] = Exception,
    log_retries: bool = True,
):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (NOT including the first attempt)
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        retry_on: Exception type or tuple of types to retry on
        log_retries: Whether to log retry attempts
    """
    # Normalize retry_on to a tuple for isinstance check
    retry_types = retry_on if isinstance(retry_on, tuple) else (retry_on,)

    def decorator(func):
        is_async = inspect.iscoroutinefunction(func)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_types as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2**attempt), max_delay)
                        if log_retries:
                            logging.debug(
                                f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                                f"after {delay:.2f}s: {type(e).__name__}: {e}"
                            )
                        time.sleep(delay)
                    # Continue to next iteration (or fall through to raise)
            raise last_exception

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retry_types as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2**attempt), max_delay)
                        if log_retries:
                            logging.debug(
                                f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                                f"after {delay:.2f}s: {type(e).__name__}: {e}"
                            )
                        await asyncio.sleep(delay)
            raise last_exception

        return async_wrapper if is_async else sync_wrapper

    return decorator


def log_performance(operation_name: str):
    """Decorator to log performance metrics"""

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start = time.time()
            try:
                result = func(self, *args, **kwargs)
                duration = time.time() - start
                if hasattr(self, "performance_monitor"):
                    self.performance_monitor.record(operation_name, duration, True)
                return result
            except Exception:
                duration = time.time() - start
                if hasattr(self, "performance_monitor"):
                    self.performance_monitor.record(operation_name, duration, False)
                raise

        return wrapper

    return decorator


__all__ = ["retry_with_backoff", "log_performance"]
