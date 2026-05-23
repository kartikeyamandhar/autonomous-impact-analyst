"""Exponential-backoff retry decorator.

Applied to flaky external calls — Databricks/Neo4j connects (SQL warehouse can
be cold), Claude API (transient 429/529), Slack sends. Deterministic logic is
never wrapped; only network boundaries.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    initial_delay: float = 1.0,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Retry a function with exponential backoff.

    Sleeps initial_delay * backoff_factor**attempt between tries. Re-raises the
    last exception if all attempts fail.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exc: BaseException | None = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_attempts - 1:
                        delay = initial_delay * (backoff_factor ** attempt)
                        logger.warning(
                            "%s failed (attempt %d/%d): %s; retrying in %.1fs",
                            func.__name__, attempt + 1, max_attempts, e, delay,
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__, max_attempts, e,
                        )
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator
