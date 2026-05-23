"""Phase 8 hardening tests: retry decorator + logging config."""

import pytest

from src.logging_config import configure_logging, get_logger, new_run_id
from src.utils.retry import retry

pytestmark = pytest.mark.phase_8


def test_retry_succeeds_first_try():
    calls = {"n": 0}

    @retry(max_attempts=3, initial_delay=0)
    def f():
        calls["n"] += 1
        return "ok"

    assert f() == "ok"
    assert calls["n"] == 1


def test_retry_recovers_after_failures():
    calls = {"n": 0}

    @retry(max_attempts=3, initial_delay=0)
    def f():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return "ok"

    assert f() == "ok"
    assert calls["n"] == 3


def test_retry_exhausts_and_raises():
    calls = {"n": 0}

    @retry(max_attempts=3, initial_delay=0)
    def f():
        calls["n"] += 1
        raise RuntimeError("always")

    with pytest.raises(RuntimeError, match="always"):
        f()
    assert calls["n"] == 3


def test_retry_only_catches_listed_exceptions():
    @retry(max_attempts=3, initial_delay=0, exceptions=(ValueError,))
    def f():
        raise KeyError("not retried")

    with pytest.raises(KeyError):
        f()


def test_logging_config_and_run_id():
    configure_logging(json=True)
    log = get_logger("test")
    log.info("hello", k="v")  # must not raise
    rid = new_run_id()
    assert len(rid) == 12 and all(c in "0123456789abcdef" for c in rid)
