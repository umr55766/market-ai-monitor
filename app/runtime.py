"""
Small runtime helpers shared by entrypoints/workers.

Keep this module dependency-light; it should be safe to import from any worker.
"""
 
from __future__ import annotations

import time
from typing import Callable, Optional


def wait_for(
    check: Callable[[], bool],
    *,
    attempts: int,
    delay_s: float,
    on_retry: Optional[Callable[[int, Exception], None]] = None,
) -> bool:
    """
    Retry `check()` up to `attempts` times, sleeping `delay_s` between failures.

    Returns True as soon as `check()` returns True.
    Returns False if it never succeeds.
    """
    last_err: Optional[Exception] = None
    for i in range(1, attempts + 1):
        try:
            if check():
                return True
        except Exception as e:
            last_err = e
            if on_retry:
                on_retry(i, e)
        time.sleep(delay_s)
    return False


def heartbeat_sleep(
    *,
    sleep_s: float,
    heartbeat_every_s: float,
    heartbeat: Callable[[], None],
    tick_s: float = 2.0,
) -> None:
    """
    Sleep for `sleep_s` seconds, emitting `heartbeat()` every `heartbeat_every_s`.
    """
    elapsed = 0.0
    since_heartbeat = 0.0
    while elapsed < sleep_s:
        step = min(tick_s, sleep_s - elapsed)
        time.sleep(step)
        elapsed += step
        since_heartbeat += step
        if since_heartbeat >= heartbeat_every_s:
            heartbeat()
            since_heartbeat = 0.0

