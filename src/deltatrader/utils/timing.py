"""Timestamp utilities for microsecond precision."""

import time


def get_timestamp_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)


def get_timestamp_us() -> int:
    """Get current timestamp in microseconds."""
    return int(time.time() * 1_000_000)


def get_timestamp_seconds() -> int:
    """Get current timestamp in seconds (for REST API signing)."""
    return int(time.time())


def parse_timestamp_us(ts: int | str) -> int:
    """Parse timestamp in microseconds to integer."""
    if isinstance(ts, str):
        return int(ts)
    return ts


def us_to_seconds(us: int) -> float:
    """Convert microseconds to seconds."""
    return us / 1_000_000


def seconds_to_us(seconds: float) -> int:
    """Convert seconds to microseconds."""
    return int(seconds * 1_000_000)


def format_timestamp_us(us: int) -> str:
    """Format microsecond timestamp as human-readable string."""
    seconds = us / 1_000_000
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(seconds))
