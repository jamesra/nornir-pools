"""Sliding submit window yields FIFO and stays within max_in_flight."""

from __future__ import annotations

from nornir_pools import submit_bounded


def test_submit_bounded_keeps_fifo_and_depth() -> None:
    submitted: list[int] = []
    peaks: list[int] = []

    def submit(item: int) -> int:
        submitted.append(item)
        return item

    yielded = list(
        submit_bounded(submit, range(6), max_in_flight=2, on_in_flight=peaks.append)
    )
    assert submitted == list(range(6))
    assert yielded == list(range(6))
    assert max(peaks) == 2
