"""Unit tests for scripts/ingestion/_memory_guard.py -- the peak-RSS tracker
and hard safety-threshold check every staging script calls after each chunk.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026
"""

from __future__ import annotations

import pytest

from scripts.ingestion._memory_guard import MemoryLimitExceeded, PeakRSSMonitor

pytestmark = [pytest.mark.unit]


def test_check_returns_current_rss_and_does_not_raise_under_threshold():
    monitor = PeakRSSMonitor(safety_threshold_bytes=10**12)  # ~1 TB, never crossed
    rss = monitor.check()
    assert rss > 0
    assert monitor.peak_rss_bytes >= rss


def test_check_raises_once_rss_crosses_the_safety_threshold():
    # threshold of 1 byte -- current process RSS is certain to exceed it
    monitor = PeakRSSMonitor(safety_threshold_bytes=1)
    with pytest.raises(MemoryLimitExceeded, match="exceeded safety threshold"):
        monitor.check()


def test_peak_rss_mb_tracks_the_observed_maximum_not_the_latest_sample():
    monitor = PeakRSSMonitor(safety_threshold_bytes=10**12)
    monitor.peak_rss_bytes = 500 * 1024 * 1024  # simulate an earlier, higher sample
    monitor.check()  # a real (lower) sample must not overwrite a higher peak
    assert monitor.peak_rss_mb >= 500


def test_default_threshold_is_below_the_documented_1gb_hard_limit():
    monitor = PeakRSSMonitor()
    assert monitor.safety_threshold_bytes < 1024 * 1024 * 1024
