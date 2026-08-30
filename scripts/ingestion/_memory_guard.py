# Shared memory guard
# Energy Commerce and Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: lightweight peak-RSS tracking + a hard safety-threshold check
# for the staging scripts. Call check() after writing each chunk;
# it samples current RSS, updates the observed peak, and raises if RSS
# has crossed the safety threshold -- fail clearly rather than let a
# script silently balloon past the 1 GB hard limit.

import os

import psutil

SAFETY_THRESHOLD_BYTES = 900 * 1024 * 1024  # ~900 MB, safely under the 1 GB hard limit


class MemoryLimitExceeded(RuntimeError):
    pass


class PeakRSSMonitor:
    def __init__(self, safety_threshold_bytes: int = SAFETY_THRESHOLD_BYTES):
        self._process = psutil.Process(os.getpid())
        self.safety_threshold_bytes = safety_threshold_bytes
        self.peak_rss_bytes = self._process.memory_info().rss

    def check(self) -> int:
        rss = self._process.memory_info().rss
        self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
        if rss > self.safety_threshold_bytes:
            raise MemoryLimitExceeded(
                f"RSS {rss / 1024 / 1024:.1f} MB exceeded safety threshold "
                f"{self.safety_threshold_bytes / 1024 / 1024:.0f} MB -- aborting."
            )
        return rss

    @property
    def peak_rss_mb(self) -> float:
        return self.peak_rss_bytes / 1024 / 1024
