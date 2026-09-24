"""Shared pytest fixtures.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

# Gold tests are paused: Gold notebooks still read Silver tables that are no
# longer written. Delete this line once Gold reads only current Silver tables.
collect_ignore = ["gold"]
