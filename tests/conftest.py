"""Shared pytest fixtures.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="session")
def built_tables() -> dict[str, list[dict]]:
    """The synthetic operational dataset, built in-process from the fixed seed."""
    from src.generators.build import build_all

    return build_all()


@pytest.fixture(scope="session")
def pg_dsn() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "ecrmap"),
        "user": os.getenv("POSTGRES_USER", ""),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }


@pytest.fixture
def pg_conn(pg_dsn):
    """A live connection to the operational database, or skip if unreachable."""
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(connect_timeout=5, **pg_dsn)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()
