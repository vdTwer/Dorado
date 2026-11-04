"""Tests for the Spark transformations."""

import datetime as dt
from pathlib import Path

from pyspark.sql import SparkSession

from dorado.transform import read_events, select_events

FIXTURES = Path(__file__).parent / "fixtures"
DAY = dt.date(2025, 6, 18)
REPOS = ("grafana/grafana", "astral-sh/uv")


def test_read_events_parses_nested_fields(spark: SparkSession) -> None:
    """Timestamps and nested payload fields arrive typed, extra fields are dropped."""
    events = read_events(spark, str(FIXTURES / "releases.json"))

    first = events.where("id = 'r2'").first()

    assert first is not None
    assert first["created_at"] == dt.datetime(2025, 6, 18, 10, 0)
    assert first["repo"]["name"] == "grafana/grafana"
    assert first["payload"]["release"]["prerelease"] is True
    assert "url" not in events.schema["repo"].dataType.fieldNames()


def test_select_events_keeps_one_clean_copy_per_event(spark: SparkSession) -> None:
    """Duplicates, other repos, other days, and malformed lines are dropped."""
    events = read_events(spark, str(FIXTURES / "noise.json"))

    ids = [row["id"] for row in select_events(events, REPOS, DAY).collect()]

    assert ids == ["n1"]
