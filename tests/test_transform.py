"""Tests for the Spark transformations."""

import datetime as dt
from pathlib import Path

from pyspark.sql import SparkSession

from dorado.transform import (
    change_failures,
    deployments,
    lead_times,
    read_events,
    select_events,
)

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


def test_deployments_count_only_stable_releases(spark: SparkSession) -> None:
    """Drafts and pre-releases are not deployments."""
    events = read_events(spark, str(FIXTURES / "releases.json"))

    counts = {row["repo"]: row["releases"] for row in deployments(events).collect()}

    assert counts == {"grafana/grafana": 1, "astral-sh/uv": 1}


def test_lead_times_take_the_median_over_merged_pull_requests(spark: SparkSession) -> None:
    """Merged pull requests took 24, 2, and 4 hours. Unmerged ones do not count."""
    events = read_events(spark, str(FIXTURES / "pull_requests.json"))

    rows = lead_times(events).collect()

    assert [(row["repo"], row["lead_time_hours"]) for row in rows] == [("grafana/grafana", 4.0)]


def test_change_failures_count_reverts_and_hotfixes(spark: SparkSession) -> None:
    """Of three merged pull requests, one is a revert and one a hotfix branch."""
    events = read_events(spark, str(FIXTURES / "pull_requests.json"))

    row = change_failures(events).first()

    assert row is not None
    assert (row["repo"], row["merged_changes"], row["failed_changes"]) == ("grafana/grafana", 3, 2)
