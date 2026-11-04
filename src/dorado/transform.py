"""Spark transformations from raw GH Archive events to daily DORA metrics."""

import datetime as dt
from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

# Only the fields the metrics need. An explicit schema spares Spark a full pass
# over the input to infer one, and it keeps the columns stable when GitHub adds
# or removes fields. A field missing from an event reads as null.
EVENT_SCHEMA = """
    id STRING,
    type STRING,
    created_at TIMESTAMP,
    repo STRUCT<name: STRING>,
    payload STRUCT<
        action: STRING,
        pull_request: STRUCT<
            title: STRING,
            created_at: TIMESTAMP,
            merged_at: TIMESTAMP,
            merged: BOOLEAN,
            head: STRUCT<ref: STRING>
        >,
        issue: STRUCT<
            created_at: TIMESTAMP,
            closed_at: TIMESTAMP,
            state_reason: STRING,
            labels: ARRAY<STRUCT<name: STRING>>
        >,
        release: STRUCT<draft: BOOLEAN, prerelease: BOOLEAN>
    >
"""


def read_events(spark: SparkSession, path: str) -> DataFrame:
    """Read GH Archive files into a data frame with the event schema.

    Args:
        spark: The active Spark session.
        path: A file, directory, or glob of newline-delimited JSON. Spark reads
            gzip-compressed files directly.

    Returns:
        One row per event. Malformed lines become rows of nulls.
    """
    return spark.read.schema(EVENT_SCHEMA).json(path)


def select_events(events: DataFrame, repositories: Sequence[str], day: dt.date) -> DataFrame:
    """Keep one day of events for the tracked repositories, each event once.

    Args:
        events: Raw events as returned by `read_events`.
        repositories: Full names such as ``"grafana/grafana"``.
        day: The UTC date to keep.

    Returns:
        The matching events. The archive repeats some events, so rows are
        deduplicated on the event id.
    """
    return events.where(
        F.col("repo.name").isin(list(repositories)) & (F.to_date("created_at") == F.lit(day))
    ).dropDuplicates(["id"])


def deployments(events: DataFrame) -> DataFrame:
    """Count stable releases per repository.

    Args:
        events: Selected events as returned by `select_events`.

    Returns:
        Columns ``repo`` and ``releases``. Repositories without a release are absent.
    """
    release = F.col("payload.release")
    return (
        events.where(
            (F.col("type") == "ReleaseEvent")
            & (F.col("payload.action") == "published")
            & ~release["draft"]
            & ~release["prerelease"]
        )
        .groupBy(F.col("repo.name").alias("repo"))
        .agg(F.count("*").alias("releases"))
    )


def _hours_between(start: Column, end: Column) -> Column:
    """Return the time from start to end in hours."""
    return (end.cast("long") - start.cast("long")) / 3600


def _merged_pull_requests(events: DataFrame) -> DataFrame:
    """Keep the closing events of merged pull requests."""
    return events.where(
        (F.col("type") == "PullRequestEvent")
        & (F.col("payload.action") == "closed")
        & F.col("payload.pull_request.merged")
    )


def lead_times(events: DataFrame) -> DataFrame:
    """Compute the median time from opening to merging a pull request.

    Args:
        events: Selected events as returned by `select_events`.

    Returns:
        Columns ``repo`` and ``lead_time_hours``. Repositories without a merge
        are absent.
    """
    pull_request = F.col("payload.pull_request")
    return (
        _merged_pull_requests(events)
        .groupBy(F.col("repo.name").alias("repo"))
        .agg(
            F.median(_hours_between(pull_request["created_at"], pull_request["merged_at"])).alias(
                "lead_time_hours"
            )
        )
    )
