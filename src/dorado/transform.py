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


def change_failures(events: DataFrame) -> DataFrame:
    """Count merged pull requests and those that revert or hotfix a change.

    A change counts as a failure fix when its title starts with ``Revert``, its
    branch starts with ``revert-``, or its title or branch contains ``hotfix``.
    GitHub's revert button produces the first two patterns.

    Args:
        events: Selected events as returned by `select_events`.

    Returns:
        Columns ``repo``, ``merged_changes``, and ``failed_changes``.
        Repositories without a merge are absent.
    """
    title = F.col("payload.pull_request.title")
    branch = F.col("payload.pull_request.head.ref")
    is_fix = (
        title.startswith("Revert")
        | branch.startswith("revert-")
        | F.lower(F.concat_ws(" ", title, branch)).contains("hotfix")
    )
    return (
        _merged_pull_requests(events)
        .groupBy(F.col("repo.name").alias("repo"))
        .agg(F.count("*").alias("merged_changes"), F.count_if(is_fix).alias("failed_changes"))
    )


def restores(events: DataFrame) -> DataFrame:
    """Compute the median time from opening a bug report to resolving it.

    A bug report is an issue with at least one label whose name contains
    ``bug``, ignoring case. Only issues closed as completed count.

    Args:
        events: Selected events as returned by `select_events`.

    Returns:
        Columns ``repo``, ``restored_bugs``, and ``time_to_restore_hours``.
        Repositories without a resolved bug are absent.
    """
    issue = F.col("payload.issue")
    is_bug = F.exists(issue["labels"], lambda label: F.lower(label["name"]).contains("bug"))
    return (
        events.where(
            (F.col("type") == "IssuesEvent")
            & (F.col("payload.action") == "closed")
            & (issue["state_reason"] == "completed")
            & is_bug
        )
        .groupBy(F.col("repo.name").alias("repo"))
        .agg(
            F.count("*").alias("restored_bugs"),
            F.median(_hours_between(issue["created_at"], issue["closed_at"])).alias(
                "time_to_restore_hours"
            ),
        )
    )
