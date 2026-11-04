"""Spark transformations from raw GH Archive events to daily DORA metrics."""

import datetime as dt
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
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
