"""Tests for the Spark session factory."""

import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_session_dates_timestamps_in_utc(spark: SparkSession) -> None:
    """A late-evening UTC event must keep its UTC date on any host."""
    df = spark.createDataFrame([("2026-10-28T23:30:00Z",)], ["created_at"])

    day = df.select(F.to_date(F.to_timestamp("created_at")).alias("day")).first()

    assert day is not None
    assert day["day"] == dt.date(2026, 10, 28)
