"""Spark session factory shared by the job and the tests."""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "dorado") -> SparkSession:
    """Create a local Spark session tuned for small, single-machine batches.

    Spark runs in local mode inside the calling process. The daily input is a few
    gigabytes at most, so a cluster would add moving parts without any benefit.

    Args:
        app_name: Name shown in the Spark UI and logs.

    Returns:
        A new or existing Spark session.
    """
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        # Event timestamps are UTC. Pinning the session time zone keeps date
        # truncation independent of the host's locale.
        .config("spark.sql.session.timeZone", "UTC")
        # The default of 200 shuffle partitions suits clusters, not one machine.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
