"""Shared pytest fixtures."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from dorado.spark import create_spark_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Provide one Spark session for the whole test run.

    Starting the JVM takes seconds, so the session is shared across tests.
    """
    session = create_spark_session("dorado-tests")
    yield session
    session.stop()
