"""
conftest.py — Shared fixtures for all test modules.

A single SparkSession is created once per pytest session using local mode
(no cluster needed). This keeps test execution fast and self-contained.
"""

import os
import pytest
from pyspark.sql import SparkSession

# Java 17+ removed Subject.getSubject() which PySpark 3.5 / Hadoop requires.
# Pass the necessary --add-opens flags so the JVM allows the reflective access.
_JAVA17_OPTS = " ".join([
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
])
os.environ.setdefault("JAVA_TOOL_OPTIONS", _JAVA17_OPTS)


@pytest.fixture(scope="session")
def spark():
    """SparkSession in local mode, shared across the entire test session."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("spark-benchmark-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions", _JAVA17_OPTS)
        .config("spark.executor.extraJavaOptions", _JAVA17_OPTS)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
