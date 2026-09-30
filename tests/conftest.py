import os
import sys

# Point Spark at this venv's Python (Windows has no "python3" command)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Make src/transformation importable from tests
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "src", "transformation")
)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("PytestSparkSession")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()