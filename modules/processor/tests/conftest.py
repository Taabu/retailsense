import os
import sys

import pytest
from pyspark.sql import SparkSession

# Set test environment variables
os.environ.setdefault("PROCESSOR_INSTANCE_ID", "test-instance")
os.environ.setdefault("SPARK_MASTER", "local[*]")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("HIVE_METASTORE_URI", "thrift://localhost:9083")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """Create a real local Spark session for testing."""
    spark = (
        SparkSession.builder.appName("retailsense-test")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def reset_settings_cache():
    """Reset settings cache before each test."""
    from retailsense_processor.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def reset_log_level():
    """Reset LOG_LEVEL to default before each test."""
    original = os.environ.get("LOG_LEVEL")
    if "LOG_LEVEL" in os.environ:
        del os.environ["LOG_LEVEL"]
    yield
    if original:
        os.environ["LOG_LEVEL"] = original
    elif "LOG_LEVEL" in os.environ:
        del os.environ["LOG_LEVEL"]
