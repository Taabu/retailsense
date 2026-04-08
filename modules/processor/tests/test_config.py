"""
Tests for configuration settings.
"""

import os

import pytest
from pydantic import ValidationError

from retailsense_processor.config import Settings, get_settings


class TestSettingsValidation:
    """Test settings validation logic."""

    def test_default_settings(self):
        """Test that default settings load correctly."""
        settings = Settings()

        assert settings.app_name == "retailsense-processor"
        assert settings.log_level == "INFO"
        assert settings.spark_master == "local[*]"
        assert settings.kafka_topic == "sensor-events"

    def test_custom_env_variables(self):
        """Test that environment variables override defaults."""
        os.environ["LOG_LEVEL"] = "DEBUG"
        os.environ["SPARK_EXECUTOR_MEMORY"] = "4g"

        settings = Settings()

        assert settings.log_level == "DEBUG"
        assert settings.spark_executor_memory == "4g"

        del os.environ["LOG_LEVEL"]
        del os.environ["SPARK_EXECUTOR_MEMORY"]

    def test_invalid_log_level(self):
        """Test that invalid log level raises error."""
        os.environ["LOG_LEVEL"] = "INVALID"

        with pytest.raises(ValidationError):
            Settings()

        del os.environ["LOG_LEVEL"]

    def test_invalid_memory_format(self):
        """Test that invalid memory format raises error."""
        os.environ["SPARK_EXECUTOR_MEMORY"] = "invalid"

        with pytest.raises(ValidationError):
            Settings()

        del os.environ["SPARK_EXECUTOR_MEMORY"]

    def test_valid_memory_formats(self):
        """Test valid memory formats."""
        for mem in ["1g", "2g", "512m", "1024k"]:
            os.environ["SPARK_EXECUTOR_MEMORY"] = mem
            settings = Settings()
            assert settings.spark_executor_memory == mem.lower()
            del os.environ["SPARK_EXECUTOR_MEMORY"]

    def test_kafka_offsets_validation(self):
        """Test Kafka offsets validation."""
        for offset in ["earliest", "latest"]:
            os.environ["KAFKA_STARTING_OFFSETS"] = offset
            settings = Settings()
            assert settings.kafka_starting_offsets == offset
            del os.environ["KAFKA_STARTING_OFFSETS"]

        os.environ["KAFKA_STARTING_OFFSETS"] = "invalid"
        with pytest.raises(ValidationError):
            Settings()
        del os.environ["KAFKA_STARTING_OFFSETS"]

    def test_minio_endpoint_validation(self):
        """Test MinIO endpoint validation."""
        os.environ["MINIO_ENDPOINT"] = "ftp://invalid"
        with pytest.raises(ValidationError):
            Settings()
        del os.environ["MINIO_ENDPOINT"]

        os.environ["MINIO_ENDPOINT"] = "http://valid"
        settings = Settings()
        assert settings.minio_endpoint == "http://valid"
        del os.environ["MINIO_ENDPOINT"]

    def test_cache_behavior(self):
        """Test that settings are cached."""
        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2

    def test_kafka_bootstrap_servers_parsing(self):
        """Test Kafka bootstrap servers are parsed into list."""
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "host1:9092,host2:9092"
        settings = Settings()

        assert settings.kafka_bootstrap_servers_list == ["host1:9092", "host2:9092"]

        del os.environ["KAFKA_BOOTSTRAP_SERVERS"]
