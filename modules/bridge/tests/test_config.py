"""
Tests for configuration settings.
"""

import os

import pytest
from pydantic import ValidationError

from retailsense_bridge.config import Settings, get_settings


class TestSettingsValidation:
    """Test settings validation logic."""

    def test_default_settings(self):
        """Test that default settings load correctly."""
        settings = Settings()

        assert settings.app_name == "retailsense-bridge"
        assert settings.log_level == "INFO"
        assert settings.mqtt_broker_port == 1883
        assert settings.kafka_acks == "all"

    def test_custom_env_variables(self, settings_override):
        """Test that environment variables override defaults."""
        settings = Settings()

        assert settings.mqtt_broker_host == "test-mqtt.local"
        assert settings.kafka_bootstrap_servers == "test-kafka:9092"
        assert settings.bridge_instance_id == "test-001"

    def test_invalid_log_level(self):
        """Test that invalid log level raises error."""
        os.environ["LOG_LEVEL"] = "INVALID"

        with pytest.raises(ValidationError):
            Settings()

        del os.environ["LOG_LEVEL"]

    def test_invalid_kafka_acks(self):
        """Test that invalid Kafka acks raises error."""
        os.environ["KAFKA_ACKS"] = "invalid"

        with pytest.raises(ValidationError):
            Settings()

        del os.environ["KAFKA_ACKS"]

    def test_valid_kafka_acks_values(self):
        """Test all valid Kafka acks values."""
        for acks in ["0", "1", "all"]:
            os.environ["KAFKA_ACKS"] = acks
            settings = Settings()
            assert settings.kafka_acks == acks
            del os.environ["KAFKA_ACKS"]

    def test_port_validation(self):
        """Test that port numbers are validated."""
        # Valid port
        os.environ["MQTT_BROKER_PORT"] = "8080"
        settings = Settings()
        assert settings.mqtt_broker_port == 8080
        del os.environ["MQTT_BROKER_PORT"]

        # Invalid port (too high)
        os.environ["MQTT_BROKER_PORT"] = "70000"
        with pytest.raises(ValidationError):
            Settings()
        del os.environ["MQTT_BROKER_PORT"]

    def test_cache_behavior(self):
        """Test that settings are cached."""
        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2

    def test_mqtt_client_id_generation(self):
        """Test MQTT client ID is generated correctly."""
        settings = Settings()
        expected = f"{settings.app_name}-{settings.bridge_instance_id}"
        assert settings.mqtt_client_id == expected

    def test_kafka_bootstrap_servers_parsing(self):
        """Test Kafka bootstrap servers are parsed into list."""
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "host1:9092,host2:9092"
        settings = Settings()

        assert settings.kafka_bootstrap_servers_list == ["host1:9092", "host2:9092"]

        del os.environ["KAFKA_BOOTSTRAP_SERVERS"]
