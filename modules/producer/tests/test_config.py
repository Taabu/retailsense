"""
Tests for configuration settings.
"""

import os

import pytest
from pydantic import ValidationError

from retailsense_producer.config import Settings, get_settings


class TestSettingsValidation:
    """Test settings validation logic."""

    def test_default_settings(self):
        """Test that default settings load correctly."""
        settings = Settings()

        assert settings.app_name == "retailsense-producer"
        assert settings.log_level == "INFO"
        assert settings.mqtt_broker_port == 1883
        assert settings.producer_rate == 100

    def test_custom_env_variables(self, settings_override):
        """Test that environment variables override defaults."""
        settings = Settings()

        assert settings.mqtt_broker_host == "test-mqtt.local"
        assert settings.producer_rate == 50
        assert settings.producer_instance_id == "test-instance"

    def test_invalid_log_level(self):
        """Test that invalid log level raises error."""
        os.environ["LOG_LEVEL"] = "INVALID"

        with pytest.raises(ValidationError):
            Settings()

        del os.environ["LOG_LEVEL"]

    def test_invalid_timezone(self):
        """Test that invalid timezone raises error."""
        os.environ["TIMEZONE"] = "Invalid/Timezone"

        with pytest.raises(ValidationError):
            Settings()

        del os.environ["TIMEZONE"]

    def test_valid_timezone(self):
        """Test that valid timezone is accepted."""
        os.environ["TIMEZONE"] = "America/New_York"
        settings = Settings()
        assert settings.timezone == "America/New_York"
        del os.environ["TIMEZONE"]

    def test_visitor_range_validation(self):
        """Test that max_visitors > min_visitors."""
        os.environ["MIN_VISITORS"] = "10"
        os.environ["MAX_VISITORS"] = "5"  # Invalid

        with pytest.raises(ValidationError):
            Settings()

        del os.environ["MIN_VISITORS"]
        del os.environ["MAX_VISITORS"]

    def test_dwell_time_range_validation(self):
        """Test that max_dwell_time > min_dwell_time."""
        os.environ["MIN_DWELL_TIME_SECONDS"] = "100"
        os.environ["MAX_DWELL_TIME_SECONDS"] = "50"  # Invalid

        with pytest.raises(ValidationError):
            Settings()

        del os.environ["MIN_DWELL_TIME_SECONDS"]
        del os.environ["MAX_DWELL_TIME_SECONDS"]

    def test_cache_behavior(self):
        """Test that settings are cached."""
        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2

    def test_mqtt_client_id_generation(self):
        """Test MQTT client ID is generated correctly."""
        settings = Settings()
        expected = f"{settings.app_name}-{settings.producer_instance_id}"
        assert settings.mqtt_client_id == expected

    def test_store_ids_generation(self):
        """Test store IDs are generated correctly."""
        os.environ["NUM_STORES"] = "3"
        settings = Settings()
        assert len(settings.store_ids) == 3
        assert settings.store_ids == ["STORE_01", "STORE_02", "STORE_03"]
        del os.environ["NUM_STORES"]

    def test_zone_ids_generation(self):
        """Test zone IDs are generated correctly."""
        os.environ["NUM_ZONES_PER_STORE"] = "4"
        settings = Settings()
        assert len(settings.zone_ids) == 4
        assert settings.zone_ids == ["ZONE_A", "ZONE_B", "ZONE_C", "ZONE_D"]
        del os.environ["NUM_ZONES_PER_STORE"]
