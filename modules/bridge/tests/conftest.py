"""
Shared pytest fixtures for bridge tests.
"""

import asyncio
import os
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

# Set test environment variables before importing settings
os.environ.setdefault("BRIDGE_INSTANCE_ID", "test-instance")


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_mqtt_client():
    """Mock MQTT client for testing."""
    client = MagicMock()
    client.connect.return_value = 0
    client.subscribe.return_value = None
    client.loop_start.return_value = None
    client.loop_stop.return_value = None
    client.disconnect.return_value = None
    return client


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing."""
    producer = MagicMock()
    future = MagicMock()
    future.add_callback = MagicMock()
    future.add_errback = MagicMock()
    producer.send.return_value = future
    producer.flush.return_value = None
    producer.close.return_value = None
    return producer


@pytest.fixture
def settings_override():
    """Override settings for testing."""
    os.environ["MQTT_BROKER_HOST"] = "test-mqtt.local"
    os.environ["MQTT_BROKER_PORT"] = "1883"
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "test-kafka:9092"
    os.environ["KAFKA_TOPIC"] = "test-topic"
    os.environ["BRIDGE_INSTANCE_ID"] = "test-001"
    os.environ["LOG_LEVEL"] = "DEBUG"

    # Clear the cache to force reload
    from retailsense_bridge.config import get_settings

    get_settings.cache_clear()

    yield

    # Cleanup
    get_settings.cache_clear()


@pytest_asyncio.fixture
async def bridge(mock_mqtt_client, mock_kafka_producer):
    """Create a bridge instance with mocked dependencies."""
    from retailsense_bridge.main import MqttToKafkaBridge

    bridge = MqttToKafkaBridge()
    bridge.mqtt_client = mock_mqtt_client
    bridge.kafka_producer = mock_kafka_producer
    bridge.running = True

    return bridge


@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables to defaults before each test."""
    # Save original values
    original_log_level = os.environ.get("LOG_LEVEL")

    # Clear LOG_LEVEL to force default behavior
    if "LOG_LEVEL" in os.environ:
        del os.environ["LOG_LEVEL"]

    yield

    # Restore original value if it existed
    if original_log_level:
        os.environ["LOG_LEVEL"] = original_log_level
    elif "LOG_LEVEL" in os.environ:
        del os.environ["LOG_LEVEL"]
