"""
Shared pytest fixtures for producer tests.
"""

import os

import pytest

# Set test-specific overrides
os.environ.setdefault("PRODUCER_INSTANCE_ID", "test-instance")


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests."""
    import asyncio

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def reset_log_level():
    """Reset LOG_LEVEL to default before each test to ensure clean state."""
    # Save original
    original = os.environ.get("LOG_LEVEL")

    # Remove it to force default behavior for tests expecting defaults
    if "LOG_LEVEL" in os.environ:
        del os.environ["LOG_LEVEL"]

    yield

    # Restore if it existed
    if original:
        os.environ["LOG_LEVEL"] = original
    elif "LOG_LEVEL" in os.environ:
        del os.environ["LOG_LEVEL"]


@pytest.fixture
def settings_override():
    """Override settings for testing."""
    os.environ["MQTT_BROKER_HOST"] = "test-mqtt.local"
    os.environ["MQTT_BROKER_PORT"] = "1883"
    os.environ["MQTT_TOPIC"] = "test/topic"
    os.environ["PRODUCER_RATE"] = "50"
    os.environ["BATCH_SIZE"] = "50"
    os.environ["NUM_STORES"] = "3"
    os.environ["NUM_ZONES_PER_STORE"] = "3"
    os.environ["LOG_LEVEL"] = "DEBUG"  # Explicitly set for this specific override

    # Clear the cache to force reload
    from retailsense_producer.config import get_settings

    get_settings.cache_clear()

    yield

    # Cleanup
    get_settings.cache_clear()
