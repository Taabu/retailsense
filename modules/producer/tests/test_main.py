"""
Tests for the main module (MqttPublisher).
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from retailsense_producer.main import MqttPublisher


class TestMqttPublisher:
    """Test the MqttPublisher class."""

    def test_initialization(self):
        """Test publisher initializes correctly."""
        publisher = MqttPublisher()

        assert publisher.client is None
        assert publisher.simulator is not None
        assert publisher.running is False

    def test_on_connect_success(self):
        """Test successful MQTT connection callback."""
        publisher = MqttPublisher()
        publisher.on_connect(None, None, None, 0)
        # Just verify no exception is raised

    def test_on_connect_failure(self):
        """Test failed MQTT connection callback."""
        publisher = MqttPublisher()
        publisher.on_connect(None, None, None, 5)
        # Just verify no exception is raised

    def test_on_disconnect(self):
        """Test MQTT disconnection callback."""
        publisher = MqttPublisher()
        publisher.on_disconnect(None, None, 1)
        # Just verify no exception is raised

    @pytest.mark.asyncio
    async def test_publish_event_no_client(self):
        """Test publishing when client is not connected."""
        publisher = MqttPublisher()

        event = publisher.simulator.generate_event()
        await publisher.publish_event(event)

        # Should log error but not crash
        assert publisher.client is None

    @pytest.mark.asyncio
    async def test_publish_event_success(self, settings_override):
        """Test successful event publishing with mocked client."""

        publisher = MqttPublisher()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.wait_for_publish = MagicMock()
        mock_client.publish.return_value = mock_result

        publisher.client = mock_client

        event = publisher.simulator.generate_event()
        await publisher.publish_event(event)

        mock_client.publish.assert_called_once()
        mock_result.wait_for_publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_loop(self, settings_override):
        """Test the publishing loop runs and stops correctly."""

        publisher = MqttPublisher()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.wait_for_publish = MagicMock()
        mock_client.publish.return_value = mock_result

        publisher.client = mock_client

        # Run for a short time then stop
        async def stop_after_delay():
            await asyncio.sleep(0.5)
            await publisher.stop()

        task = asyncio.create_task(stop_after_delay())

        try:
            await publisher.publish_loop()
        except asyncio.CancelledError:
            pass

        # Verify some events were published
        assert mock_client.publish.call_count > 0
        assert publisher.running is False

    @pytest.mark.asyncio
    async def test_stop(self):
        """Test graceful shutdown."""

        publisher = MqttPublisher()
        mock_client = MagicMock()
        publisher.client = mock_client

        await publisher.stop()

        mock_client.loop_stop.assert_called_once()
        mock_client.disconnect.assert_called_once()
        assert publisher.running is False
