"""
Tests for the MQTT to Kafka bridge logic.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from retailsense_bridge.main import MqttToKafkaBridge


class TestMqttToKafkaBridge:
    """Test the bridge class."""

    def test_initialization(self, mock_mqtt_client, mock_kafka_producer):
        """Test bridge initializes with None dependencies."""
        bridge = MqttToKafkaBridge()

        assert bridge.mqtt_client is None
        assert bridge.kafka_producer is None
        assert bridge.running is False
        assert bridge.messages_sent == 0
        assert bridge.messages_failed == 0

    def test_on_mqtt_connect_success(self, bridge, mock_mqtt_client):
        """Test successful MQTT connection."""
        bridge.on_mqtt_connect(mock_mqtt_client, None, None, 0)

        mock_mqtt_client.subscribe.assert_called_once()
        assert bridge.messages_sent == 0

    def test_on_mqtt_connect_failure(self, bridge, mock_mqtt_client):
        """Test failed MQTT connection."""
        bridge.on_mqtt_connect(mock_mqtt_client, None, None, 5)

        mock_mqtt_client.subscribe.assert_not_called()
        mock_mqtt_client.reconnect_delay_set.assert_called_once()

    def test_on_mqtt_disconnect(self, bridge, mock_mqtt_client):
        """Test MQTT disconnection triggers reconnection."""
        bridge.running = True
        bridge.on_mqtt_disconnect(mock_mqtt_client, None, 1)

        mock_mqtt_client.reconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_mqtt_message_success(self, bridge, mock_kafka_producer):
        """Test successful message forwarding to Kafka."""
        from paho.mqtt.client import MQTTMessage

        # Create mock MQTT message
        msg = MQTTMessage()
        msg.topic = b"sensors/store1/traffic"
        msg.payload = b'{"visitor_count": 10}'

        # Mock the future callback
        future = mock_kafka_producer.send.return_value
        success_callback = None
        error_callback = None

        def capture_callbacks(callback, errback=None):
            nonlocal success_callback, error_callback
            success_callback = callback
            error_callback = errback

        future.add_callback.side_effect = capture_callbacks

        # Process message
        bridge.on_mqtt_message(None, None, msg)

        # Verify Kafka send was called
        mock_kafka_producer.send.assert_called_once()

        # Simulate success
        if success_callback:
            success_callback(MagicMock(offset=123))

        assert bridge.messages_sent == 1

    @pytest.mark.asyncio
    async def test_on_mqtt_message_kafka_failure(self, bridge, mock_kafka_producer):
        """Test Kafka send failure is handled gracefully."""
        from paho.mqtt.client import MQTTMessage

        msg = MQTTMessage()
        msg.topic = b"sensors/store1/traffic"
        msg.payload = b'{"visitor_count": 10}'

        future = mock_kafka_producer.send.return_value
        error_callback = None

        # Capture the errback callback
        def capture_errback(errback):
            nonlocal error_callback
            error_callback = errback

        future.add_errback.side_effect = capture_errback

        # Process message
        bridge.on_mqtt_message(None, None, msg)

        # Verify Kafka send was called
        mock_kafka_producer.send.assert_called_once()

        # Simulate the error callback being triggered
        if error_callback:
            error_callback(Exception("Kafka connection failed"))

        # The counter should have incremented
        assert bridge.messages_failed == 1

    @pytest.mark.asyncio
    async def test_on_mqtt_message_invalid_utf8(self, bridge):
        """Test invalid UTF-8 payload is handled."""
        from paho.mqtt.client import MQTTMessage

        msg = MQTTMessage()
        msg.topic = b"sensors/store1/traffic"
        msg.payload = b"\xff\xfe"  # Invalid UTF-8

        # Process message
        bridge.on_mqtt_message(None, None, msg)

        # The counter should have incremented
        assert bridge.messages_failed == 1

    @pytest.mark.asyncio
    async def test_shutdown_graceful(
        self, bridge, mock_mqtt_client, mock_kafka_producer
    ):
        """Test graceful shutdown closes all resources."""
        await bridge.shutdown()

        mock_mqtt_client.loop_stop.assert_called_once()
        mock_mqtt_client.disconnect.assert_called_once()
        mock_kafka_producer.flush.assert_called_once()
        mock_kafka_producer.close.assert_called_once()
        assert bridge.running is False

    @pytest.mark.asyncio
    async def test_run_sets_up_signal_handlers(
        self, bridge, mock_mqtt_client, mock_kafka_producer
    ):
        """Test run method sets up signal handlers."""
        with patch("paho.mqtt.client.Client", return_value=mock_mqtt_client):
            with patch(
                "retailsense_bridge.main.KafkaProducer",
                return_value=mock_kafka_producer,
            ):
                with patch("asyncio.get_event_loop") as mock_loop:
                    mock_loop.return_value.add_signal_handler = MagicMock()

                    # Run for a short time then cancel
                    async def cancel_after_delay():
                        await asyncio.sleep(0.1)
                        bridge.running = False

                    task = asyncio.create_task(cancel_after_delay())

                    try:
                        await bridge.run()
                    except asyncio.CancelledError:
                        pass

                    # Verify signal handlers were set up
                    assert mock_loop.return_value.add_signal_handler.called
                    # Verify connect was called on our mock
                    mock_mqtt_client.connect.assert_called_once()
                    mock_mqtt_client.loop_start.assert_called_once()
