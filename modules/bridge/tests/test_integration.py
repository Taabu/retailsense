"""
Integration tests for the bridge (requires running services).

These tests should be marked as integration tests and may be skipped
in CI if services are not available.
"""

import os

import pytest

# Skip integration tests if not explicitly enabled
pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION_TESTS"),
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=1 to enable.",
)
class TestBridgeIntegration:
    """Integration tests that require real MQTT and Kafka services."""

    @pytest.fixture(autouse=True)
    def skip_if_no_services(self):
        """Skip test if services are not running."""
        # Check if MQTT broker is reachable
        import socket

        mqtt_host = os.environ.get("MQTT_BROKER_HOST", "localhost")
        mqtt_port = int(os.environ.get("MQTT_BROKER_PORT", 1883))

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((mqtt_host, mqtt_port))
            sock.close()

            if result != 0:
                pytest.skip(f"MQTT broker not available at {mqtt_host}:{mqtt_port}")
        except Exception:
            pytest.skip("Could not connect to MQTT broker")

    def test_real_mqtt_connection(self):
        """Test real MQTT connection (if services available)."""
        import paho.mqtt.client as mqtt

        client = mqtt.Client()
        result = client.connect(
            os.environ.get("MQTT_BROKER_HOST", "localhost"),
            int(os.environ.get("MQTT_BROKER_PORT", 1883)),
            60,
        )

        assert result == 0
        client.disconnect()

    def test_real_kafka_connection(self):
        """Test real Kafka connection (if services available)."""
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            api_version_auto_timeout_ms=2000,
        )

        # Try to send a test message
        try:
            producer.send("test-topic", value=b"test")
            producer.flush()
        except Exception:
            pytest.skip("Kafka broker not available")
        finally:
            producer.close()
