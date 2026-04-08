"""
Bridge: Subscribes to MQTT and forwards messages to Kafka.
"""

import asyncio
import logging
import signal
import sys
from typing import Optional

import paho.mqtt.client as mqtt_client
from kafka import KafkaProducer

from retailsense_bridge.config import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MqttToKafkaBridge:
    """Bridge service that forwards MQTT messages to Kafka."""

    def __init__(self):
        self.mqtt_client: Optional[mqtt_client.Client] = None
        self.kafka_producer: Optional[KafkaProducer] = None
        self.running = False
        self.messages_sent = 0
        self.messages_failed = 0

    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("✅ MQTT Connected")
            client.subscribe(settings.mqtt_topic)
            logger.info(f"📡 Subscribed to {settings.mqtt_topic}")
        else:
            logger.error(f"❌ MQTT Failed to connect, code {rc}")
            # Reconnect logic
            client.reconnect_delay_set(min_delay=1, max_delay=60)

    def on_mqtt_disconnect(self, client, userdata, rc):
        logger.warning(f"⚠️ MQTT Disconnected, code {rc}. Attempting reconnection...")
        if self.running:
            client.reconnect()

    def on_mqtt_message(self, client, userdata, msg):
        logger.debug(
            f"📩 RAW MESSAGE RECEIVED: topic={msg.topic}, payload={msg.payload}"
        )
        try:
            payload = msg.payload.decode("utf-8")

            # Send to Kafka (async with callback)
            future = self.kafka_producer.send(
                settings.kafka_topic,
                value=payload.encode("utf-8"),
                key=msg.topic.encode("utf-8"),
            )

            # Track result
            def on_success(record_metadata):
                self.messages_sent += 1
                logger.debug(f"✅ Message sent: offset={record_metadata.offset}")

            def on_error(error):
                self.messages_failed += 1
                logger.error(f"❌ Kafka send failed: {error}")
                # Optional: Send to dead letter queue
                # self.send_to_dlq(msg.topic, payload, str(error))

            future.add_callback(on_success)
            future.add_errback(on_error)

        except UnicodeDecodeError:
            logger.error(f"⚠️ Invalid UTF-8 payload on topic {msg.topic}")
            self.messages_failed += 1
        except Exception as e:
            logger.error(f"❌ Error processing MQTT message: {e}")
            self.messages_failed += 1

    async def run(self):
        """Main entry point for the bridge service."""
        self.running = True

        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        try:
            # Initialize Kafka
            logger.info("🚀 Initializing Kafka Producer...")
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: v,
                key_serializer=lambda k: k,
                acks="all",
                retries=3,
                retry_backoff_ms=100,
                max_in_flight_requests_per_connection=1,  # Ensure ordering
            )
            logger.info("✅ Kafka Producer Initialized")

            # Initialize MQTT
            logger.info("🚀 Initializing MQTT Client...")
            self.mqtt_client = mqtt_client.Client(
                client_id=f"retailsense-bridge-{settings.bridge_instance_id}",
                protocol=mqtt_client.MQTTv311,
            )
            self.mqtt_client.on_connect = self.on_mqtt_connect
            self.mqtt_client.on_message = self.on_mqtt_message
            self.mqtt_client.on_disconnect = self.on_mqtt_disconnect

            self.mqtt_client.connect(
                settings.mqtt_broker_host,
                settings.mqtt_broker_port,
                settings.mqtt_client_timeout,
            )
            self.mqtt_client.loop_start()
            logger.info("✅ MQTT Client Initialized")

            # Wait for shutdown signal
            logger.info("🟢 Bridge running. Press Ctrl+C to stop.")
            while self.running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.exception(f"💥 Bridge crashed: {e}")
            raise
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown of all resources."""
        logger.info("🛑 Initiating graceful shutdown...")
        self.running = False

        if self.mqtt_client:
            logger.info("📡 Stopping MQTT client...")
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

        if self.kafka_producer:
            logger.info("📨 Flushing Kafka producer...")
            self.kafka_producer.flush(timeout=30)
            self.kafka_producer.close()

        logger.info(
            f"📊 Final Stats - Sent: {self.messages_sent}, Failed: {self.messages_failed}"
        )
        logger.info("✅ Shutdown complete")


async def main():
    """Application entry point."""
    bridge = MqttToKafkaBridge()
    await bridge.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Interrupted by user")
    except Exception as e:
        logger.exception(f"💥 Unhandled exception: {e}")
        sys.exit(1)
