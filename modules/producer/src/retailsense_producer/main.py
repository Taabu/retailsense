"""
Main entry point for the RetailSense Sensor Simulator.
Generates fake shopper events and publishes them to MQTT.
"""

import asyncio
import logging
from typing import Optional

from paho.mqtt import client as mqtt_client

from retailsense_producer.config import settings
from retailsense_producer.simulator import EventSimulator, SensorEvent

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MqttPublisher:
    """Publishes sensor events to MQTT broker."""

    def __init__(self):
        self.client: Optional[mqtt_client.Client] = None
        self.simulator = EventSimulator()
        self.running = False

    def on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker."""
        if rc == 0:
            logger.info("✅ Connected to MQTT Broker!")
        else:
            logger.error(f"❌ Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from MQTT broker."""
        logger.warning("⚠️ Disconnected from MQTT Broker")

    async def connect(self):
        """Connect to MQTT broker."""
        self.client = mqtt_client.Client(
            client_id=f"retailsense_producer_{uuid.uuid4().hex[:8]}"
        )
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

        # Connect to broker
        self.client.connect(settings.mqtt_broker_host, settings.mqtt_broker_port, 60)
        self.client.loop_start()

        # Wait for connection
        await asyncio.sleep(1)

    async def publish_event(self, event: SensorEvent):
        """Publish a single event to MQTT."""
        if not self.client:
            logger.error("MQTT client not connected")
            return

        payload = event.model_dump_json()
        result = self.client.publish(settings.mqtt_topic, payload)
        result.wait_for_publish()

    async def publish_loop(self):
        """Main loop: generate and publish events at specified rate."""
        self.running = True
        interval = 1.0 / settings.producer_rate

        logger.info(
            f"🚀 Starting event generation at {settings.producer_rate} events/sec..."
        )
        logger.info(f"📡 Publishing to topic: {settings.mqtt_topic}")

        event_count = 0

        try:
            while self.running:
                event = self.simulator.generate_event()
                await self.publish_event(event)

                event_count += 1

                # Log progress every 100 events
                if event_count % 100 == 0:
                    logger.info(f"📨 Published {event_count} events")
                    logger.debug(
                        f"Last event: {event.store_id} -> {event.zone_id} ({event.dwell_time_seconds}s)"
                    )

                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            logger.info("🛑 Publisher loop cancelled")
        finally:
            self.running = False

    async def stop(self):
        """Stop the publisher."""
        self.running = False
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("🔌 MQTT client disconnected")


async def main():
    """Main entry point."""
    publisher = MqttPublisher()

    try:
        await publisher.connect()
        await publisher.publish_loop()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await publisher.stop()


if __name__ == "__main__":
    import uuid

    # Run the async main function
    asyncio.run(main())
