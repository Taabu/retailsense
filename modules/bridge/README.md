# RetailSense Bridge

MQTT to Kafka bridge for the RetailSense data pipeline.

## Overview

This module subscribes to MQTT topics from edge sensors and forwards the messages to Kafka for downstream processing.

## Usage

```bash
uv run python -m retailsense_bridge.main