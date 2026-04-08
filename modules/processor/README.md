# RetailSense Processor

Spark Structured Streaming processor for the RetailSense data pipeline.

## Overview

This module consumes real-time sensor events from Kafka, cleans noisy data, enriches it with metadata (e.g., peak hours, weekends), aggregates traffic statistics by store and zone, and writes the results to an Iceberg data lake for analytics.

## Usage

```bash
uv run python -m retailsense_processor.main