"""
Configuration settings for the RetailSense Bridge module.

Uses Pydantic Settings for type-safe, environment-based configuration.
"""

from functools import lru_cache
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    All settings can be overridden via environment variables.
    Environment variable names are case-insensitive.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Application Settings
    # ─────────────────────────────────────────────────────────────────────────
    app_name: str = Field(
        default="retailsense-bridge",
        description="Application name for logging and identification",
    )
    app_version: str = Field(
        default="1.0.0",
        description="Application version",
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    bridge_instance_id: str = Field(
        default="instance-001",
        description="Unique identifier for this bridge instance",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # MQTT Settings
    # ─────────────────────────────────────────────────────────────────────────
    mqtt_broker_host: str = Field(
        default="localhost",
        description="MQTT broker hostname or IP",
    )
    mqtt_broker_port: int = Field(
        default=1883,
        ge=1,
        le=65535,
        description="MQTT broker port",
    )
    mqtt_client_timeout: int = Field(
        default=60,
        ge=1,
        le=3600,
        description="MQTT connection timeout in seconds",
    )
    mqtt_topic: str = Field(
        default="sensors/traffic",
        description="MQTT topic pattern to subscribe to",
    )
    mqtt_qos: int = Field(
        default=1,
        ge=0,
        le=2,
        description="MQTT Quality of Service level (0, 1, or 2)",
    )
    mqtt_keepalive: int = Field(
        default=60,
        ge=10,
        le=3600,
        description="MQTT keepalive interval in seconds",
    )
    mqtt_clean_session: bool = Field(
        default=True,
        description="Whether to start with a clean session",
    )
    mqtt_reconnect_min_delay: int = Field(
        default=1,
        description="Minimum delay for MQTT reconnection attempts (seconds)",
    )
    mqtt_reconnect_max_delay: int = Field(
        default=60,
        description="Maximum delay for MQTT reconnection attempts (seconds)",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Kafka Settings
    # ─────────────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    kafka_topic: str = Field(
        default="sensor-traffic",
        description="Kafka topic to publish messages to",
    )
    kafka_acks: str = Field(
        default="all",
        description="Kafka acknowledgment setting (0, 1, or all)",
    )
    kafka_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Number of retries for Kafka send operations",
    )
    kafka_retry_backoff_ms: int = Field(
        default=100,
        ge=0,
        le=60000,
        description="Backoff time between retries in milliseconds",
    )
    kafka_batch_size: int = Field(
        default=16384,
        ge=1,
        le=1048576,
        description="Kafka producer batch size in bytes",
    )
    kafka_linger_ms: int = Field(
        default=5,
        ge=0,
        le=5000,
        description="Time to wait for batch to fill (milliseconds)",
    )
    kafka_buffer_memory: int = Field(
        default=33554432,  # 32 MB
        ge=1048576,
        le=1073741824,
        description="Total memory available for buffering records",
    )
    kafka_compression_type: str = Field(
        default="none",
        description="Compression type (none, gzip, snappy, lz4, zstd)",
    )
    kafka_max_in_flight_requests: int = Field(
        default=1,
        ge=1,
        le=5,
        description="Max in-flight requests per connection (1 for ordering)",
    )
    kafka_enable_idempotence: bool = Field(
        default=True,
        description="Enable idempotent producer",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Dead Letter Queue Settings
    # ─────────────────────────────────────────────────────────────────────────
    dlq_enabled: bool = Field(
        default=False,
        description="Enable dead letter queue for failed messages",
    )
    dlq_topic: str = Field(
        default="sensor-traffic-dlq",
        description="Dead letter queue topic name",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Health Check Settings
    # ─────────────────────────────────────────────────────────────────────────
    health_check_enabled: bool = Field(
        default=False,
        description="Enable HTTP health check endpoint",
    )
    health_check_port: int = Field(
        default=8080,
        ge=1,
        le=65535,
        description="Port for health check HTTP server",
    )
    health_check_path: str = Field(
        default="/health",
        description="Health check endpoint path",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Metrics Settings
    # ─────────────────────────────────────────────────────────────────────────
    metrics_enabled: bool = Field(
        default=False,
        description="Enable Prometheus metrics endpoint",
    )
    metrics_port: int = Field(
        default=9090,
        ge=1,
        le=65535,
        description="Port for Prometheus metrics server",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Validators
    # ─────────────────────────────────────────────────────────────────────────
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is one of the accepted values."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}, got '{v}'")
        return v_upper

    @field_validator("kafka_acks")
    @classmethod
    def validate_kafka_acks(cls, v: str) -> str:
        """Validate Kafka acks setting."""
        valid_acks = {"0", "1", "all"}
        if v not in valid_acks:
            raise ValueError(f"kafka_acks must be one of {valid_acks}, got '{v}'")
        return v

    @field_validator("kafka_compression_type")
    @classmethod
    def validate_kafka_compression(cls, v: str) -> str:
        """Validate Kafka compression type."""
        valid_types = {"none", "gzip", "snappy", "lz4", "zstd"}
        v_lower = v.lower()
        if v_lower not in valid_types:
            raise ValueError(
                f"kafka_compression_type must be one of {valid_types}, got '{v}'"
            )
        return v_lower

    @property
    def kafka_bootstrap_servers_list(self) -> List[str]:
        """Parse Kafka bootstrap servers into a list."""
        return [s.strip() for s in self.kafka_bootstrap_servers.split(",")]

    @property
    def mqtt_client_id(self) -> str:
        """Generate a unique MQTT client ID."""
        return f"{self.app_name}-{self.bridge_instance_id}"


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses LRU cache to ensure settings are only loaded once.
    """
    return Settings()


# Global settings instance
settings = get_settings()
