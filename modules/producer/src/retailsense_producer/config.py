"""
Configuration settings for the RetailSense Sensor Simulator.

Uses Pydantic Settings for type-safe, environment-based configuration.
"""

from functools import lru_cache
from typing import List, Optional

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
        default="retailsense-producer",
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
    producer_instance_id: str = Field(
        default="producer-001",
        description="Unique identifier for this producer instance",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # MQTT Configuration
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
    mqtt_topic: str = Field(
        default="sensors/traffic",
        description="MQTT topic to publish sensor data to",
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
    mqtt_client_timeout: int = Field(
        default=60,
        ge=1,
        le=3600,
        description="MQTT connection timeout in seconds",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Producer Configuration
    # ─────────────────────────────────────────────────────────────────────────
    producer_rate: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Number of events to generate per second",
    )
    batch_size: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Number of events per batch",
    )
    batch_interval_ms: int = Field(
        default=1000,
        ge=100,
        le=60000,
        description="Interval between batches in milliseconds",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Sensor Simulation Configuration
    # ─────────────────────────────────────────────────────────────────────────
    num_stores: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Number of simulated stores",
    )
    num_zones_per_store: int = Field(
        default=4,
        ge=1,
        le=20,
        description="Number of zones per store",
    )
    min_visitors: int = Field(
        default=1,
        ge=0,
        description="Minimum visitors per event",
    )
    max_visitors: int = Field(
        default=50,
        ge=1,
        description="Maximum visitors per event",
    )
    min_dwell_time_seconds: float = Field(
        default=5.0,
        ge=0.0,
        description="Minimum dwell time in seconds",
    )
    max_dwell_time_seconds: float = Field(
        default=300.0,
        ge=1.0,
        description="Maximum dwell time in seconds",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Data Generation Options
    # ─────────────────────────────────────────────────────────────────────────
    use_realistic_patterns: bool = Field(
        default=True,
        description="Generate realistic traffic patterns (peaks during rush hours)",
    )
    timezone: str = Field(
        default="Europe/Madrid",
        description="Timezone for realistic patterns",
    )
    seed: Optional[int] = Field(
        default=None,
        description="Random seed for reproducible data generation",
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

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validate timezone is a valid IANA timezone string."""
        # Common valid timezones (add more if needed)
        valid_timezones = {
            "UTC",
            "Europe/Madrid",
            "Europe/Berlin",
            "Europe/Zurich",
            "America/New_York",
            "America/Los_Angeles",
            "Asia/Tokyo",
            "Australia/Sydney",
        }

        # If it's in our known list, accept it
        if v in valid_timezones:
            return v

        # Otherwise, try to validate using zoneinfo (may fail on some systems)
        try:
            from zoneinfo import ZoneInfo

            ZoneInfo(v)
            return v
        except Exception:
            # If validation fails, raise a clear error
            raise ValueError(
                f"Invalid timezone: '{v}'. Must be a valid IANA timezone (e.g., 'Europe/Madrid', 'UTC')."
            )

    @field_validator("max_visitors")
    @classmethod
    def validate_visitor_range(cls, v: int, info) -> int:
        """Ensure max_visitors > min_visitors."""
        min_v = info.data.get("min_visitors", 1)
        if v <= min_v:
            raise ValueError(
                f"max_visitors ({v}) must be greater than min_visitors ({min_v})"
            )
        return v

    @field_validator("max_dwell_time_seconds")
    @classmethod
    def validate_dwell_time_range(cls, v: float, info) -> float:
        """Ensure max_dwell_time > min_dwell_time."""
        min_v = info.data.get("min_dwell_time_seconds", 5.0)
        if v <= min_v:
            raise ValueError(
                f"max_dwell_time_seconds ({v}) must be greater than min_dwell_time_seconds ({min_v})"
            )
        return v

    # ─────────────────────────────────────────────────────────────────────────
    # Computed Properties
    # ─────────────────────────────────────────────────────────────────────────
    @property
    def mqtt_client_id(self) -> str:
        """Generate a unique MQTT client ID."""
        return f"{self.app_name}-{self.producer_instance_id}"

    @property
    def store_ids(self) -> List[str]:
        """Generate store IDs."""
        return [f"STORE_{i:02d}" for i in range(1, self.num_stores + 1)]

    @property
    def zone_ids(self) -> List[str]:
        """Generate zone IDs."""
        return [f"ZONE_{chr(65 + i)}" for i in range(self.num_zones_per_store)]


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses LRU cache to ensure settings are only loaded once.
    """
    return Settings()


# Global settings instance
settings = get_settings()
