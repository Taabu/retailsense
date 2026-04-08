"""
Configuration settings for the RetailSense Spark Processor.

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
        default="retailsense-processor",
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
    processor_instance_id: str = Field(
        default="processor-001",
        description="Unique identifier for this processor instance",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Spark Configuration
    # ─────────────────────────────────────────────────────────────────────────
    spark_master: str = Field(
        default="local[*]",
        description="Spark master URL (e.g., local[*], spark://master:7077)",
    )
    spark_app_name: str = Field(
        default="RetailSense-Processor",
        description="Spark application name",
    )
    spark_executor_memory: str = Field(
        default="2g",
        description="Spark executor memory",
    )
    spark_driver_memory: str = Field(
        default="1g",
        description="Spark driver memory",
    )
    spark_cores_max: int = Field(
        default=4,
        ge=1,
        le=32,
        description="Maximum number of cores for Spark",
    )
    spark_shuffle_partitions: int = Field(
        default=200,
        ge=10,
        le=1000,
        description="Number of shuffle partitions",
    )
    spark_sql_shuffle_partitions: int = Field(
        default=200,
        ge=10,
        le=1000,
        description="Number of shuffle partitions for SQL operations",
    )
    spark_default_parallelism: int = Field(
        default=200,
        ge=10,
        le=1000,
        description="Default parallelism for Spark operations",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Kafka Configuration
    # ─────────────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    kafka_topic: str = Field(
        default="sensor-events",
        description="Kafka topic to consume from",
    )
    kafka_group_id: str = Field(
        default="retailsense-processor-group",
        description="Kafka consumer group ID",
    )
    kafka_starting_offsets: str = Field(
        default="latest",
        description="Kafka starting offsets (earliest, latest)",
    )
    kafka_max_offsets_per_partition: int = Field(
        default=1000,
        ge=100,
        le=10000,
        description="Maximum number of offsets to fetch per partition",
    )
    kafka_fail_on_data_loss: bool = Field(
        default=False,  # For development only
        description="Fail streaming query if data loss is detected",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Iceberg / MinIO Configuration
    # ─────────────────────────────────────────────────────────────────────────
    iceberg_warehouse: str = Field(
        default="s3a://iceberg-data/",
        description="Iceberg warehouse location (S3 path)",
    )
    minio_endpoint: str = Field(
        default="http://localhost:9000",
        description="MinIO/S3 endpoint URL",
    )
    minio_access_key: str = Field(
        default="minioadmin",
        description="MinIO/S3 access key ID",
    )
    minio_secret_key: str = Field(
        default="minioadmin",
        description="MinIO/S3 secret access key",
    )
    minio_region: str = Field(
        default="us-east-1",
        description="MinIO/S3 region",
    )
    s3_path_style_access: bool = Field(
        default=True,
        description="Use path-style access for S3 (required for MinIO)",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Hive Metastore Configuration
    # ─────────────────────────────────────────────────────────────────────────
    hive_metastore_uri: str = Field(
        default="thrift://localhost:9083",
        description="Hive Metastore Thrift URI",
    )
    hive_database: str = Field(
        default="retailsense",
        description="Hive database name for Iceberg tables",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Stream Processing Configuration
    # ─────────────────────────────────────────────────────────────────────────
    checkpoint_path: str = Field(
        default="/tmp/spark-checkpoints/retailsense",
        description="Spark checkpoint location for fault tolerance",
    )
    watermark_delay: str = Field(
        default="10 minutes",
        description="Watermark delay for late data handling",
    )
    aggregation_window: str = Field(
        default="5 minutes",
        description="Time window for aggregations",
    )
    trigger_interval: str = Field(
        default="5 seconds",
        description="Streaming query trigger interval",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Dead Letter Queue Configuration
    # ─────────────────────────────────────────────────────────────────────────
    dlq_enabled: bool = Field(
        default=False,
        description="Enable dead letter queue for failed messages",
    )
    dlq_topic: str = Field(
        default="sensor-events-dlq",
        description="Dead letter queue Kafka topic",
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

    @field_validator("kafka_starting_offsets")
    @classmethod
    def validate_kafka_offsets(cls, v: str) -> str:
        """Validate Kafka starting offsets setting."""
        valid_offsets = {"earliest", "latest"}
        v_lower = v.lower()
        if v_lower not in valid_offsets:
            raise ValueError(
                f"kafka_starting_offsets must be one of {valid_offsets}, got '{v}'"
            )
        return v_lower

    @field_validator("minio_endpoint")
    @classmethod
    def validate_minio_endpoint(cls, v: str) -> str:
        """Validate MinIO endpoint is a valid URL."""
        if not v.startswith(("http://", "https://")):
            raise ValueError(
                f"minio_endpoint must start with http:// or https://, got '{v}'"
            )
        return v

    @field_validator("spark_executor_memory", "spark_driver_memory")
    @classmethod
    def validate_memory(cls, v: str) -> str:
        """Validate memory format (e.g., 1g, 2g, 512m)."""
        import re

        pattern = r"^\d+[mgkMGK]$"
        if not re.match(pattern, v):
            raise ValueError(f"memory must be in format like '1g', '512m', got '{v}'")
        return v.lower()

    # ─────────────────────────────────────────────────────────────────────────
    # Computed Properties
    # ─────────────────────────────────────────────────────────────────────────
    @property
    def kafka_bootstrap_servers_list(self) -> List[str]:
        """Parse Kafka bootstrap servers into a list."""
        return [s.strip() for s in self.kafka_bootstrap_servers.split(",")]

    @property
    def spark_config_dict(self) -> dict:
        """Generate Spark configuration dictionary."""
        return {
            "spark.app.name": self.spark_app_name,
            "spark.executor.memory": self.spark_executor_memory,
            "spark.driver.memory": self.spark_driver_memory,
            "spark.cores.max": str(self.spark_cores_max),
            "spark.sql.shuffle.partitions": str(self.spark_shuffle_partitions),
            "spark.default.parallelism": str(self.spark_default_parallelism),
            "spark.sql.streaming.schemaInference": "true",
        }

    @property
    def s3_config_dict(self) -> dict:
        """Generate S3/MinIO configuration dictionary."""
        return {
            "fs.s3a.access.key": self.minio_access_key,
            "fs.s3a.secret.key": self.minio_secret_key,
            "fs.s3a.endpoint": self.minio_endpoint,
            "fs.s3a.path.style.access": str(self.s3_path_style_access).lower(),
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses LRU cache to ensure settings are only loaded once.
    """
    return Settings()


# Global settings instance
settings = get_settings()
