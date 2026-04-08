import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


def clean_sensor_event(df: DataFrame) -> DataFrame:
    """Remove invalid/noisy events."""

    # Filter out invalid events
    df_clean = df.filter(
        (col("dwell_time_seconds") > 0)
        & (col("dwell_time_seconds") <= 3600)
        & (col("store_id").isNotNull())
        & (col("zone_id").isNotNull())
        & (col("event_id").isNotNull())
    )

    # Log a message without triggering execution
    logger.info("Applied noise filter to streaming data.")

    return df_clean
