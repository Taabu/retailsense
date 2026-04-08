from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, dayofweek, hour, to_timestamp, when


def enrich_with_metadata(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Enrich events with metadata.

    For MVP: Adds a simple 'is_peak_hour' flag based on UTC hour.
    In Production: Join with a broadcasted metadata table from Postgres.
    """

    # Convert timestamp (milliseconds) to TIMESTAMP type first
    # timestamp is stored as Long (milliseconds since epoch)
    df_with_timestamp = df.withColumn(
        "event_timestamp", to_timestamp(col("timestamp") / 1000)
    )

    # Extract hour from the proper TIMESTAMP type
    df_with_hour = df_with_timestamp.withColumn(
        "event_hour", hour(col("event_timestamp"))
    )

    # Define peak hours (e.g., 9 AM to 6 PM)
    # Note: This is UTC. For local time, you'd need to convert based on store timezone.
    df_enriched = df_with_hour.withColumn(
        "is_peak_hour",
        when((col("event_hour") >= 9) & (col("event_hour") < 18), True).otherwise(
            False
        ),
    ).withColumn(
        "is_weekend",
        when(dayofweek(col("event_timestamp")).isin([1, 7]), True).otherwise(
            False
        ),  # 1=Sun, 7=Sat
    )

    # Drop the helper columns if not needed in final output
    return df_enriched.drop("event_hour", "event_timestamp")
