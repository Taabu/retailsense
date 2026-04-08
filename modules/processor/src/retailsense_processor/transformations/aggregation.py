from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, window
from pyspark.sql.functions import sum as _sum


def aggregate_traffic(df: DataFrame) -> DataFrame:
    """Aggregate traffic by store and zone per 5-minute window."""

    # Create the window column
    window_col = window(col("event_time"), "5 minutes")

    # Group by
    df_grouped = df.groupBy(
        window_col.alias("window"), col("store_id"), col("zone_id")
    ).agg(
        count("*").alias("visitor_count"),
        _sum("dwell_time_seconds").alias("total_dwell_time"),
    )

    # Flatten the window struct into start/end columns for easier writing
    return df_grouped.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("store_id"),
        col("zone_id"),
        col("visitor_count"),
        col("total_dwell_time"),
    )
