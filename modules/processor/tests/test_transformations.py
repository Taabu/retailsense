from datetime import datetime, timezone

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retailsense_processor.transformations import (
    aggregate_traffic,
    clean_sensor_event,
    enrich_with_metadata,
)


class TestNoiseFilter:
    """Test the noise filter transformation."""

    def test_filter_invalid_dwell_time_zero(self, spark):
        """Test filtering out zero dwell time."""
        data = [
            ("evt1", 1000, "STORE_01", "ZONE_A", "SENSOR_1", "26-35", "male", 0.0),
            ("evt2", 1000, "STORE_01", "ZONE_A", "SENSOR_1", "26-35", "male", 5.0),
            (
                "evt3",
                1000,
                "STORE_01",
                "ZONE_A",
                "SENSOR_1",
                "26-35",
                "male",
                3700.0,
            ),  # > 3600
        ]
        schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("sensor_id", StringType(), True),
                StructField("age_range", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = clean_sensor_event(df)
        collected = result.collect()

        # Should only keep the row with 5.0 seconds
        assert len(collected) == 1
        assert collected[0]["dwell_time_seconds"] == 5.0

    def test_filter_null_store_id(self, spark):
        """Test filtering out null store_id."""
        data = [
            ("evt1", 1000, None, "ZONE_A", "SENSOR_1", "26-35", "male", 5.0),
            ("evt2", 1000, "STORE_01", "ZONE_A", "SENSOR_1", "26-35", "male", 5.0),
        ]
        schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("sensor_id", StringType(), True),
                StructField("age_range", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = clean_sensor_event(df)
        collected = result.collect()

        assert len(collected) == 1
        assert collected[0]["store_id"] == "STORE_01"

    def test_filter_null_zone_id(self, spark):
        """Test filtering out null zone_id."""
        data = [
            ("evt1", 1000, "STORE_01", None, "SENSOR_1", "26-35", "male", 5.0),
            ("evt2", 1000, "STORE_01", "ZONE_A", "SENSOR_1", "26-35", "male", 5.0),
        ]
        schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("sensor_id", StringType(), True),
                StructField("age_range", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = clean_sensor_event(df)
        collected = result.collect()

        assert len(collected) == 1
        assert collected[0]["zone_id"] == "ZONE_A"

    def test_filter_dwell_time_exceeds_limit(self, spark):
        """Test filtering out dwell time > 3600."""
        data = [
            ("evt1", 1000, "STORE_01", "ZONE_A", "SENSOR_1", "26-35", "male", 3601.0),
            ("evt2", 1000, "STORE_01", "ZONE_A", "SENSOR_1", "26-35", "male", 5.0),
        ]
        schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("sensor_id", StringType(), True),
                StructField("age_range", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = clean_sensor_event(df)
        collected = result.collect()

        assert len(collected) == 1
        assert collected[0]["dwell_time_seconds"] == 5.0


class TestEnrichment:
    """Test the enrichment transformation."""

    def test_adds_is_peak_hour_column(self, spark):
        """Test that is_peak_hour column is added."""
        # 9 AM (9*3600*1000 ms) -> Peak
        # 2 AM (2*3600*1000 ms) -> Not Peak
        data = [
            (
                "evt1",
                9 * 3600 * 1000,
                "STORE_01",
                "ZONE_A",
                "SENSOR_1",
                "26-35",
                "male",
                5.0,
            ),
            (
                "evt2",
                2 * 3600 * 1000,
                "STORE_01",
                "ZONE_A",
                "SENSOR_1",
                "26-35",
                "male",
                5.0,
            ),
        ]
        schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("sensor_id", StringType(), True),
                StructField("age_range", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = enrich_with_metadata(df, spark)
        collected = result.collect()

        assert len(collected) == 2
        # Check is_peak_hour values (True for 9 AM, False for 2 AM)
        # Note: The logic in enrichment.py uses hour() which extracts UTC hour
        assert collected[0]["is_peak_hour"]
        assert not collected[1]["is_peak_hour"]

    def test_adds_is_weekend_column(self, spark):
        """Test that is_weekend column is added."""
        # Sunday (dayofweek=1) -> Weekend
        # Monday (dayofweek=2) -> Not Weekend
        # We need specific timestamps for Sunday and Monday
        # Jan 1, 2024 was a Monday. Jan 7, 2024 was a Sunday.
        # Monday: 1704067200000 (Jan 1, 2024 00:00:00 UTC)
        # Sunday: 1704585600000 (Jan 7, 2024 00:00:00 UTC)
        data = [
            (
                "evt1",
                1704585600000,
                "STORE_01",
                "ZONE_A",
                "SENSOR_1",
                "26-35",
                "male",
                5.0,
            ),  # Sunday
            (
                "evt2",
                1704067200000,
                "STORE_01",
                "ZONE_A",
                "SENSOR_1",
                "26-35",
                "male",
                5.0,
            ),  # Monday
        ]
        schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("sensor_id", StringType(), True),
                StructField("age_range", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = enrich_with_metadata(df, spark)
        collected = result.collect()

        assert len(collected) == 2
        assert collected[0]["is_weekend"]  # Sunday
        assert not collected[1]["is_weekend"]  # Monday

    def test_drops_helper_column(self, spark):
        """Test that helper column (event_hour) is dropped."""
        data = [
            (
                "evt1",
                9 * 3600 * 1000,
                "STORE_01",
                "ZONE_A",
                "SENSOR_1",
                "26-35",
                "male",
                5.0,
            ),
        ]
        schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("sensor_id", StringType(), True),
                StructField("age_range", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = enrich_with_metadata(df, spark)
        columns = result.columns

        assert "event_hour" not in columns
        assert "is_peak_hour" in columns


class TestAggregation:
    """Test the aggregation transformation."""

    def test_creates_window_column(self, spark):
        """Test that window column is created and flattened."""
        # Two events in the same 5-minute window
        # Use proper datetime objects for TimestampType
        dt1 = datetime(2026, 4, 7, 10, 0, 0, tzinfo=timezone.utc)
        dt2 = datetime(2026, 4, 7, 10, 0, 1, tzinfo=timezone.utc)
        data = [
            (dt1, "STORE_01", "ZONE_A", 5.0),
            (dt2, "STORE_01", "ZONE_A", 10.0),
        ]
        schema = StructType(
            [
                StructField("event_time", TimestampType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = aggregate_traffic(df)
        collected = result.collect()

        assert len(collected) == 1
        assert "window_start" in result.columns
        assert "window_end" in result.columns
        assert collected[0]["visitor_count"] == 2
        assert collected[0]["total_dwell_time"] == 15.0

    def test_groups_by_store_and_zone(self, spark):
        """Test grouping by store_id and zone_id."""
        dt = datetime(2026, 4, 7, 10, 0, 0, tzinfo=timezone.utc)
        data = [
            (dt, "STORE_01", "ZONE_A", 5.0),
            (dt, "STORE_01", "ZONE_B", 10.0),
            (dt, "STORE_02", "ZONE_A", 15.0),
        ]
        schema = StructType(
            [
                StructField("event_time", TimestampType(), True),
                StructField("store_id", StringType(), True),
                StructField("zone_id", StringType(), True),
                StructField("dwell_time_seconds", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = aggregate_traffic(df)
        collected = result.collect()

        assert len(collected) == 3
        counts = {(r["store_id"], r["zone_id"]): r["visitor_count"] for r in collected}
        assert counts[("STORE_01", "ZONE_A")] == 1
        assert counts[("STORE_01", "ZONE_B")] == 1
        assert counts[("STORE_02", "ZONE_A")] == 1
