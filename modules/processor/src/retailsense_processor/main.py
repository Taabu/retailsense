import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DoubleType, LongType, StringType, StructType

from retailsense_processor.config import settings
from retailsense_processor.transformations import (
    aggregate_traffic,
    clean_sensor_event,
    enrich_with_metadata,
)

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Configure Spark with Iceberg and Kafka dependencies."""
    logger.info("Initializing Spark Session...")

    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "software.amazon.awssdk:bundle:2.17.178",
        "software.amazon.awssdk:url-connection-client:2.17.178",
    ]

    try:
        spark = (
            SparkSession.builder.appName(settings.spark_app_name)
            .master(settings.spark_master)
            .config("spark.jars.packages", ",".join(packages))
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config(
                "spark.sql.catalog.spark_catalog.warehouse", settings.iceberg_warehouse
            )
            .config("spark.sql.catalog.spark_catalog.uri", settings.hive_metastore_uri)
            .config(
                "spark.sql.catalog.spark_catalog.s3.endpoint", settings.minio_endpoint
            )
            .config("spark.sql.catalog.spark_catalog.s3.path-style-access", "true")
            .config(
                "spark.sql.catalog.spark_catalog.s3.access-key",
                settings.minio_access_key,
            )
            .config(
                "spark.sql.catalog.spark_catalog.s3.secret-key",
                settings.minio_secret_key,
            )
            .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.sql.streaming.schemaInference", "true")
            .config(
                "spark.driver.extraJavaOptions",
                "-Dio.netty.tryReflectionSetAccessible=true",
            )
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark Session Created")
        return spark
    except Exception as e:
        logger.error(f"❌ Failed to create Spark Session: {e}")
        raise


def main():
    spark = create_spark_session()

    try:
        # 1. Read from Kafka
        logger.info(
            f"📡 Connecting to Kafka: {settings.kafka_bootstrap_servers} / {settings.kafka_topic}"
        )
        df_raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
            .option("subscribe", settings.kafka_topic)
            .option("startingOffsets", settings.kafka_starting_offsets)
            .option("failOnDataLoss", settings.kafka_fail_on_data_loss)
            .load()
        )

        # 2. Parse JSON
        schema = (
            StructType()
            .add("event_id", StringType())
            .add("timestamp", LongType())
            .add("store_id", StringType())
            .add("zone_id", StringType())
            .add("sensor_id", StringType())
            .add("age_range", StringType())
            .add("gender", StringType())
            .add("dwell_time_seconds", DoubleType())
        )

        df_parsed = (
            df_raw.selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), schema).alias("data"))
            .select("data.*")
        )

        # 3. Clean
        logger.info("🧹 Applying noise filter...")
        df_clean = clean_sensor_event(df_parsed)

        # 4. Enrich
        logger.info("🔗 Enriching with metadata...")
        df_enriched = enrich_with_metadata(df_clean, spark)

        # 5. Add Event Time & Watermark
        df_with_event_time = df_enriched.withColumn(
            "event_time", to_timestamp(col("timestamp") / 1000)
        )
        df_watermarked = df_with_event_time.withWatermark(
            "event_time", settings.watermark_delay
        )

        # 6. Aggregate
        logger.info("📊 Aggregating traffic...")
        df_agg = aggregate_traffic(df_watermarked)

        # 7. Write to Iceberg
        def write_to_iceberg(batch_df, batch_id):
            try:
                # Get input count (this is safe inside foreachBatch because it's a micro-batch, not a stream)
                # Note: .count() on a micro-batch DataFrame is allowed and executes immediately.
                input_count = batch_df.count()

                spark.sql(f"CREATE DATABASE IF NOT EXISTS {settings.hive_database}")
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {settings.hive_database}.traffic_stats (
                        window_start TIMESTAMP,
                        window_end TIMESTAMP,
                        store_id STRING,
                        zone_id STRING,
                        visitor_count LONG,
                        total_dwell_time DOUBLE
                    ) USING iceberg
                    PARTITIONED BY (store_id, zone_id)
                """)

                batch_df.write.mode("append").format("iceberg").saveAsTable(
                    f"{settings.hive_database}.traffic_stats"
                )

                # You could also count the output if needed, but usually input count is enough for logging
                logger.info(
                    f"✅ Batch {batch_id} written. Input records: {input_count}"
                )

            except Exception as e:
                logger.error(f"❌ Failed to write batch {batch_id}: {e}")
                raise

        logger.info("🚀 Starting Stream Query...")
        query = (
            df_agg.writeStream.outputMode("update")
            .option("checkpointLocation", settings.checkpoint_path)
            .foreachBatch(write_to_iceberg)
            .trigger(processingTime=settings.trigger_interval)
            .start()
        )

        logger.info("🟢 Stream started. Waiting for data...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"💥 Stream failed: {e}")
        sys.exit(1)
    finally:
        logger.info("🛑 Shutting down Spark Session...")
        spark.stop()


if __name__ == "__main__":
    main()
