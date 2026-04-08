"""
Tests for the main module (Spark Session and Stream).
"""

from unittest.mock import MagicMock, patch

import pytest

from retailsense_processor.main import create_spark_session, main


class TestCreateSparkSession:
    """Test Spark session creation."""

    @patch("retailsense_processor.main.SparkSession")
    def test_creates_session_with_correct_config(self, mock_spark_class):
        """Test that Spark session is created with correct config."""
        mock_builder = MagicMock()
        mock_spark_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        spark = create_spark_session()

        assert mock_builder.appName.called
        assert mock_builder.master.called
        assert mock_builder.config.called
        assert mock_builder.getOrCreate.called

    @patch("retailsense_processor.main.SparkSession")
    def test_handles_creation_failure(self, mock_spark_class):
        """Test that creation failure raises exception."""
        mock_builder = MagicMock()
        mock_spark_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.side_effect = Exception(
            "Failed to create Spark session"
        )

        with pytest.raises(Exception, match="Failed to create Spark session"):
            create_spark_session()


class TestMain:
    """Test the main function."""

    @patch("retailsense_processor.main.aggregate_traffic")
    @patch("retailsense_processor.main.enrich_with_metadata")
    @patch("retailsense_processor.main.clean_sensor_event")
    @patch("retailsense_processor.main.from_json")
    @patch("retailsense_processor.main.col")
    @patch("retailsense_processor.main.to_timestamp")
    @patch("retailsense_processor.main.create_spark_session")
    def test_main_runs_pipeline(
        self,
        mock_create_session,
        mock_to_timestamp,
        mock_col,
        mock_from_json,
        mock_clean,
        mock_enrich,
        mock_agg,
    ):
        """Test that main runs the full pipeline."""
        mock_spark = MagicMock()
        mock_create_session.return_value = mock_spark

        # Mock the readStream chain
        mock_read_stream = MagicMock()
        mock_spark.readStream = mock_read_stream
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_load = MagicMock()
        mock_read_stream.load.return_value = mock_load

        # Mock PySpark functions
        mock_col.return_value = MagicMock()
        mock_from_json.return_value = MagicMock()
        mock_to_timestamp.return_value = MagicMock()

        # Mock selectExpr and select chains
        mock_select_expr = MagicMock()
        mock_load.selectExpr.return_value = mock_select_expr
        mock_select_expr.select.return_value = mock_select_expr

        # Mock transformations
        mock_clean.return_value = mock_select_expr
        mock_enrich.return_value = mock_select_expr
        mock_agg.return_value = mock_select_expr

        # Mock withColumn / withWatermark chains
        mock_select_expr.withColumn.return_value = mock_select_expr
        mock_select_expr.withWatermark.return_value = mock_select_expr

        # Mock writeStream
        mock_write_stream = MagicMock()
        mock_select_expr.writeStream = mock_write_stream
        mock_write_stream.outputMode.return_value = mock_write_stream
        mock_write_stream.option.return_value = mock_write_stream
        mock_write_stream.foreachBatch.return_value = mock_write_stream
        mock_write_stream.trigger.return_value = mock_write_stream
        mock_query = MagicMock()
        mock_write_stream.start.return_value = mock_query

        # Mock awaitTermination to return immediately
        mock_query.awaitTermination.return_value = None

        # Run main
        main()

        # Verify pipeline steps were called
        assert mock_create_session.called
        assert mock_clean.called
        assert mock_enrich.called
        assert mock_agg.called
        assert mock_write_stream.start.called
        assert mock_query.awaitTermination.called

    @patch("retailsense_processor.main.create_spark_session")
    def test_main_handles_exception(self, mock_create_session):
        """Test that main handles exceptions gracefully."""
        mock_spark = MagicMock()
        mock_create_session.return_value = mock_spark

        # Mock readStream to fail
        mock_spark.readStream.format.side_effect = Exception("Kafka Error")

        with pytest.raises(SystemExit):
            main()

        mock_spark.stop.assert_called_once()
