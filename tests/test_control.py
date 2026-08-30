# =============================================================================
# tests/test_control.py
# Testes unitários de src/control.py
# Responsável: Raul Teles
# =============================================================================
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, "/Workspace/Repos/<SEU-USER>/T3-DE-INGESTAO")


class TestIngestionControl:
    """Testa IngestionControl com SparkSession mockada."""

    def _make_control(self):
        """Helper que instancia IngestionControl com Spark mockado."""
        spark_mock = MagicMock()
        # Mock para _ensure_tables não falhar
        spark_mock.sql.return_value = MagicMock()

        from src.control import IngestionControl
        ctrl = IngestionControl.__new__(IngestionControl)
        ctrl.spark = spark_mock
        ctrl.full_schema = "mflix_catalog.bronze"
        return ctrl, spark_mock

    def test_get_watermark_returns_none_when_not_found(self):
        ctrl, spark_mock = self._make_control()
        # Simula resultado vazio do SELECT
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_df.first.return_value = None
        spark_mock.sql.return_value = mock_df

        result = ctrl.get_watermark("movies")
        assert result is None

    def test_get_watermark_returns_value_when_found(self):
        ctrl, spark_mock = self._make_control()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: "2024-01-01 00:00:00"
        mock_row.watermark_value = "2024-01-01 00:00:00"

        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_df.first.return_value = mock_row
        spark_mock.sql.return_value = mock_df

        result = ctrl.get_watermark("movies")
        assert result == "2024-01-01 00:00:00"

    def test_save_watermark_calls_merge_sql(self):
        ctrl, spark_mock = self._make_control()
        ctrl.save_watermark("comments", "2024-06-01T00:00:00")

        # Verifica que spark.sql foi chamado (MERGE)
        assert spark_mock.sql.called
        sql_call = spark_mock.sql.call_args[0][0]
        assert "MERGE" in sql_call.upper()
        assert "watermark_store" in sql_call

    def test_log_start_inserts_running_status(self):
        ctrl, spark_mock = self._make_control()
        run_id = "test-run-id-123"
        start = datetime.now(timezone.utc)

        ctrl.log_start(
            run_id=run_id,
            collection="movies",
            load_type="incremental",
            watermark_ini="2024-01-01",
            start_time=start
        )

        # Verifica que o DataFrame foi criado com status RUNNING
        assert spark_mock.createDataFrame.called
        create_df_call = spark_mock.createDataFrame.call_args
        df_data = create_df_call.args[0]
        schema = create_df_call.kwargs["schema"]
        assert len(df_data) == 1
        row = dict(zip(schema.fieldNames(), df_data[0]))
        assert row["status"] == "RUNNING"
        assert row["_ingestion_id"] == run_id
        assert row["collection"] == "movies"

    def test_log_end_updates_with_success(self):
        ctrl, spark_mock = self._make_control()
        start = datetime.now(timezone.utc)
        end = datetime.now(timezone.utc)

        ctrl.log_end(
            run_id="run-123",
            collection="movies",
            qtd_lida=1000,
            qtd_gravada=1000,
            status="SUCCESS",
            watermark_final="2024-06-01",
            start_time=start,
            end_time=end,
            erro=None
        )

        assert spark_mock.sql.called
        sql_call = spark_mock.sql.call_args[0][0]
        assert "MERGE" in sql_call.upper()
        assert "SUCCESS" in sql_call

    def test_log_end_with_error_sets_failed_status(self):
        ctrl, spark_mock = self._make_control()
        start = datetime.now(timezone.utc)

        ctrl.log_end(
            run_id="run-456",
            collection="comments",
            qtd_lida=0,
            qtd_gravada=0,
            status="FAILED",
            watermark_final=None,
            start_time=start,
            end_time=datetime.now(timezone.utc),
            erro="Connection timeout"
        )

        sql_call = spark_mock.sql.call_args[0][0]
        assert "FAILED" in sql_call
