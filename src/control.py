"""
control.py
----------
Manages the watermark store and ingestion control log in Delta Lake.

Provides `IngestionControl`, which:
  - Ensures the two control tables exist on startup.
  - Gets / saves watermarks via MERGE (upsert).
  - Logs the start and end of every ingestion run.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


class IngestionControl:
    """Handles watermark tracking and ingestion audit logging in Delta Lake."""

    def __init__(self, spark: SparkSession, catalog: str, schema: str) -> None:
        """
        Initialize the control layer and ensure control tables exist.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        catalog : str
            Unity Catalog name, e.g. ``'mflix_catalog'``.
        schema : str
            Schema (database) name, e.g. ``'bronze'``.
        """
        self.spark = spark
        self.full_schema = f"{catalog}.{schema}"
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Table bootstrap
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        """Create control tables if they do not already exist."""

        logger.info("Ensuring control tables exist in '%s'.", self.full_schema)

        # Watermark store – one row per collection, updated via MERGE.
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.full_schema}.watermark_store (
              collection      STRING NOT NULL,
              watermark_value STRING,
              updated_at      TIMESTAMP
            )
            USING DELTA
            """
        )

        # Ingestion audit log – append-only, one row per run.
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.full_schema}.control_ingestion_log (
              _ingestion_id       STRING,
              collection          STRING,
              load_type           STRING,
              watermark_inicial   STRING,
              watermark_final     STRING,
              qtd_lida_origem     LONG,
              qtd_gravada_destino LONG,
              start_time          TIMESTAMP,
              end_time            TIMESTAMP,
              duracao_seg         DOUBLE,
              status              STRING,
              mensagem_erro       STRING
            )
            USING DELTA
            """
        )

        logger.info("Control tables verified / created.")

    # ------------------------------------------------------------------
    # Watermark helpers
    # ------------------------------------------------------------------

    def get_watermark(self, collection: str) -> str | None:
        """
        Return the last saved watermark for *collection*, or ``None``.

        Parameters
        ----------
        collection : str
            Collection name used as the primary key in ``watermark_store``.

        Returns
        -------
        str | None
            The stored watermark string, or ``None`` if no record exists.
        """
        # Usamos .first() em vez de .collect() para não materializar um array no driver.
        # A watermark_store tem no máximo 1 linha por coleção (6 linhas total) —
        # .first() retorna a Row diretamente ou None se o resultado estiver vazio.
        row = (
            self.spark.sql(
                f"""
                SELECT watermark_value
                FROM   {self.full_schema}.watermark_store
                WHERE  collection = '{collection}'
                """
            )
            .first()
        )

        if row is not None:
            watermark = row["watermark_value"]
            logger.info("Watermark for '%s': %s", collection, watermark)
            return watermark

        logger.info("No watermark found for '%s' — full load required.", collection)
        return None

    def save_watermark(self, collection: str, value: str) -> None:
        """
        Upsert the watermark for *collection* using a Delta MERGE.

        Parameters
        ----------
        collection : str
            Collection name (merge key).
        value : str
            New watermark value to persist.
        """
        logger.info("Saving watermark for '%s': %s", collection, value)

        self.spark.sql(
            f"""
            MERGE INTO {self.full_schema}.watermark_store AS target
            USING (
                SELECT
                    '{collection}'        AS collection,
                    '{value}'             AS watermark_value,
                    current_timestamp()   AS updated_at
            ) AS source
            ON target.collection = source.collection
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
            """
        )

        logger.info("Watermark saved for '%s'.", collection)

    # ------------------------------------------------------------------
    # Run logging
    # ------------------------------------------------------------------

    def log_start(
        self,
        run_id: str,
        collection: str,
        load_type: str,
        watermark_ini: str | None,
        start_time: datetime,
    ) -> None:
        """
        Insert an initial ``RUNNING`` row into ``control_ingestion_log``.

        Parameters
        ----------
        run_id : str
            Unique identifier for this ingestion run (UUID).
        collection : str
            Source collection name.
        load_type : str
            ``'FULL'`` or ``'INCREMENTAL'``.
        watermark_ini : str | None
            Watermark value at the start of this run (``None`` for full load).
        start_time : datetime
            UTC timestamp when the run began.
        """
        logger.info(
            "Logging START for run '%s' — collection '%s', type '%s'.",
            run_id,
            collection,
            load_type,
        )

        # Define schema explicitly to guarantee column order and nullability.
        schema = StructType(
            [
                StructField("_ingestion_id", StringType(), True),
                StructField("collection", StringType(), True),
                StructField("load_type", StringType(), True),
                StructField("watermark_inicial", StringType(), True),
                StructField("watermark_final", StringType(), True),
                StructField("qtd_lida_origem", LongType(), True),
                StructField("qtd_gravada_destino", LongType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duracao_seg", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("mensagem_erro", StringType(), True),
            ]
        )

        row = [
            (
                run_id,
                collection,
                load_type,
                watermark_ini,  # watermark_inicial
                None,           # watermark_final  — unknown at start
                None,           # qtd_lida_origem
                None,           # qtd_gravada_destino
                start_time,
                None,           # end_time
                None,           # duracao_seg
                "RUNNING",
                None,           # mensagem_erro
            )
        ]

        (
            self.spark.createDataFrame(row, schema=schema)
            .write
            .mode("append")
            .saveAsTable(f"{self.full_schema}.control_ingestion_log")
        )

        logger.info("START log written for run '%s'.", run_id)

    def log_end(
        self,
        run_id: str,
        collection: str,
        qtd_lida: int,
        qtd_gravada: int,
        status: str,
        watermark_final: str | None,
        start_time: datetime,
        end_time: datetime,
        erro: str | None,
    ) -> None:
        """
        Update the ``RUNNING`` log row to its final state via Delta MERGE.

        Parameters
        ----------
        run_id : str
            Unique identifier matching the row written by :meth:`log_start`.
        collection : str
            Source collection name (secondary key in the MERGE predicate).
        qtd_lida : int
            Number of records read from the source.
        qtd_gravada : int
            Number of records written to Bronze.
        status : str
            Final status string — ``'SUCCESS'`` or ``'ERROR'``.
        watermark_final : str | None
            Watermark value at the end of the run.
        start_time : datetime
            UTC timestamp when the run began (used to compute duration).
        end_time : datetime
            UTC timestamp when the run finished.
        erro : str | None
            Error message if ``status == 'ERROR'``, otherwise ``None``.
        """
        duracao_seg = (end_time - start_time).total_seconds()

        # Represent None / NULL values as SQL-safe literals.
        wf_sql = f"'{watermark_final}'" if watermark_final is not None else "NULL"
        erro_sql = f"'{erro}'" if erro is not None else "NULL"

        logger.info(
            "Logging END for run '%s' — status '%s', duration %.2fs.",
            run_id,
            status,
            duracao_seg,
        )

        self.spark.sql(
            f"""
            MERGE INTO {self.full_schema}.control_ingestion_log AS target
            USING (
                SELECT
                    '{run_id}'          AS _ingestion_id,
                    '{collection}'      AS collection,
                    {qtd_lida}          AS qtd_lida_origem,
                    {qtd_gravada}       AS qtd_gravada_destino,
                    '{status}'          AS status,
                    {wf_sql}            AS watermark_final,
                    current_timestamp() AS end_time,
                    {duracao_seg}       AS duracao_seg,
                    {erro_sql}          AS mensagem_erro
            ) AS source
            ON  target._ingestion_id = source._ingestion_id
            AND target.collection    = source.collection
            WHEN MATCHED THEN
                UPDATE SET
                    target.qtd_lida_origem     = source.qtd_lida_origem,
                    target.qtd_gravada_destino = source.qtd_gravada_destino,
                    target.status              = source.status,
                    target.watermark_final     = source.watermark_final,
                    target.end_time            = source.end_time,
                    target.duracao_seg         = source.duracao_seg,
                    target.mensagem_erro       = source.mensagem_erro
            """
        )

        logger.info("END log written for run '%s'.", run_id)
