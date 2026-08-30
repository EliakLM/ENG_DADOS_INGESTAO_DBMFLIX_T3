"""
loader.py
---------
Reads JSONL files from the landing zone using Auto Loader (Structured Streaming)
and writes them to Bronze Delta tables with traceability metadata columns.
"""

import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import coalesce, col, current_date, current_timestamp, lit

logger = logging.getLogger(__name__)


class BronzeLoader:
    """
    Loads raw data from the landing zone into Bronze Delta tables.

    Uses Databricks Auto Loader (``cloudFiles`` format) with
    ``trigger(availableNow=True)`` so the streaming job processes all
    available files and terminates — behaving like a batch run while
    still benefiting from Auto Loader's incremental file tracking.
    """

    def __init__(self, spark: SparkSession, config: dict) -> None:
        """
        Initialize the loader from the pipeline configuration.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        config : dict
            Dictionary loaded from ``pipeline_config.yaml``.  Expected keys:

            - ``catalog``         — Unity Catalog name.
            - ``bronze_schema``   — Schema that holds Bronze tables.
            - ``landing_path``    — Root path of the landing zone (DBFS / ADLS).
            - ``checkpoint_base`` — Root path for Auto Loader checkpoints.
        """
        self.spark = spark
        self.config = config
        self.catalog = config["catalog"]
        self.schema = config["bronze_schema"]
        self.landing_path = config["landing_path"]
        self.checkpoint_base = config["checkpoint_base"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, collection: str, run_id: str, load_type: str) -> int:
        """
        Ingest one collection from the landing zone into its Bronze table.

        The method:
        1. Configures Auto Loader to read JSONL files with schema inference.
        2. Adds traceability columns (run ID, timestamps, source path, etc.).
        3. Writes to a partitioned Delta table using ``availableNow`` trigger.
        4. Counts and returns the number of rows written in this run.

        Parameters
        ----------
        collection : str
            Collection name (e.g. ``'movies'``). Used to derive source path,
            checkpoint path, and target table name.
        run_id : str
            UUID identifying this ingestion run; stored in ``_ingestion_id``.
        load_type : str
            ``'FULL'`` or ``'INCREMENTAL'``; stored in ``_load_type``.

        Returns
        -------
        int
            Number of records written to the Bronze table in this run.
        """
        source_path = f"{self.landing_path}/{collection}/"
        checkpoint_path = f"{self.checkpoint_base}/{collection}"
        target_table = f"{self.catalog}.{self.schema}.{collection}"

        logger.info(
            "Starting Auto Loader ingestion — collection '%s', run '%s', type '%s'.",
            collection,
            run_id,
            load_type,
        )
        logger.info("  source_path    : %s", source_path)
        logger.info("  checkpoint_path: %s", checkpoint_path)
        logger.info("  target_table   : %s", target_table)

        # ---------------------------------------------------------------
        # 1. Read with Auto Loader (streaming source).
        #    - schemaEvolutionMode='addNewColumns' handles new fields without
        #      failing; the target table schema is merged on write.
        #    - includeExistingFiles='true' ensures a clean run always picks up
        #      every file already present in the landing zone.
        # ---------------------------------------------------------------
        df = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.includeExistingFiles", "true")
            .load(source_path)
        )

        # ---------------------------------------------------------------
        # 2. Add traceability / metadata columns (R4 requirement).
        #    _source_id falls back to 'UNKNOWN' when the raw _id is absent.
        # ---------------------------------------------------------------
        df = (
            df
            .withColumn("_ingestion_id", lit(run_id))
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn(
                "_source_path",
                lit(f"mongodb_atlas/sample_mflix/{collection}"),
            )
            .withColumn("_load_type", lit(load_type))
            .withColumn("_ingestion_date", current_date())
            .withColumn(
                "_source_id",
                coalesce(col("_id").cast("string"), lit("UNKNOWN")),
            )
        )

        # ---------------------------------------------------------------
        # 3. Write to Delta using availableNow trigger.
        #    - availableNow=True processes all pending files then stops,
        #      giving batch semantics with Auto Loader's incremental tracking.
        #    - mergeSchema=true propagates any schema evolution to the table.
        #    - Partitioning by _ingestion_date keeps file sizes manageable.
        # ---------------------------------------------------------------
        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{checkpoint_path}/ckpt")
            .option("mergeSchema", "true")
            .partitionBy("_ingestion_date")
            .trigger(availableNow=True)
            .toTable(target_table)
        )

        # Block until all micro-batches for this trigger have been processed.
        query.awaitTermination()

        logger.info(
            "Streaming query finished for collection '%s'.", collection
        )

        # ---------------------------------------------------------------
        # 4. Count rows written in this specific run for the audit log.
        # ---------------------------------------------------------------
        count = (
            self.spark.table(target_table)
            .filter(col("_ingestion_id") == run_id)
            .count()
        )

        logger.info(
            "Rows written to '%s' for run '%s': %d", target_table, run_id, count
        )

        return count
