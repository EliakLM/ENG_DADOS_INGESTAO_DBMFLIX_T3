# Databricks notebook source
# MAGIC %pip install pymongo
# MAGIC dbutils.library.restartPython()

"""
extractor.py
------------
MongoDB Atlas extractor for the T3-DE-INGESTAO ingestion pipeline.

Reads documents from a MongoDB Atlas cluster and writes them as JSONL files
to the Databricks Landing Zone using plain Python file I/O (no Spark).

Design principles:
  - Cursor is iterated doc-by-doc (never loaded fully into memory via list()).
  - Secrets are resolved at runtime via Databricks dbutils.secrets.
  - All network calls are wrapped with retry_with_backoff for resilience.
  - Each extraction produces a time-stamped, run-scoped JSONL file.
"""

import datetime
import json
import logging
import os
from pathlib import Path

from pymongo import MongoClient, DESCENDING

from src.utils import encode_bson, retry_with_backoff, generate_run_id

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MongoExtractor
# ---------------------------------------------------------------------------

class MongoExtractor:
    """Extract documents from MongoDB Atlas and persist them as JSONL files.

    Parameters
    ----------
    config:
        Dictionary loaded from ``pipeline_config.yaml``.  Expected keys::

            secret_scope: "<databricks-secret-scope>"
            secret_key:   "<secret-key-holding-uri>"
            database:     "<database-name>"
            landing_path: "<dbfs-or-local-landing-zone-path>"

    dbutils:
        The Databricks ``dbutils`` object, passed in explicitly to keep the
        module testable outside of a Databricks cluster environment.
    """

    def __init__(self, config: dict, dbutils):
        """Initialise the extractor: resolve secrets and create MongoClient."""
        self.config = config
        self.database_name: str = config["database"]

        # Resolve the MongoDB Atlas URI from Databricks secret store at
        # runtime — never hard-code credentials in source files.
        scope: str = config["secret_scope"]
        key: str = config["secret_key"]
        mongo_uri: str = dbutils.secrets.get(scope=scope, key=key)

        # A single, long-lived MongoClient is created here and reused across
        # all collection extractions within the same pipeline run.
        self.client = MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=15_000,   # fail fast if cluster unreachable
            socketTimeoutMS=300_000,           # allow slow cursors on large collections
            appName="databricks-mflix-ingestor",
        )
        logger.info(
            "MongoClient initialised — database: '%s'", self.database_name
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(
        self,
        collection_cfg: dict,
        run_id: str,
        watermark: str | None,
    ) -> int:
        """Extract documents from a single collection and write a JSONL file.

        Documents are streamed from MongoDB one-by-one via a cursor and written
        line-by-line to disk — the full result set is **never** loaded into
        memory at once.

        Parameters
        ----------
        collection_cfg:
            Per-collection configuration block from ``pipeline_config.yaml``::

                name:            "<collection-name>"
                load_type:       "full" | "incremental"
                watermark_field: "<field-name>"   # only for incremental
                projection:      {<field>: 1, ...}
                batch_size:      <int>

            ``projection`` and ``batch_size`` are optional; sensible defaults
            are applied when absent.

        run_id:
            Unique identifier for the current pipeline run (UUID v4 string).

        watermark:
            For incremental loads, the maximum value of ``watermark_field``
            from the previous successful run.  Pass ``None`` to perform a
            full (re)load regardless of ``load_type``.

        Returns
        -------
        int
            Number of documents written to the JSONL file.  Returns ``0`` and
            still creates an empty file when the collection/filter returns no
            results.
        """
        collection_name: str = collection_cfg["name"]
        load_type: str = collection_cfg.get("load_type", "full")
        watermark_field: str | None = collection_cfg.get("watermark_field")
        projection: dict | None = collection_cfg.get("projection")
        batch_size: int = collection_cfg.get("batch_size", 1000)

        # ----------------------------------------------------------------
        # Build filter
        # ----------------------------------------------------------------
        if load_type == "incremental" and watermark is not None and watermark_field:
            # Only fetch documents inserted/updated after the last watermark.
            watermark_value = watermark
            if collection_cfg.get("watermark_type") == "datetime" and isinstance(watermark, str):
                watermark_value = datetime.datetime.fromisoformat(
                    watermark.replace("Z", "+00:00")
                )
            doc_filter: dict = {watermark_field: {"$gt": watermark_value}}
            logger.info(
                "[%s] Incremental load — filter: %s", collection_name, doc_filter
            )
        else:
            doc_filter = {}
            logger.info(
                "[%s] Full load — no filter applied.", collection_name
            )

        # ----------------------------------------------------------------
        # Resolve output path
        # ----------------------------------------------------------------
        landing_base: str = self.config["landing_path"]
        timestamp: str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_dir = Path(landing_base) / collection_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{collection_name}_{run_id}_{timestamp}.jsonl"

        # ----------------------------------------------------------------
        # Execute query + stream to JSONL
        # ----------------------------------------------------------------
        count = 0
        try:
            # Wrap the cursor creation (network call) with retry logic.
            # NOTE: We do NOT wrap cursor iteration — MongoDB cursors are
            # stateful and cannot be safely retried mid-stream.
            cursor = retry_with_backoff(
                self.client[self.database_name][collection_name].find,
                filter=doc_filter,
                projection=projection,
                batch_size=batch_size,
            )

            logger.info(
                "[%s] Writing JSONL → %s", collection_name, output_file
            )

            with output_file.open("w", encoding="utf-8") as fh:
                for doc in cursor:
                    # Serialise each document individually; encode_bson handles
                    # BSON-specific types that the standard json module rejects.
                    line: str = json.dumps(doc, default=encode_bson, ensure_ascii=False)
                    fh.write(line + "\n")
                    count += 1

                    # Emit a progress log every 10 000 documents so long-running
                    # extractions remain observable in Databricks logs.
                    if count % 10_000 == 0:
                        logger.info(
                            "[%s] %d documents written so far…", collection_name, count
                        )

            logger.info(
                "[%s] Extraction complete — %d document(s) written to %s.",
                collection_name,
                count,
                output_file,
            )

        except Exception as exc:
            logger.error(
                "[%s] Extraction failed after writing %d document(s). Error: %s",
                collection_name,
                count,
                exc,
                exc_info=True,
            )
            raise

        return count

    def get_max_watermark(
        self,
        collection_cfg: dict,
        doc_filter: dict,
    ) -> str | None:
        """Return the maximum value of the watermark field for the given filter.

        Used after a successful incremental extraction to persist the new high-
        water mark so the next run only fetches newer documents.

        Parameters
        ----------
        collection_cfg:
            Per-collection configuration block (same structure as in
            :meth:`extract`).  Must contain ``watermark_field``.

        doc_filter:
            The MongoDB filter dict that was used during extraction, so that
            the sort query operates over the same document set.

        Returns
        -------
        str or None
            String representation of the maximum watermark value, or ``None``
            if no documents matched *doc_filter* or the field is absent.
        """
        collection_name: str = collection_cfg["name"]
        watermark_field: str | None = collection_cfg.get("watermark_field")

        if not watermark_field:
            logger.warning(
                "[%s] No 'watermark_field' configured — returning None.",
                collection_name,
            )
            return None

        # Fetch only the watermark field from the document with the highest
        # value, using an index-friendly descending sort + limit-1.
        result = retry_with_backoff(
            self.client[self.database_name][collection_name].find_one,
            doc_filter,
            sort=[(watermark_field, DESCENDING)],
            projection={watermark_field: 1},
        )

        if result is None or watermark_field not in result:
            logger.info(
                "[%s] No documents found for watermark query — returning None.",
                collection_name,
            )
            return None

        max_value = result[watermark_field]
        max_value_str = str(max_value)
        logger.info(
            "[%s] Max watermark for field '%s': %s",
            collection_name,
            watermark_field,
            max_value_str,
        )
        return max_value_str

    def close(self) -> None:
        """Close the underlying MongoClient connection pool.

        Should be called in a ``finally`` block after all collections have
        been processed to release network resources promptly.
        """
        self.client.close()
        logger.info("MongoClient connection closed.")
