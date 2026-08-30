# =============================================================================
# tests/test_extractor.py
# Testes unitários de src/extractor.py
# Responsável: Raul Teles
# =============================================================================
import datetime
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, "/Workspace/Repos/<SEU-USER>/T3-DE-INGESTAO")

# Mock bson e pymongo para rodar fora do Databricks
bson_mock = MagicMock()
bson_mock.ObjectId = str
bson_mock.Decimal128 = str
sys.modules["bson"] = bson_mock
sys.modules["pymongo"] = MagicMock()
sys.modules["pymongo.errors"] = MagicMock()


class TestMongoExtractor:
    """Testa MongoExtractor com MongoClient mockado."""

    def _make_config(self, landing_path: str) -> dict:
        return {
            "catalog": "mflix_catalog",
            "bronze_schema": "bronze",
            "landing_path": landing_path,
            "checkpoint_base": f"{landing_path}/_checkpoints",
            "secret_scope": "conn-db",
            "secret_key": "cnn-mongodb-sampleflix",
            "database": "sample_mflix",
            "max_retries": 1,
            "retry_base_delay": 0.01,
        }

    def _make_collection_cfg(
        self, name="movies", load_type="full", watermark_field=None, watermark_type=None
    ):
        return {
            "name": name,
            "load_type": load_type,
            "watermark_field": watermark_field,
            "watermark_type": watermark_type,
            "projection": {"fullplot": 0},
            "batch_size": 100,
        }

    def test_extract_writes_jsonl_and_returns_count(self):
        """Extração deve escrever JSONL na landing e retornar a contagem correta."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._make_config(tmpdir)
            col_cfg = self._make_collection_cfg("movies")
            run_id = "test-run-001"

            # Mock docs retornados pelo cursor
            mock_docs = [
                {"_id": "id1", "title": "Movie A"},
                {"_id": "id2", "title": "Movie B"},
                {"_id": "id3", "title": "Movie C"},
            ]

            mock_cursor = iter(mock_docs)  # cursor real é um iterável
            mock_collection = MagicMock()
            mock_collection.find.return_value = mock_cursor
            mock_db = MagicMock()
            mock_db.__getitem__ = lambda self, key: mock_collection
            mock_client = MagicMock()
            mock_client.__getitem__ = lambda self, key: mock_db

            dbutils_mock = MagicMock()
            dbutils_mock.secrets.get.return_value = "mongodb://fake-uri"

            from src.extractor import MongoExtractor
            with patch("src.extractor.MongoClient", return_value=mock_client):
                extractor = MongoExtractor(config=config, dbutils=dbutils_mock)
                count = extractor.extract(collection_cfg=col_cfg, run_id=run_id, watermark=None)

            assert count == 3

            # Verifica que o arquivo JSONL foi criado
            landing_dir = os.path.join(tmpdir, "movies")
            files = os.listdir(landing_dir)
            assert len(files) == 1
            assert files[0].endswith(".jsonl")

            # Verifica conteúdo do JSONL
            with open(os.path.join(landing_dir, files[0])) as f:
                lines = f.readlines()
            assert len(lines) == 3
            for line in lines:
                doc = json.loads(line)
                assert "_id" in doc

    def test_extract_handles_empty_collection(self):
        """Coleção vazia deve retornar 0 sem exceção."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._make_config(tmpdir)
            col_cfg = self._make_collection_cfg("sessions")
            run_id = "test-run-002"

            mock_cursor = iter([])  # cursor vazio
            mock_collection = MagicMock()
            mock_collection.find.return_value = mock_cursor
            mock_db = MagicMock()
            mock_db.__getitem__ = lambda self, key: mock_collection
            mock_client = MagicMock()
            mock_client.__getitem__ = lambda self, key: mock_db

            dbutils_mock = MagicMock()
            dbutils_mock.secrets.get.return_value = "mongodb://fake-uri"

            from src.extractor import MongoExtractor
            with patch("src.extractor.MongoClient", return_value=mock_client):
                extractor = MongoExtractor(config=config, dbutils=dbutils_mock)
                count = extractor.extract(collection_cfg=col_cfg, run_id=run_id, watermark=None)

            assert count == 0

    def test_extract_never_calls_list_on_cursor(self):
        """Garante que list(cursor) nunca é chamado — exigência de boas práticas R2."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._make_config(tmpdir)
            col_cfg = self._make_collection_cfg()
            run_id = "test-run-003"

            # Cursor que rastreia se list() foi chamado
            class TrackedCursor:
                def __init__(self, docs):
                    self.docs = docs
                    self.list_called = False
                def __iter__(self):
                    return iter(self.docs)
                def __len__(self):
                    self.list_called = True  # list() chama __len__ em alguns contextos
                    return len(self.docs)

            cursor = TrackedCursor([{"_id": "id1", "title": "X"}])
            mock_collection = MagicMock()
            mock_collection.find.return_value = cursor
            mock_db = MagicMock()
            mock_db.__getitem__ = lambda self, key: mock_collection
            mock_client = MagicMock()
            mock_client.__getitem__ = lambda self, key: mock_db

            dbutils_mock = MagicMock()
            dbutils_mock.secrets.get.return_value = "mongodb://fake-uri"

            from src.extractor import MongoExtractor
            with patch("src.extractor.MongoClient", return_value=mock_client):
                extractor = MongoExtractor(config=config, dbutils=dbutils_mock)
                extractor.extract(collection_cfg=col_cfg, run_id=run_id, watermark=None)

            assert not cursor.list_called, "list(cursor) foi chamado — violação de R2!"

    def test_incremental_filter_uses_watermark(self):
        """Extração incremental deve passar filtro com $gt na watermark."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._make_config(tmpdir)
            col_cfg = self._make_collection_cfg(
                "comments", "incremental", "date", "datetime"
            )
            run_id = "test-run-004"
            watermark = "2024-01-01T00:00:00"

            mock_cursor = iter([])
            mock_collection = MagicMock()
            mock_collection.find.return_value = mock_cursor
            mock_db = MagicMock()
            mock_db.__getitem__ = lambda self, key: mock_collection
            mock_client = MagicMock()
            mock_client.__getitem__ = lambda self, key: mock_db

            dbutils_mock = MagicMock()
            dbutils_mock.secrets.get.return_value = "mongodb://fake-uri"

            from src.extractor import MongoExtractor
            with patch("src.extractor.MongoClient", return_value=mock_client):
                extractor = MongoExtractor(config=config, dbutils=dbutils_mock)
                extractor.extract(collection_cfg=col_cfg, run_id=run_id, watermark=watermark)

            # Verifica que find() foi chamado com o filtro correto
            find_call_kwargs = mock_collection.find.call_args
            filter_arg = find_call_kwargs[1].get("filter") or find_call_kwargs[0][0]
            assert "date" in filter_arg
            assert "$gt" in filter_arg["date"]
            assert isinstance(filter_arg["date"]["$gt"], datetime.datetime)

    def test_close_closes_mongo_client(self):
        """close() deve encerrar o MongoClient."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._make_config(tmpdir)
            mock_client = MagicMock()
            dbutils_mock = MagicMock()
            dbutils_mock.secrets.get.return_value = "mongodb://fake-uri"

            from src.extractor import MongoExtractor
            with patch("src.extractor.MongoClient", return_value=mock_client):
                extractor = MongoExtractor(config=config, dbutils=dbutils_mock)
                extractor.close()

            mock_client.close.assert_called_once()
