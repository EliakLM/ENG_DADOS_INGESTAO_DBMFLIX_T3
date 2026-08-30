# =============================================================================
# tests/test_utils.py
# Testes unitários de src/utils.py
# Responsável: Raul Teles
# =============================================================================
import datetime
import hashlib
import sys
import time
from unittest.mock import patch, MagicMock

import pytest

# Garante que src/ está no path
sys.path.insert(0, "/Workspace/Repos/<SEU-USER>/T3-DE-INGESTAO")

# bson pode não estar disponível fora do Databricks — usamos mock
bson_mock = MagicMock()
bson_mock.ObjectId = type("ObjectId", (), {"__str__": lambda self: "507f1f77bcf86cd799439011"})
bson_mock.Decimal128 = type("Decimal128", (), {"__str__": lambda self: "3.14"})
sys.modules["bson"] = bson_mock

from src.utils import encode_bson, retry_with_backoff, generate_run_id, hash_document


class TestEncodeBson:
    """Testa a conversão de tipos BSON para tipos JSON-serializáveis."""

    def test_objectid_returns_string(self):
        obj_id = bson_mock.ObjectId()
        result = encode_bson(obj_id)
        assert isinstance(result, str)

    def test_datetime_returns_iso_string(self):
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
        result = encode_bson(dt)
        assert result == "2024-01-15T10:30:00"
        assert isinstance(result, str)

    def test_date_returns_iso_string(self):
        d = datetime.date(2024, 1, 15)
        result = encode_bson(d)
        assert "2024-01-15" in result

    def test_decimal128_returns_string(self):
        dec = bson_mock.Decimal128()
        result = encode_bson(dec)
        assert isinstance(result, str)

    def test_bytes_returns_hex(self):
        b = bytes([0xDE, 0xAD, 0xBE, 0xEF])
        result = encode_bson(b)
        assert result == "deadbeef"

    def test_unknown_type_returns_str(self):
        class Weird:
            def __str__(self):
                return "weird_value"
        result = encode_bson(Weird())
        assert result == "weird_value"


class TestRetryWithBackoff:
    """Testa a lógica de retry com exponential backoff."""

    def test_succeeds_on_first_try(self):
        fn = MagicMock(return_value=42)
        result = retry_with_backoff(fn, max_retries=3, base_delay=0.01)
        assert result == 42
        fn.assert_called_once()

    def test_retries_on_failure_then_succeeds(self):
        fn = MagicMock(side_effect=[Exception("fail"), Exception("fail"), 99])
        result = retry_with_backoff(fn, max_retries=3, base_delay=0.01)
        assert result == 99
        assert fn.call_count == 3

    def test_raises_after_max_retries(self):
        fn = MagicMock(side_effect=ConnectionError("sempre falha"))
        with pytest.raises(ConnectionError, match="sempre falha"):
            retry_with_backoff(fn, max_retries=3, base_delay=0.01)
        assert fn.call_count == 4

    def test_passes_args_and_kwargs(self):
        fn = MagicMock(return_value="ok")
        retry_with_backoff(fn, "arg1", max_retries=1, base_delay=0.01, kwarg1="val")
        fn.assert_called_once_with("arg1", kwarg1="val")


class TestGenerateRunId:
    """Testa a geração de UUIDs únicos."""

    def test_returns_string(self):
        result = generate_run_id()
        assert isinstance(result, str)

    def test_has_uuid_format(self):
        import re
        result = generate_run_id()
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        assert re.match(uuid_pattern, result), f"'{result}' não é UUID v4 válido"

    def test_is_unique(self):
        ids = {generate_run_id() for _ in range(100)}
        assert len(ids) == 100, "Colisão de UUIDs detectada"


class TestHashDocument:
    """Testa a geração de hash SHA-256 de documentos."""

    def test_returns_string(self):
        result = hash_document('{"key": "value"}')
        assert isinstance(result, str)

    def test_same_input_same_hash(self):
        doc = '{"_id": "abc", "title": "Test"}'
        assert hash_document(doc) == hash_document(doc)

    def test_different_inputs_different_hashes(self):
        assert hash_document('{"a": 1}') != hash_document('{"a": 2}')

    def test_correct_sha256(self):
        doc = "hello"
        expected = hashlib.sha256(doc.encode()).hexdigest()
        assert hash_document(doc) == expected
