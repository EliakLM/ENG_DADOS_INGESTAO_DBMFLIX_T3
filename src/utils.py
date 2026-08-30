"""
utils.py
--------
Shared utility functions for the T3-DE-INGESTAO ingestion pipeline.

Provides:
  - BSON/MongoDB type serialisation for JSON output
  - Exponential-backoff retry wrapper
  - Pipeline run-ID generation
  - SHA-256 document hashing
"""

import datetime
import hashlib
import logging
import random
import time
import uuid

import bson

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# BSON / MongoDB → JSON serialisation
# ---------------------------------------------------------------------------

def encode_bson(o):
    """Convert BSON/MongoDB types to JSON-serialisable Python types.

    Intended for use as the ``default`` argument to :func:`json.dumps`.

    Conversion rules:
    - :class:`bson.ObjectId`                → ``str``
    - :class:`datetime.datetime` or
      :class:`datetime.date`                → ISO 8601 string
    - :class:`bson.Decimal128`              → ``str``
    - :class:`bytes`                        → hexadecimal string
    - anything else                         → ``str(o)`` (safe fallback)

    Parameters
    ----------
    o:
        The object that :func:`json.dumps` could not serialise natively.

    Returns
    -------
    str
        A JSON-serialisable representation of *o*.
    """
    if isinstance(o, bson.ObjectId):
        return str(o)

    if isinstance(o, (datetime.datetime, datetime.date)):
        # datetime.datetime must be checked first because it is a subclass
        # of datetime.date; isoformat() produces the correct ISO 8601 string
        # for both types.
        return o.isoformat()

    if isinstance(o, bson.Decimal128):
        return str(o)

    if isinstance(o, bytes):
        # Hex is safer than base64 when downstream consumers expect strings.
        return o.hex()

    # Safe fallback: unknown types are stringified so serialisation never fails.
    return str(o)


# ---------------------------------------------------------------------------
# Retry with exponential back-off
# ---------------------------------------------------------------------------

def retry_with_backoff(fn, *args, max_retries: int = 3, base_delay: float = 2.0, **kwargs):
    """Call *fn* with retry logic and exponential back-off.

    Retries up to *max_retries* times on any :class:`Exception`.  Between
    attempts the function sleeps for ``base_delay * 2^attempt`` seconds plus
    a random jitter in ``[0, 1)`` seconds to avoid thundering-herd problems.

    Parameters
    ----------
    fn:
        Callable to invoke.
    *args:
        Positional arguments forwarded to *fn*.
    max_retries:
        Maximum number of retry attempts (default: 3).
        Total calls = max_retries + 1.
    base_delay:
        Base delay in seconds for the exponential formula (default: 2.0).
    **kwargs:
        Keyword arguments forwarded to *fn*.

    Returns
    -------
    Any
        The return value of *fn* on success.

    Raises
    ------
    Exception
        Re-raises the **last** exception after all retries are exhausted.
    """
    last_exc = None

    for attempt in range(max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc

            if attempt == max_retries:
                # All attempts exhausted — propagate the error to the caller.
                logger.error(
                    "All %d attempt(s) failed for '%s'. Last error: %s",
                    max_retries + 1,
                    getattr(fn, "__name__", repr(fn)),
                    exc,
                )
                raise

            # Exponential back-off: 2^attempt * base_delay + jitter
            delay = base_delay * (2 ** attempt) + random.random()
            logger.warning(
                "Attempt %d/%d failed for '%s' — retrying in %.2fs. Error: %s",
                attempt + 1,
                max_retries + 1,
                getattr(fn, "__name__", repr(fn)),
                delay,
                exc,
            )
            time.sleep(delay)

    # This line is unreachable but satisfies type-checkers.
    raise last_exc


# ---------------------------------------------------------------------------
# Pipeline run-ID generation
# ---------------------------------------------------------------------------

def generate_run_id() -> str:
    """Return a new UUID v4 string to uniquely identify a pipeline run.

    Returns
    -------
    str
        A UUID v4 in canonical hyphenated form, e.g.
        ``'550e8400-e29b-41d4-a716-446655440000'``.
    """
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Document hashing
# ---------------------------------------------------------------------------

def hash_document(doc_str: str) -> str:
    """Return the SHA-256 hex digest of *doc_str*.

    Useful for detecting duplicate or unchanged documents without storing
    the full document content.

    Parameters
    ----------
    doc_str:
        A JSON string representation of the document.

    Returns
    -------
    str
        64-character lowercase hexadecimal SHA-256 digest.
    """
    return hashlib.sha256(doc_str.encode("utf-8")).hexdigest()
