"""
Structured logging setup using structlog.

Every request handler should call `get_logger()` and attach contextual
fields (request_id, object_key, sql_query_id) so that the full chain
from Fabric object read → SQL execution → Parquet response is traceable.
"""
from __future__ import annotations

import logging
import os
import sys

import structlog

from observability.logbuffer import RingBufferHandler, capture_renderer


class _Suppress404AccessFilter(logging.Filter):
    """Drop uvicorn access-log records for 404 responses.

    In this proxy every 404 is an expected S3 probe: Fabric's ADLS-over-S3 shim
    checks whether each key is a folder by requesting ``key/`` (trailing slash)
    and HEADing folder prefixes, and it probes for optional format markers such
    as ``_metadata/table.json.gz``. None of these are errors, so they only add
    noise. Genuine failures surface as 5xx or as higher-level structured logs.
    Set ``QUIET_404_LOGS=0`` to see every 404 again.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            args = record.args
            if args and len(args) >= 5 and args[4] == 404:
                return False
        except Exception:
            pass
        return True


def configure_logging(level: str = "INFO") -> None:
    """Call once at application startup."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    # Silence the flood of expected-probe 404s in the uvicorn access log unless
    # explicitly disabled.
    if os.environ.get("QUIET_404_LOGS", "1").lower() not in ("0", "false", "no"):
        logging.getLogger("uvicorn.access").addFilter(_Suppress404AccessFilter())

    # Mirror stdlib logs into the rolling in-memory buffer (monitor log viewer).
    root = logging.getLogger()
    if not any(isinstance(h, RingBufferHandler) for h in root.handlers):
        ring = RingBufferHandler()
        ring.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        root.addHandler(ring)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            capture_renderer(structlog.dev.ConsoleRenderer()),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = __name__) -> structlog.BoundLogger:
    return structlog.get_logger(name)
