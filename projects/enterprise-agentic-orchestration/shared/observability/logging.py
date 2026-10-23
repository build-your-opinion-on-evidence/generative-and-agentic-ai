"""
shared/observability/logging.py
Structured JSON logging setup using structlog.
All services must call configure_logging() at startup.
"""
from __future__ import annotations

import logging
import os
import sys

import structlog


def configure_logging(service_name: str, service_version: str = "1.0.0") -> None:
    """Configure structlog for JSON output with standard fields."""
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.stdlib.ExtraAdder(),
        # Inject standard service fields
        _inject_service_fields(service_name, service_version),
    ]

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)

    # Reduce noisy library loggers
    for lib in ("uvicorn.access", "httpx", "confluent_kafka"):
        logging.getLogger(lib).setLevel(logging.WARNING)


def _inject_service_fields(service_name: str, service_version: str):
    env = os.environ.get("ENVIRONMENT", "local")

    def processor(logger, method, event_dict):
        event_dict.setdefault("service_name", service_name)
        event_dict.setdefault("service_version", service_version)
        event_dict.setdefault("environment", env)
        return event_dict

    return processor
