"""
shared/observability/tracing.py
OpenTelemetry setup for all services.
"""
from __future__ import annotations

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor


def configure_tracing(service_name: str, service_version: str = "1.0.0") -> None:
    """Initialize OpenTelemetry with OTLP exporter."""
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": service_version,
            "deployment.environment": os.environ.get("ENVIRONMENT", "local"),
        }
    )

    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    if otlp_endpoint:
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    else:
        # Fallback to console in dev
        exporter = ConsoleSpanExporter()

    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # Auto-instrument HTTP and DB (imports are conditional since not all
    # services install every instrumentation package).
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        HTTPXClientInstrumentor().instrument()
    except ImportError:
        pass
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        SQLAlchemyInstrumentor().instrument()
    except ImportError:
        pass


def instrument_fastapi(app) -> None:
    """Apply FastAPI auto-instrumentation."""
    FastAPIInstrumentor.instrument_app(app)
