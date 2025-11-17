"""
OpenTelemetry instrumentation for observability.
"""

from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter


def setup_observability(service_name: str = "cleaning-engine", console_export: bool = True) -> None:
    """
    Configure OpenTelemetry tracing.

    Args:
        service_name: Name of the service for tracing
        console_export: Whether to export traces to console (useful for dev/testing)
    """
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    if console_export:
        # Export to console for development/debugging
        console_exporter = ConsoleSpanExporter()
        provider.add_span_processor(BatchSpanProcessor(console_exporter))

    # Set as global default
    trace.set_tracer_provider(provider)


def get_tracer(name: str) -> trace.Tracer:
    """
    Get a tracer instance.

    Args:
        name: Name for the tracer (typically __name__)

    Returns:
        Tracer instance
    """
    return trace.get_tracer(name)
