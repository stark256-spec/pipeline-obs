"""OpenTelemetry exporter — emits PipelineRun as OTLP spans and metrics."""

from __future__ import annotations

from datetime import timezone
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace import SpanKind, StatusCode

from pipeline_obs.schema import PipelineRun, PipelineStatus

_RESOURCE_ATTRS = {
    "service.name": "pipeline-obs",
    "service.version": "0.1.0",
    "telemetry.sdk.name": "pipeline-obs",
}


def _ns(dt: Any) -> int | None:
    """Convert datetime to nanoseconds since epoch."""
    if dt is None:
        return None
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1e9)


class OtelExporter:
    """Export PipelineRun objects as OpenTelemetry spans and metrics.

    Usage:
        exporter = OtelExporter()           # console output
        exporter = OtelExporter(otlp_endpoint="http://otelcollector:4317")
    """

    def __init__(
        self,
        *,
        otlp_endpoint: str | None = None,
        service_name: str = "pipeline-obs",
    ) -> None:
        resource = Resource.create({**_RESOURCE_ATTRS, "service.name": service_name})

        # Tracer
        tracer_provider = TracerProvider(resource=resource)
        if otlp_endpoint:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            tracer_provider.add_span_processor(
                SimpleSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint))
            )
        else:
            tracer_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(tracer_provider)
        self._tracer = trace.get_tracer("pipeline-obs")

        # Meter
        if otlp_endpoint:
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
            reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=otlp_endpoint))
        else:
            reader = PeriodicExportingMetricReader(ConsoleMetricExporter())
        meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(meter_provider)
        self._meter = metrics.get_meter("pipeline-obs")

        self._run_counter = self._meter.create_counter(
            "pipeline.runs",
            unit="{run}",
            description="Total pipeline runs by status",
        )
        self._duration_histogram = self._meter.create_histogram(
            "pipeline.run.duration",
            unit="s",
            description="Pipeline run duration in seconds",
        )
        self._rows_counter = self._meter.create_counter(
            "pipeline.rows.written",
            unit="{row}",
            description="Total rows written",
        )
        self._cost_counter = self._meter.create_counter(
            "pipeline.cost.usd",
            unit="USD",
            description="Total estimated cost in USD",
        )
        self._quality_failures = self._meter.create_counter(
            "pipeline.quality.rules.failed",
            unit="{rule}",
            description="Total quality rules failed",
        )

    def export(self, run: PipelineRun) -> None:
        """Export a single PipelineRun as a span + metrics."""
        attrs = run.to_otel_attributes()

        # Span
        start_ns = _ns(run.started_at)
        end_ns = _ns(run.finished_at) if run.finished_at else None

        with self._tracer.start_as_current_span(
            f"pipeline.run {run.name}",
            kind=SpanKind.INTERNAL,
            start_time=start_ns,
            attributes=attrs,
        ) as span:
            if run.status == PipelineStatus.FAILED:
                span.set_status(StatusCode.ERROR, run.error_message or "pipeline failed")
            else:
                span.set_status(StatusCode.OK)

            if end_ns:
                span.end(end_time=end_ns)

        # Metrics
        label_attrs = {
            "pipeline.name": run.name,
            "pipeline.engine": run.engine.value,
            "pipeline.status": run.status.value,
            "pipeline.environment": run.environment,
        }

        self._run_counter.add(1, label_attrs)

        if run.duration_seconds is not None:
            self._duration_histogram.record(run.duration_seconds, label_attrs)

        if run.io.rows_written:
            self._rows_counter.add(run.io.rows_written, label_attrs)

        if run.cost.cost_usd:
            self._cost_counter.add(run.cost.cost_usd, label_attrs)

        if run.quality.rules_failed:
            self._quality_failures.add(run.quality.rules_failed, label_attrs)

    def export_all(self, runs: list[PipelineRun]) -> None:
        for run in runs:
            self.export(run)
