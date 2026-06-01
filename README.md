# pipeline-obs

**Unified observability schema and collector for multi-cloud data pipelines.**

An open schema (like OpenTelemetry, but for data pipelines) and a lightweight collector that aggregates metrics from Databricks, Delta Live Tables, and dbt into one OTLP-compatible stream — so you can use Grafana, Datadog, Honeycomb, or any OpenTelemetry-native backend to monitor pipeline health, lineage, and cost in one place.

[![CI](https://github.com/stark256-spec/pipeline-obs/actions/workflows/ci.yml/badge.svg)](https://github.com/stark256-spec/pipeline-obs/actions/workflows/ci.yml)
[![OTEP](https://img.shields.io/badge/OTEP-draft-blue)](otep/otep-data-pipeline-semconv.md)

---

## The problem

Every company running data pipelines across Azure + AWS has the same issue: Azure Monitor, Databricks Lineage, dbt docs, and Grafana are silos. There's no:

- Standard span model for pipeline runs across engines
- Unified lineage representation across clouds
- Cost attribution in the same system as latency and error rates
- Cross-engine data quality signal

OpenTelemetry solved this for web services with `http.*` and `db.*` semantic conventions. This project proposes the same for data pipelines: the `pipeline.*` namespace.

---

## Quick start

```bash
pip install pipeline-obs

# Collect from Databricks
export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
export DATABRICKS_TOKEN=dapi-...
pipeline-obs collect databricks --since 24

# Send to your OTLP collector (Grafana Agent, OpenTelemetry Collector, etc.)
pipeline-obs collect databricks --otlp http://localhost:4317

# Import a local dbt run_results.json (no API key)
pipeline-obs dbt-artifacts target/run_results.json --manifest target/manifest.json

# Validate a JSONL file of pipeline runs against the schema
pipeline-obs validate runs.jsonl

# Print the full JSON Schema
pipeline-obs schema-dump
```

---

## Schema: `pipeline.*` semantic conventions

The core of this project is a proposed extension to OpenTelemetry semantic conventions. See [otep/otep-data-pipeline-semconv.md](otep/otep-data-pipeline-semconv.md) for the full proposal.

### A pipeline run span

```json
{
  "name": "pipeline.run",
  "attributes": {
    "pipeline.name":                      "raw_to_bronze.orders",
    "pipeline.run_id":                    "01HZ9XKJQ3WFGP7RBTD6VE4MN",
    "pipeline.engine":                    "databricks",
    "pipeline.type":                      "incremental",
    "pipeline.status":                    "succeeded",
    "pipeline.cloud":                     "azure",
    "pipeline.environment":               "production",
    "pipeline.rows_read":                 2500000,
    "pipeline.rows_written":              2499817,
    "pipeline.cost_usd":                  1.84,
    "pipeline.dbu_consumed":              3.2,
    "pipeline.lineage.sources":           ["abfss://raw@lake.dfs.core.windows.net/orders/"],
    "pipeline.lineage.destinations":      ["main.bronze.orders"],
    "pipeline.quality.rules_passed":      12,
    "pipeline.quality.freshness_lag_seconds": 127
  }
}
```

### Standard metrics

| Metric | Type | Unit |
|---|---|---|
| `pipeline.runs` | Counter | `{run}` |
| `pipeline.run.duration` | Histogram | `s` |
| `pipeline.rows.written` | Counter | `{row}` |
| `pipeline.cost.usd` | Counter | `USD` |
| `pipeline.quality.rules.failed` | Counter | `{rule}` |
| `pipeline.freshness.lag` | Gauge | `s` |

---

## Supported engines

| Engine | Collector | Auth |
|---|---|---|
| Databricks Jobs | `DatabricksCollector` | PAT / Service Principal |
| Databricks Delta Live Tables | `DeltaLiveTablesCollector` | PAT |
| dbt Cloud | `DbtCloudCollector` | API token |
| dbt Core (artifacts) | `DbtArtifactsCollector` | None — reads local files |

More engines (Glue, ADF, Airflow, Pinot) are tracked in [Issues](https://github.com/stark256-spec/pipeline-obs/issues).

---

## Use as a library

```python
from pipeline_obs.collectors.databricks import DatabricksCollector
from pipeline_obs.collectors.dbt import DbtArtifactsCollector
from pipeline_obs.exporters.otel import OtelExporter
from pipeline_obs.schema import PipelineRun

# Collect
collector = DatabricksCollector(host="https://adb-xxx.azuredatabricks.net", token="dapi-...")
runs: list[PipelineRun] = await collector.collect(limit=50)

# Export to OTLP
exporter = OtelExporter(otlp_endpoint="http://localhost:4317")
exporter.export_all(runs)

# Or access the schema directly
for run in runs:
    print(run.name, run.status, run.cost.cost_usd, run.lineage.sources)
    otel_attrs = run.to_otel_attributes()  # flat dict of pipeline.* attributes
```

---

## Grafana dashboard

Import [dashboards/grafana.json](dashboards/grafana.json) into any Grafana instance backed by Prometheus/Mimir. Includes:

- Run count, success rate, total rows, estimated cost (stat panels)
- Run duration p50/p95/p99 (timeseries)
- Runs by status (timeseries)
- Rows written by engine (timeseries)
- Cost by pipeline (timeseries)
- Freshness lag table

---

## OTEP: proposing `pipeline.*` to OpenTelemetry

The [otep/otep-data-pipeline-semconv.md](otep/otep-data-pipeline-semconv.md) document follows the [OpenTelemetry Enhancement Proposal](https://github.com/open-telemetry/oteps) format and proposes the `pipeline.*` semantic convention namespace for inclusion in the official OpenTelemetry specification.

The proposal is being submitted to the [OpenTelemetry Semantic Conventions working group](https://github.com/open-telemetry/community/blob/main/guides/semantic_conventions.md). Discussion welcome — open an issue or comment on the OTEP directly.

---

## Development

```bash
git clone https://github.com/stark256-spec/pipeline-obs
cd pipeline-obs
pip install -e ".[dev]"
pytest tests/ -v
```

---

## License

Apache 2.0 — matching OpenTelemetry's license for maximum compatibility.
