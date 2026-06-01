# OTEP: Semantic Conventions for Data Pipeline Observability

**Status:** Draft  
**Author:** stark256-spec  
**Created:** 2026-06-01  
**OpenTelemetry Working Group:** Semantic Conventions  
**Related issues:** [open-telemetry/semantic-conventions#1316](https://github.com/open-telemetry/semantic-conventions/issues/1316) (database semantic conventions)

---

## Motivation

Data pipelines are a core workload in modern cloud infrastructure. Organizations running pipelines on Databricks, Apache Spark, dbt, AWS Glue, Azure Data Factory, Apache Airflow, and Pinot face a fragmented observability landscape:

- **No standard span model** — each engine emits proprietary events in incompatible schemas.
- **No unified lineage representation** — source/destination datasets use different naming conventions across clouds and engines.
- **No standard cost attribution** — compute costs (Databricks Units, slot-ms, node-seconds) are engine-specific.
- **No cross-engine quality signal** — dbt tests, Great Expectations checks, and Databricks DQS all produce incompatible quality events.

This means teams operating multi-cloud or multi-engine pipelines cannot:
- Correlate a slow dashboard with the upstream pipeline that produced its data
- Set a single SLO across pipelines regardless of engine
- Attribute infrastructure cost to logical business domains
- Build unified alerting on pipeline failure across clouds

OpenTelemetry is the obvious home for this standard. It already defines conventions for databases ([`db.*`](https://opentelemetry.io/docs/specs/semconv/database/)), messaging systems ([`messaging.*`](https://opentelemetry.io/docs/specs/semconv/messaging/)), and FaaS ([`faas.*`](https://opentelemetry.io/docs/specs/semconv/faas/)). Data pipelines are the missing piece.

---

## Proposed Namespace: `pipeline.*`

We propose a new top-level namespace `pipeline.*` for spans, metrics, and events emitted during the execution of data pipeline workloads.

### Design Principles

1. **Engine-agnostic** — attributes must be expressible for any pipeline engine.
2. **Additive** — engines may emit additional attributes without conflicting with the standard.
3. **Lineage-first** — dataset URIs are a first-class concept, not an afterthought.
4. **Cost-aware** — cost attribution is a standard field, not an opt-in extra.
5. **Compatible with existing conventions** — re-use `db.*`, `cloud.*`, `service.*` where they apply; do not duplicate.

---

## Span: `pipeline.run`

A `pipeline.run` span represents a single execution of a pipeline. It maps to a Databricks Job Run, a dbt invocation, a Glue Job run, an Airflow DAG run, etc.

### Required Attributes

| Attribute | Type | Description | Examples |
|---|---|---|---|
| `pipeline.name` | string | Stable logical name of the pipeline | `"raw_to_bronze.orders"`, `"dbt.orders_daily"` |
| `pipeline.run_id` | string | Unique execution ID | `"run-20240601-abc123"` |
| `pipeline.engine` | string enum | Engine that executed this run | `"databricks"`, `"dbt"`, `"spark"` |
| `pipeline.status` | string enum | Terminal status | `"succeeded"`, `"failed"`, `"cancelled"` |

### Recommended Attributes

| Attribute | Type | Description | Examples |
|---|---|---|---|
| `pipeline.type` | string enum | Execution mode | `"batch"`, `"streaming"`, `"incremental"` |
| `pipeline.cloud` | string enum | Cloud provider | `"aws"`, `"azure"`, `"gcp"` |
| `pipeline.environment` | string | Deployment environment | `"production"`, `"staging"` |
| `pipeline.rows_read` | int | Total rows read from all sources | `1_500_000` |
| `pipeline.rows_written` | int | Total rows written to destinations | `1_498_322` |
| `pipeline.lineage.sources` | string[] | Input dataset URIs | `["catalog.bronze.raw_orders"]` |
| `pipeline.lineage.destinations` | string[] | Output dataset URIs | `["catalog.silver.orders"]` |

### Opt-in Attributes

| Attribute | Type | Description |
|---|---|---|
| `pipeline.region` | string | Cloud region |
| `pipeline.owner` | string | Team or individual responsible |
| `pipeline.tags` | string[] | Free-form grouping labels |
| `pipeline.bytes_read` | int | Bytes read (uncompressed) |
| `pipeline.bytes_written` | int | Bytes written (uncompressed) |
| `pipeline.rows_failed` | int | Rows rejected by quality checks |
| `pipeline.cost_usd` | double | Estimated cost in USD |
| `pipeline.dbu_consumed` | double | Databricks Units (Databricks-specific) |
| `pipeline.compute_seconds` | int | VM-seconds of compute used |
| `pipeline.lineage.schema_version` | string | Schema version of the output |
| `pipeline.quality.rules_passed` | int | Data quality rules that passed |
| `pipeline.quality.rules_failed` | int | Data quality rules that failed |
| `pipeline.quality.null_rate` | double | Fraction of null rows (0.0–1.0) |
| `pipeline.quality.freshness_lag_seconds` | int | Lag between event time and completion |

---

## Dataset URI Convention

A persistent challenge in data lineage is inconsistent dataset naming across engines. We propose the following conventions for `pipeline.lineage.sources` and `pipeline.lineage.destinations`:

| Platform | URI Format | Example |
|---|---|---|
| Unity Catalog / Iceberg | `{catalog}.{schema}.{table}` | `main.silver.orders` |
| Hive / Spark | `{database}.{table}` | `default.raw_events` |
| AWS S3 | `s3://{bucket}/{prefix}` | `s3://datalake/raw/events/` |
| Azure ADLS | `abfss://{container}@{account}.dfs.core.windows.net/{path}` | `abfss://bronze@myaccount.dfs.core.windows.net/orders/` |
| GCS | `gs://{bucket}/{prefix}` | `gs://datalake/raw/` |
| Kafka | `kafka://{cluster}/{topic}` | `kafka://prod-cluster/clickstream` |

Implementations should use the most specific identifier available. A Unity Catalog three-part name is preferred over a raw S3 path when both are known.

---

## Standard Metrics

The following OTLP metric names are defined under this proposal. All carry at minimum `pipeline.name`, `pipeline.engine`, and `pipeline.environment` as resource/dimension attributes.

| Metric Name | Type | Unit | Description |
|---|---|---|---|
| `pipeline.runs` | Counter | `{run}` | Total runs, labeled by `pipeline.status` |
| `pipeline.run.duration` | Histogram | `s` | Run duration in seconds |
| `pipeline.rows.written` | Counter | `{row}` | Rows written per run |
| `pipeline.cost.usd` | Counter | `USD` | Cumulative cost in USD |
| `pipeline.quality.rules.failed` | Counter | `{rule}` | Quality rules that failed |
| `pipeline.freshness.lag` | Gauge | `s` | Current data freshness lag |

---

## Relationship to Existing Conventions

| Convention | Relationship |
|---|---|
| `db.*` | Use for the underlying database operation within a pipeline task (e.g., a SQL query). `pipeline.run` wraps zero or more `db.*` spans. |
| `cloud.*` | Use resource attributes `cloud.provider`, `cloud.region` instead of `pipeline.cloud`, `pipeline.region` where possible. The pipeline-specific fields are retained for simpler filtering in environments that do not set resource attributes. |
| `service.*` | The data pipeline system (e.g., Databricks workspace, dbt Cloud account) is the `service`. |
| `faas.*` | Serverless pipeline tasks (Glue, Lambda-based pipelines) may combine `faas.*` with `pipeline.*`. |

---

## Examples

### Databricks Job Run

```json
{
  "name": "pipeline.run",
  "kind": "SPAN_KIND_INTERNAL",
  "startTimeUnixNano": "1717228800000000000",
  "endTimeUnixNano":   "1717232400000000000",
  "attributes": {
    "pipeline.name": "raw_to_bronze.orders",
    "pipeline.run_id": "01HZ9XKJQ3WFGP7RBTD6VE4MN",
    "pipeline.engine": "databricks",
    "pipeline.type": "incremental",
    "pipeline.status": "succeeded",
    "pipeline.cloud": "azure",
    "pipeline.region": "eastus2",
    "pipeline.environment": "production",
    "pipeline.rows_read": 2500000,
    "pipeline.rows_written": 2499817,
    "pipeline.rows_failed": 183,
    "pipeline.bytes_written": 812450816,
    "pipeline.cost_usd": 1.84,
    "pipeline.dbu_consumed": 3.2,
    "pipeline.lineage.sources": ["abfss://raw@datalake.dfs.core.windows.net/orders/"],
    "pipeline.lineage.destinations": ["main.bronze.orders"],
    "pipeline.lineage.schema_version": "v4",
    "pipeline.quality.rules_passed": 12,
    "pipeline.quality.rules_failed": 0,
    "pipeline.quality.freshness_lag_seconds": 127
  }
}
```

### dbt Model Run

```json
{
  "name": "pipeline.run",
  "attributes": {
    "pipeline.name": "dbt.orders_daily",
    "pipeline.run_id": "inv-20240601-abc.model.orders_daily",
    "pipeline.engine": "dbt",
    "pipeline.type": "incremental",
    "pipeline.status": "succeeded",
    "pipeline.environment": "production",
    "pipeline.rows_written": 86400,
    "pipeline.lineage.sources": ["main.bronze.orders", "main.dim.customers"],
    "pipeline.lineage.destinations": ["main.silver.orders_daily"],
    "pipeline.quality.rules_passed": 3,
    "pipeline.quality.rules_failed": 0
  }
}
```

---

## Open Questions

1. **Task-level vs. run-level granularity** — Should individual tasks within a DAG (e.g., a single dbt model within an invocation) emit child spans under the parent `pipeline.run` span? The current proposal allows both but does not require task-level spans.

2. **Schema evolution events** — Should schema changes (new columns, type changes) be emitted as span events or as separate `pipeline.schema_change` spans? This intersects with the data contract ecosystem.

3. **Streaming pipelines** — Streaming jobs do not have a discrete "finished_at". Recommendation: emit a span per micro-batch or checkpoint interval with `pipeline.type = "streaming"`.

4. **Cross-engine lineage stitching** — Dataset URIs from different engines may refer to the same physical data but use different naming conventions. A URI normalization layer is outside the scope of this OTEP but acknowledged.

5. **Integration with OpenLineage** — OpenLineage (part of the Linux Foundation) defines a lineage event model. This proposal is compatible: an OpenLineage `RunEvent` can be mapped to a `pipeline.run` span. We propose alignment rather than competition.

---

## Reference Implementation

A reference implementation is available at [github.com/stark256-spec/pipeline-obs](https://github.com/stark256-spec/pipeline-obs). It includes:

- Pydantic models for all attributes defined in this OTEP
- Collectors for Databricks Jobs API, Delta Live Tables, and dbt Cloud/artifacts
- An OTLP exporter using the OpenTelemetry Python SDK
- A Grafana dashboard JSON for the standard metrics

---

## Prior Art

- [OpenLineage](https://openlineage.io/) — Lineage event spec (LF AI & Data). Focused on lineage; does not cover cost, quality, or metrics.
- [DataHub](https://datahubproject.io/) — Metadata platform. Not an observability protocol.
- [Marquez](https://marquezproject.ai/) — OpenLineage-compatible metadata server.
- [Monte Carlo Data](https://www.montecarlodata.com/) — Commercial data observability. Proprietary schema.
- [Elementary Data](https://www.elementary-data.com/) — dbt-native observability. Engine-specific.

None of these are OpenTelemetry-native or define a spans-and-metrics model compatible with existing OTLP infrastructure (Prometheus, Grafana, Datadog, Honeycomb, etc.).

---

## Changelog

| Date | Change |
|---|---|
| 2026-06-01 | Initial draft |
