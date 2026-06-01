"""Tests for Databricks and dbt collectors — mocked HTTP."""

import json
from pathlib import Path

import httpx
import pytest
import respx

from pipeline_obs.collectors.databricks import DatabricksCollector, DeltaLiveTablesCollector
from pipeline_obs.collectors.dbt import DbtArtifactsCollector
from pipeline_obs.schema import PipelineEngine, PipelineStatus

_DB_HOST = "https://adb-123.azuredatabricks.net"
_DB_TOKEN = "dapi-test"

_JOB_RUN = {
    "run_id": 42,
    "run_name": "raw_to_bronze.orders",
    "job_id": 10,
    "start_time": 1717228800000,
    "end_time": 1717232400000,
    "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "SUCCESS",
        "state_message": "",
    },
    "tasks": [],
    "cluster_spec": {"new_cluster": {"num_workers": 4}},
}

_RUNS_LIST = {"runs": [_JOB_RUN], "has_more": False}

_DLT_PIPELINES = {
    "statuses": [
        {
            "pipeline_id": "pipe-001",
            "name": "orders_dlt",
            "state": "RUNNING",
            "target": "catalog.silver",
            "latest_updates": [{"update_id": "upd-001"}],
        }
    ]
}


@pytest.mark.asyncio
async def test_databricks_collector_returns_pipeline_runs():
    with respx.mock:
        respx.get(f"{_DB_HOST}/api/2.1/jobs/runs/list").mock(
            return_value=httpx.Response(200, json=_RUNS_LIST)
        )
        async with httpx.AsyncClient() as client:
            collector = DatabricksCollector(_DB_HOST, _DB_TOKEN, client=client)
            runs = await collector.collect(limit=10)
    assert len(runs) == 1
    assert runs[0].name == "raw_to_bronze.orders"
    assert runs[0].engine == PipelineEngine.DATABRICKS
    assert runs[0].status == PipelineStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_databricks_collector_duration():
    with respx.mock:
        respx.get(f"{_DB_HOST}/api/2.1/jobs/runs/list").mock(
            return_value=httpx.Response(200, json=_RUNS_LIST)
        )
        async with httpx.AsyncClient() as client:
            collector = DatabricksCollector(_DB_HOST, _DB_TOKEN, client=client)
            runs = await collector.collect()
    assert runs[0].duration_seconds == 3600.0


@pytest.mark.asyncio
async def test_databricks_collector_failed_run():
    failed_run = {
        **_JOB_RUN,
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "FAILED",
            "state_message": "OOM",
        },
    }
    with respx.mock:
        respx.get(f"{_DB_HOST}/api/2.1/jobs/runs/list").mock(
            return_value=httpx.Response(200, json={"runs": [failed_run], "has_more": False})
        )
        async with httpx.AsyncClient() as client:
            collector = DatabricksCollector(_DB_HOST, _DB_TOKEN, client=client)
            runs = await collector.collect()
    assert runs[0].status == PipelineStatus.FAILED
    assert runs[0].error_message == "OOM"


@pytest.mark.asyncio
async def test_dlt_collector_returns_pipeline_runs():
    with respx.mock:
        respx.get(f"{_DB_HOST}/api/2.0/pipelines").mock(
            return_value=httpx.Response(200, json=_DLT_PIPELINES)
        )
        async with httpx.AsyncClient() as client:
            collector = DeltaLiveTablesCollector(_DB_HOST, _DB_TOKEN, client=client)
            runs = await collector.collect()
    assert len(runs) == 1
    assert runs[0].name == "orders_dlt"
    assert runs[0].engine == PipelineEngine.DELTA_LIVE_TABLES


@pytest.mark.asyncio
async def test_dbt_artifacts_collector(tmp_path: Path):
    run_results = {
        "metadata": {"invocation_id": "inv-abc"},
        "elapsed_time": 45.2,
        "results": [
            {
                "unique_id": "model.project.orders_daily",
                "status": "success",
                "adapter_response": {"rows_affected": 86400},
            },
            {
                "unique_id": "model.project.customers",
                "status": "error",
                "adapter_response": {},
            },
        ],
    }
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(run_results))

    collector = DbtArtifactsCollector(path)
    runs = await collector.collect()

    assert len(runs) == 2
    assert runs[0].status == PipelineStatus.SUCCEEDED
    assert runs[0].io.rows_written == 86400
    assert runs[1].status == PipelineStatus.FAILED
    assert runs[0].engine == PipelineEngine.DBT


@pytest.mark.asyncio
async def test_dbt_artifacts_collector_lineage(tmp_path: Path):
    manifest = {
        "nodes": {
            "model.project.orders_daily": {
                "depends_on": {"nodes": ["model.project.raw_orders", "model.project.customers"]}
            }
        }
    }
    run_results = {
        "metadata": {"invocation_id": "inv-abc"},
        "elapsed_time": 10.0,
        "results": [
            {"unique_id": "model.project.orders_daily", "status": "success", "adapter_response": {}}
        ],
    }
    rr_path = tmp_path / "run_results.json"
    m_path = tmp_path / "manifest.json"
    rr_path.write_text(json.dumps(run_results))
    m_path.write_text(json.dumps(manifest))

    collector = DbtArtifactsCollector(rr_path, m_path)
    runs = await collector.collect()
    assert "model.project.raw_orders" in runs[0].lineage.sources
