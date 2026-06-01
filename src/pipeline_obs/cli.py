"""CLI: collect, export, validate, dashboard."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    help="Unified observability collector for multi-cloud data pipelines.",
    no_args_is_help=True,
)
console = Console()


@app.command()
def collect(
    engine: Annotated[str, typer.Argument(help="databricks|dbt|dlt")],
    output: Annotated[Optional[Path], typer.Option("--output", "-o", help="JSONL output")] = None,
    otlp: Annotated[Optional[str], typer.Option("--otlp", help="OTLP endpoint")] = None,
    since_hours: Annotated[int, typer.Option("--since", help="Collect from last N hours")] = 24,
    limit: Annotated[int, typer.Option("--limit")] = 100,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Collect pipeline runs from an engine and export them."""
    from datetime import datetime, timedelta, timezone

    from pipeline_obs.collectors.databricks import DatabricksCollector, DeltaLiveTablesCollector
    from pipeline_obs.collectors.dbt import DbtCloudCollector
    from pipeline_obs.exporters.jsonl import export_jsonl
    from pipeline_obs.exporters.otel import OtelExporter

    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    collector = None
    if engine == "databricks":
        host = os.environ.get("DATABRICKS_HOST", "")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        if not host or not token:
            console.print("[red]Set DATABRICKS_HOST and DATABRICKS_TOKEN env vars.[/red]")
            raise typer.Exit(1)
        collector = DatabricksCollector(host, token)
    elif engine == "dlt":
        host = os.environ.get("DATABRICKS_HOST", "")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        collector = DeltaLiveTablesCollector(host, token)
    elif engine == "dbt":
        account_id = int(os.environ.get("DBT_ACCOUNT_ID", "0"))
        token = os.environ.get("DBT_API_TOKEN", "")
        if not account_id or not token:
            console.print("[red]Set DBT_ACCOUNT_ID and DBT_API_TOKEN env vars.[/red]")
            raise typer.Exit(1)
        collector = DbtCloudCollector(account_id, token)
    else:
        console.print(f"[red]Unknown engine:[/red] {engine}. Supported: databricks, dlt, dbt")
        raise typer.Exit(1)

    console.print(f"Collecting from [bold]{engine}[/bold] (last {since_hours}h) …")
    runs = asyncio.run(collector.collect(since=since, limit=limit))
    console.print(f"Collected [green]{len(runs)}[/green] runs.")

    if as_json:
        print(json.dumps([json.loads(r.model_dump_json(exclude={"raw"})) for r in runs], indent=2))
        return

    if otlp:
        exporter = OtelExporter(otlp_endpoint=otlp)
        exporter.export_all(runs)
        console.print(f"Exported to OTLP: {otlp}")
    elif output:
        export_jsonl(runs, output)
        console.print(f"Written to {output}")
    else:
        _print_runs_table(runs)


@app.command()
def validate(
    jsonl_path: Annotated[Path, typer.Argument(help="Path to a JSONL file of pipeline runs")],
) -> None:
    """Validate a JSONL file against the pipeline-obs schema."""
    from pipeline_obs.schema import PipelineRun

    errors = 0
    total = 0
    for i, line in enumerate(jsonl_path.read_text().splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        total += 1
        try:
            PipelineRun.model_validate_json(line)
        except Exception as exc:
            console.print(f"[red]Line {i}:[/red] {exc}")
            errors += 1

    if errors:
        console.print(f"[red]{errors}/{total} records failed validation.[/red]")
        raise typer.Exit(1)
    console.print(f"[green]All {total} records valid.[/green]")


@app.command()
def schema_dump() -> None:
    """Print the JSON Schema for PipelineRun."""
    from pipeline_obs.schema import PipelineRun

    print(json.dumps(PipelineRun.model_json_schema(), indent=2))


@app.command()
def dbt_artifacts(
    run_results: Annotated[Path, typer.Argument(help="Path to dbt run_results.json")],
    manifest: Annotated[Optional[Path], typer.Option("--manifest")] = None,
    output: Annotated[Optional[Path], typer.Option("--output", "-o")] = None,
    otlp: Annotated[Optional[str], typer.Option("--otlp")] = None,
) -> None:
    """Import a local dbt run_results.json artifact (no API key needed)."""
    from pipeline_obs.collectors.dbt import DbtArtifactsCollector
    from pipeline_obs.exporters.jsonl import export_jsonl
    from pipeline_obs.exporters.otel import OtelExporter

    collector = DbtArtifactsCollector(run_results, manifest)
    runs = asyncio.run(collector.collect())
    console.print(f"Imported [green]{len(runs)}[/green] dbt model runs.")

    if otlp:
        OtelExporter(otlp_endpoint=otlp).export_all(runs)
    elif output:
        export_jsonl(runs, output)
    else:
        _print_runs_table(runs)


def _print_runs_table(runs: list) -> None:
    table = Table(show_header=True)
    table.add_column("Name", max_width=40)
    table.add_column("Engine", max_width=15)
    table.add_column("Status")
    table.add_column("Duration")
    table.add_column("Rows written")
    for r in runs:
        dur = f"{r.duration_seconds:.0f}s" if r.duration_seconds else "—"
        rows = str(r.io.rows_written) if r.io.rows_written else "—"
        _colors = {"succeeded": "green", "failed": "red", "running": "yellow"}
        color = _colors.get(r.status.value, "white")
        table.add_row(r.name, r.engine.value, f"[{color}]{r.status.value}[/{color}]", dur, rows)
    console.print(table)
