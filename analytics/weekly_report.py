from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd

from analytics.local_dashboard_data import _build_performance_summary
from config import settings
from storage.local_mysql_storage import LocalMySQLStorage


@dataclass(slots=True)
class WeeklyReportResult:
    report_name: str
    period_start: date
    period_end: date
    output_dir: Path
    markdown_path: Path
    pdf_path: Path
    csv_paths: dict[str, Path]
    metrics: dict[str, Any]


def generate_weekly_performance_report(
    storage: LocalMySQLStorage,
    as_of_date: date | None = None,
    days: int = 7,
    output_root: str | Path | None = None,
    report_name: str = "weekly_performance",
) -> WeeklyReportResult:
    period_end = as_of_date or date.today()
    period_start = period_end - timedelta(days=days - 1)
    output_root = Path(output_root or settings.report_output_dir)
    output_dir = output_root / f"{period_start:%Y%m%d}_{period_end:%Y%m%d}"
    output_dir.mkdir(parents=True, exist_ok=True)

    frames = _load_report_frames(storage, period_start, period_end)
    performance_summary = _build_performance_summary(frames["evaluations"])
    metrics = _build_metrics(frames, performance_summary, period_start, period_end)
    csv_paths = _write_csvs(output_dir, frames, performance_summary)
    markdown_path = output_dir / "weekly_performance_report.md"
    markdown_path.write_text(
        _render_markdown(metrics, performance_summary),
        encoding="utf-8",
    )
    pdf_path = output_dir / "weekly_performance_report.pdf"
    _write_pdf(pdf_path, metrics, frames, performance_summary)

    storage.save_report_run(
        report_name=report_name,
        period_start=period_start,
        period_end=period_end,
        output_dir=str(output_dir),
        markdown_path=str(markdown_path),
        pdf_path=str(pdf_path),
        metrics=metrics,
    )

    return WeeklyReportResult(
        report_name=report_name,
        period_start=period_start,
        period_end=period_end,
        output_dir=output_dir,
        markdown_path=markdown_path,
        pdf_path=pdf_path,
        csv_paths=csv_paths,
        metrics=metrics,
    )


def _load_report_frames(
    storage: LocalMySQLStorage,
    period_start: date,
    period_end: date,
) -> dict[str, pd.DataFrame]:
    insights = storage.fetch_dashboard_table("insights")
    evaluations = storage.fetch_dashboard_table("insight_evaluations")
    snapshots = storage.fetch_dashboard_table("portfolio_snapshots")
    trades = storage.fetch_dashboard_table("portfolio_trades")
    runs = storage.fetch_dashboard_table("pipeline_run_logs")
    alerts = storage.fetch_dashboard_table("health_alerts")

    return {
        "insights": _filter_date_range(insights, "insight_date", period_start, period_end),
        "evaluations": _filter_date_range(
            evaluations,
            "evaluation_date",
            period_start,
            period_end,
        ),
        "snapshots": _filter_date_range(snapshots, "snapshot_date", period_start, period_end),
        "trades": _filter_timestamp_range(trades, "traded_at_utc", period_start, period_end),
        "runs": _filter_timestamp_range(runs, "started_at_utc", period_start, period_end),
        "alerts": _filter_timestamp_range(alerts, "detected_at_utc", period_start, period_end),
    }


def _filter_date_range(
    dataframe: pd.DataFrame,
    column: str,
    period_start: date,
    period_end: date,
) -> pd.DataFrame:
    if dataframe.empty or column not in dataframe.columns:
        return dataframe

    working = dataframe.copy()
    working[column] = pd.to_datetime(working[column], errors="coerce").dt.date
    return working[
        (working[column] >= period_start)
        & (working[column] <= period_end)
    ]


def _filter_timestamp_range(
    dataframe: pd.DataFrame,
    column: str,
    period_start: date,
    period_end: date,
) -> pd.DataFrame:
    if dataframe.empty or column not in dataframe.columns:
        return dataframe

    working = dataframe.copy()
    working[column] = pd.to_datetime(working[column], utc=True, errors="coerce")
    return working[
        (working[column].dt.date >= period_start)
        & (working[column].dt.date <= period_end)
    ]


def _build_metrics(
    frames: dict[str, pd.DataFrame],
    performance_summary: pd.DataFrame,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    snapshots = frames["snapshots"]
    latest_snapshot = (
        snapshots.sort_values("snapshot_date").iloc[-1].to_dict()
        if not snapshots.empty and "snapshot_date" in snapshots.columns
        else {}
    )
    overall = (
        performance_summary[performance_summary["segment"] == "overall"].iloc[0].to_dict()
        if not performance_summary.empty and "segment" in performance_summary.columns
        else {}
    )

    return {
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "insights_generated": int(len(frames["insights"])),
        "evaluations_saved": int(overall.get("evaluated_insights", 0) or 0),
        "real_market_evaluations": int(overall.get("real_market_evaluations", 0) or 0),
        "synthetic_evaluations": int(overall.get("synthetic_evaluations", 0) or 0),
        "real_win_rate_pct": float(overall.get("real_win_rate_pct", 0.0) or 0.0),
        "real_avg_forward_return_pct": float(
            overall.get("real_avg_forward_return_pct", 0.0) or 0.0
        ),
        "portfolio_total_equity_cad": float(
            latest_snapshot.get("total_equity_cad", 0.0) or 0.0
        ),
        "portfolio_return_pct": float(
            latest_snapshot.get("cumulative_return_pct", 0.0) or 0.0
        ),
        "trades_executed": int(len(frames["trades"])),
        "pipeline_runs": int(len(frames["runs"])),
        "health_alerts": int(len(frames["alerts"])),
    }


def _write_csvs(
    output_dir: Path,
    frames: dict[str, pd.DataFrame],
    performance_summary: pd.DataFrame,
) -> dict[str, Path]:
    csv_paths: dict[str, Path] = {}
    for name, dataframe in {**frames, "performance_summary": performance_summary}.items():
        path = output_dir / f"{name}.csv"
        dataframe.to_csv(path, index=False)
        csv_paths[name] = path
    return csv_paths


def _render_markdown(
    metrics: dict[str, Any],
    performance_summary: pd.DataFrame,
) -> str:
    lines = [
        "# Quicksilver Weekly Performance Report",
        "",
        f"Period: {metrics['period_start']} to {metrics['period_end']}",
        f"Generated at UTC: {metrics['generated_at_utc']}",
        "",
        "## Key Metrics",
        "",
        f"- Insights generated: {metrics['insights_generated']:,}",
        f"- Evaluated insights: {metrics['evaluations_saved']:,}",
        f"- Real-market evaluations: {metrics['real_market_evaluations']:,}",
        f"- Synthetic evaluations: {metrics['synthetic_evaluations']:,}",
        f"- Real win rate: {metrics['real_win_rate_pct']:.1f}%",
        f"- Real average forward return: {metrics['real_avg_forward_return_pct']:.2f}%",
        f"- Portfolio equity: ${metrics['portfolio_total_equity_cad']:,.2f} CAD",
        f"- Portfolio return: {metrics['portfolio_return_pct']:.2f}%",
        f"- Trades executed: {metrics['trades_executed']:,}",
        f"- Pipeline runs: {metrics['pipeline_runs']:,}",
        f"- Health alerts: {metrics['health_alerts']:,}",
        "",
    ]

    if not performance_summary.empty:
        lines.extend(
            [
                "## Performance Segments",
                "",
                _dataframe_to_markdown_table(performance_summary),
                "",
            ]
        )

    return "\n".join(lines)


def _dataframe_to_markdown_table(dataframe: pd.DataFrame) -> str:
    if dataframe.empty:
        return ""

    display = dataframe.copy().head(20)
    headers = [str(column) for column in display.columns]
    rows = [
        [
            _markdown_cell(value)
            for value in row
        ]
        for row in display.itertuples(index=False, name=None)
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _markdown_cell(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value).replace("|", "\\|")


def _write_pdf(
    pdf_path: Path,
    metrics: dict[str, Any],
    frames: dict[str, pd.DataFrame],
    performance_summary: pd.DataFrame,
) -> None:
    with PdfPages(pdf_path) as pdf:
        fig, ax = plt.subplots(figsize=(11, 8.5))
        ax.axis("off")
        metric_lines = [
            "Quicksilver Weekly Performance Report",
            f"{metrics['period_start']} to {metrics['period_end']}",
            "",
            f"Insights generated: {metrics['insights_generated']:,}",
            f"Evaluated insights: {metrics['evaluations_saved']:,}",
            f"Real-market evaluations: {metrics['real_market_evaluations']:,}",
            f"Synthetic evaluations: {metrics['synthetic_evaluations']:,}",
            f"Real win rate: {metrics['real_win_rate_pct']:.1f}%",
            f"Real avg forward return: {metrics['real_avg_forward_return_pct']:.2f}%",
            f"Portfolio equity: ${metrics['portfolio_total_equity_cad']:,.2f} CAD",
            f"Portfolio return: {metrics['portfolio_return_pct']:.2f}%",
            f"Trades executed: {metrics['trades_executed']:,}",
            f"Pipeline runs: {metrics['pipeline_runs']:,}",
            f"Health alerts: {metrics['health_alerts']:,}",
        ]
        ax.text(0.05, 0.95, "\n".join(metric_lines), va="top", fontsize=13)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        snapshots = frames["snapshots"]
        if not snapshots.empty and {"snapshot_date", "total_equity_cad"}.issubset(snapshots.columns):
            chart_data = snapshots.copy()
            chart_data["snapshot_date"] = pd.to_datetime(chart_data["snapshot_date"])
            chart_data = chart_data.sort_values("snapshot_date")
            fig, ax = plt.subplots(figsize=(11, 6))
            ax.plot(chart_data["snapshot_date"], chart_data["total_equity_cad"], marker="o")
            ax.axhline(settings.portfolio_initial_cash_cad, color="#777777", linewidth=1)
            ax.set_title("Mock Portfolio Equity")
            ax.set_xlabel("Date")
            ax.set_ylabel("Total equity CAD")
            fig.autofmt_xdate()
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        if not performance_summary.empty:
            fig, ax = plt.subplots(figsize=(11, 8.5))
            ax.axis("off")
            table_data = performance_summary.head(12).round(4)
            ax.table(
                cellText=table_data.values,
                colLabels=table_data.columns,
                loc="center",
                cellLoc="left",
            )
            ax.set_title("Performance Segments")
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)
