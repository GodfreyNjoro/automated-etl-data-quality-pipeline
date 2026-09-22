"""Streamlit monitoring dashboard for the ETL & data quality pipeline.

Displays pipeline run history, data-quality metrics and trends, active alerts,
source-to-target lineage, volume/freshness trends, and failed-record analysis
with interactive filters and drill-downs.

Run with::

    streamlit run dashboard/app.py

The dashboard reads from the metadata database when available and transparently
falls back to the local example run summary otherwise.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make the project importable when launched via `streamlit run`.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.data_access import DashboardData

st.set_page_config(
    page_title="ETL & Data Quality Monitor",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(ttl=30)
def _load() -> dict:
    data = DashboardData()
    return {
        "db_available": data.db_available,
        "runs": data.recent_runs(),
        "stages": data.stage_runs(),
        "scores": data.quality_scores(),
        "metrics": data.quality_metrics(),
        "lineage": data.lineage(),
        "alerts": data.alerts(),
        "quarantine": data.quarantine_log(),
    }


def _kpi(col, label: str, value: str) -> None:
    col.metric(label, value)


def main() -> None:
    st.title("📊 ETL & Data Quality Monitor")

    data = DashboardData()
    if data.db_available:
        st.caption("Connected to the metadata database.")
    else:
        st.info(
            "Metadata database not reachable — showing the latest local example "
            "run. Start PostgreSQL (see docker-compose.yml) for full history.",
            icon="ℹ️",
        )

    runs = data.recent_runs()
    if runs.empty:
        st.warning(
            "No pipeline runs found yet. Run the example first:\n\n"
            "`python examples/retail_aggregation/run_example.py`"
        )
        return

    # ---- Sidebar filters --------------------------------------------
    st.sidebar.header("Filters")
    pipelines = ["All"] + sorted(runs["pipeline_name"].dropna().unique().tolist())
    selected_pipeline = st.sidebar.selectbox("Pipeline", pipelines)
    statuses = ["All"] + sorted(runs["status"].dropna().unique().tolist())
    selected_status = st.sidebar.selectbox("Status", statuses)

    filtered = runs.copy()
    if selected_pipeline != "All":
        filtered = filtered[filtered["pipeline_name"] == selected_pipeline]
    if selected_status != "All":
        filtered = filtered[filtered["status"] == selected_status]

    # ---- KPI header --------------------------------------------------
    total_runs = len(filtered)
    success_runs = int((filtered["status"] == "success").sum())
    success_rate = f"{(success_runs / total_runs * 100):.0f}%" if total_runs else "n/a"
    total_loaded = int(filtered.get("rows_loaded", pd.Series(dtype=int)).fillna(0).sum())
    total_quarantined = int(
        filtered.get("rows_quarantined", pd.Series(dtype=int)).fillna(0).sum()
    )

    c1, c2, c3, c4 = st.columns(4)
    _kpi(c1, "Total runs", str(total_runs))
    _kpi(c2, "Success rate", success_rate)
    _kpi(c3, "Rows loaded", f"{total_loaded:,}")
    _kpi(c4, "Rows quarantined", f"{total_quarantined:,}")

    tabs = st.tabs(
        [
            "Run history",
            "Quality metrics",
            "Alerts",
            "Lineage",
            "Volume & freshness",
            "Failed records",
        ]
    )

    # ---- Run history -------------------------------------------------
    with tabs[0]:
        st.subheader("Pipeline run history")
        st.dataframe(filtered, use_container_width=True, hide_index=True)

        run_ids = filtered["run_id"].tolist()
        if run_ids:
            selected_run = st.selectbox("Inspect a run", run_ids)
            stages = data.stage_runs(selected_run)
            if stages.empty:
                stages = data.stage_runs()
            if not stages.empty:
                st.markdown("**Stage breakdown**")
                st.dataframe(stages, use_container_width=True, hide_index=True)

    # ---- Quality metrics ---------------------------------------------
    with tabs[1]:
        st.subheader("Data quality scores over time")
        scores = data.quality_scores()
        if scores.empty:
            st.write("No quality scores recorded yet.")
        else:
            if "recorded_at" in scores.columns:
                scores["recorded_at"] = pd.to_datetime(
                    scores["recorded_at"], errors="coerce"
                )
            if {"recorded_at", "score", "dataset"}.issubset(scores.columns) and scores[
                "recorded_at"
            ].notna().any():
                fig = px.line(
                    scores.sort_values("recorded_at"),
                    x="recorded_at",
                    y="score",
                    color="dataset",
                    markers=True,
                    title="Quality score trend by dataset",
                )
                fig.update_yaxes(range=[0, 1], tickformat=".0%")
                st.plotly_chart(fig, use_container_width=True)
            latest = scores.sort_values(
                "recorded_at" if "recorded_at" in scores.columns else "dataset"
            ).groupby("dataset", as_index=False).last()
            fig2 = px.bar(
                latest,
                x="dataset",
                y="score",
                color="score",
                color_continuous_scale="RdYlGn",
                range_color=[0, 1],
                title="Latest quality score by dataset",
            )
            fig2.update_yaxes(range=[0, 1], tickformat=".0%")
            st.plotly_chart(fig2, use_container_width=True)

        metrics = data.quality_metrics()
        if not metrics.empty:
            st.markdown("**Expectation-level results (latest run)**")
            st.dataframe(metrics, use_container_width=True, hide_index=True)

    # ---- Alerts ------------------------------------------------------
    with tabs[2]:
        st.subheader("Alert history")
        alerts = data.alerts()
        if alerts.empty:
            st.success("No alerts recorded.")
        else:
            st.dataframe(alerts, use_container_width=True, hide_index=True)

    # ---- Lineage -----------------------------------------------------
    with tabs[3]:
        st.subheader("Source-to-target lineage")
        lineage = data.lineage()
        if lineage.empty:
            st.write("No lineage recorded yet.")
        else:
            st.dataframe(lineage, use_container_width=True, hide_index=True)
            for _, row in lineage.iterrows():
                st.markdown(
                    f"`{row.get('source_name')}` → **{row.get('stage_name')}** → "
                    f"`{row.get('target_name')}.{row.get('target_table')}`"
                )

    # ---- Volume & freshness ------------------------------------------
    with tabs[4]:
        st.subheader("Data volume trends")
        if {"rows_extracted", "rows_loaded", "rows_quarantined"}.issubset(
            filtered.columns
        ):
            vol = filtered.copy()
            x_col = "started_at" if "started_at" in vol.columns else "run_id"
            vol[x_col] = vol[x_col].astype(str)
            melted = vol.melt(
                id_vars=[x_col],
                value_vars=["rows_extracted", "rows_loaded", "rows_quarantined"],
                var_name="metric",
                value_name="rows",
            )
            fig = px.bar(
                melted, x=x_col, y="rows", color="metric", barmode="group",
                title="Rows extracted / loaded / quarantined per run",
            )
            st.plotly_chart(fig, use_container_width=True)

        stages_all = data.stage_runs()
        if not stages_all.empty and "quality_score" in stages_all.columns:
            st.markdown("**Quarantine rate by stage**")
            stages_all = stages_all.copy()
            stages_all["quarantine_rate"] = stages_all.apply(
                lambda r: (r["rows_quarantined"] / r["rows_extracted"])
                if r.get("rows_extracted") else 0.0,
                axis=1,
            )
            fig2 = px.bar(
                stages_all, x="stage_name", y="quarantine_rate",
                title="Quarantine rate by stage",
            )
            fig2.update_yaxes(tickformat=".1%")
            st.plotly_chart(fig2, use_container_width=True)

    # ---- Failed records ----------------------------------------------
    with tabs[5]:
        st.subheader("Quarantined / failed records")
        quarantine = data.quarantine_log()
        if quarantine.empty:
            st.success("No quarantined records.")
        else:
            st.dataframe(quarantine, use_container_width=True, hide_index=True)
            if "file_path" in quarantine.columns:
                paths = quarantine["file_path"].dropna().tolist()
                if paths:
                    chosen = st.selectbox("Preview quarantined rows", paths)
                    sample = data.quarantine_sample(chosen)
                    if not sample.empty:
                        st.dataframe(sample, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
