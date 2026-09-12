"""
Phase 5 demo dashboard: a Streamlit app reading directly off the local
lakehouse -- no separate BI server to stand up. Local dev default for
"Analytics serving"; see docs/interfaces.md's "BI/serving" row for the
swap to a real BI tool (Databricks SQL dashboards in the cloud, or
Superset/Rill for a heavier local BI tool).

Two data sources, two connections, so the dashboard never takes a lock
that could block a batch write (DuckDB allows many readers but a writer
needs the file exclusive -- see docs/interfaces.md):
  - main_gold.daily_revenue (dbt-built batch gold): opened read_only
    against warehouse.duckdb.
  - gold_streaming/session_activity_1min (delta-rs, written by the
    streaming job): queried via delta_scan() on a fresh in-memory
    connection, never touching warehouse.duckdb's file lock at all.

Each successful load also records a table_reads entry (see
metadata/table_reads.py) -- otherwise a gold table someone is actually
looking at right now would still show up in Phase 7's unused-tables
report, since nothing else in the platform re-reads gold tables through a
tracked pipeline run.

Run: streamlit run serving/dashboard.py
"""
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_PATH = REPO_ROOT / "lakehouse" / "warehouse.duckdb"
STREAMING_GOLD_PATH = REPO_ROOT / "lakehouse" / "gold_streaming" / "session_activity_1min"

sys.path.append(str(REPO_ROOT / "metadata"))
from table_reads import record_table_read  # noqa: E402

st.set_page_config(page_title="Data Plant", layout="wide")
st.title("Data Plant -- Analytics")
st.caption(
    "Local dev dashboard: batch gold (DuckDB) + streaming gold (Delta), read directly off the lakehouse."
)


@st.cache_data(ttl=10)
def load_daily_revenue() -> pd.DataFrame:
    if not WAREHOUSE_PATH.exists():
        return pd.DataFrame()
    with duckdb.connect(str(WAREHOUSE_PATH), read_only=True) as con:
        df = con.sql("select * from main_gold.daily_revenue order by order_date").df()
    record_table_read(table_name="main_gold.daily_revenue", reader_job="streamlit_dashboard")
    return df


@st.cache_data(ttl=10)
def load_session_activity() -> pd.DataFrame:
    if not STREAMING_GOLD_PATH.exists():
        return pd.DataFrame()
    with duckdb.connect() as con:
        df = con.sql(
            f"select * from delta_scan('{STREAMING_GOLD_PATH.as_posix()}') "
            "order by window_start, event_type"
        ).df()
    record_table_read(table_name=str(STREAMING_GOLD_PATH), reader_job="streamlit_dashboard")
    return df


revenue = load_daily_revenue()
activity = load_session_activity()

st.header("Batch: daily revenue")
st.caption("`main_gold.daily_revenue`, built by dbt from bronze orders/customers")
if revenue.empty:
    st.info("No batch gold data yet -- run `python scripts/run_phase1_local_demo.py` first.")
else:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Revenue over time")
        st.line_chart(revenue.groupby("order_date")["total_revenue"].sum())
    with col2:
        st.subheader("Revenue by region")
        st.bar_chart(revenue.groupby("region")["total_revenue"].sum().sort_values(ascending=False))

st.header("Streaming: clickstream session activity")
st.caption("`gold_streaming/session_activity_1min`, built by the windowed Kafka consumer")
if activity.empty:
    st.info("No streaming gold data yet -- run `python scripts/run_phase4_streaming_demo.py` first.")
else:
    st.subheader("Events per window, by type")
    pivot = activity.pivot_table(
        index="window_start", columns="event_type", values="event_count", fill_value=0
    )
    st.line_chart(pivot)
    st.subheader("Raw windows")
    st.dataframe(activity, use_container_width=True)
