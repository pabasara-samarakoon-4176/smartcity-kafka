import os
import time
import pandas as pd
import streamlit as st
import polars as pl
import glob

LIVE_DATA_PATH = "./data/traffic_aggregates/parquet"
REPORT_PATH = "./data/traffic_reports/daily_peaks"

row_height = 35
header_height = 40

st.set_page_config(
    page_title="Smart City Traffic Dashboard",
    layout="wide"
)

st.title("Smart City Traffic & Congestion Monitoring")

# ----------------------------
# Helpers
# ----------------------------
@st.cache_data(ttl=30)
def load_batch_report():
    if not os.path.exists(REPORT_PATH):
        return None

    csv_files = []

    for root, dirs, files in os.walk(REPORT_PATH):
        for f in files:
            if f.endswith(".csv"):
                csv_files.append(os.path.join(root, f))

    if not csv_files:
        return None

    return pd.concat(
        [pd.read_csv(f) for f in csv_files],
        ignore_index=True
    )


@st.cache_data(ttl=5)
def load_live_data():
    all_files = glob.glob(f"{LIVE_DATA_PATH}/**/*.parquet", recursive=True)
    if not all_files:
        return None

    dfs = []
    for f in all_files:
        try:
            df = pl.read_parquet(f)
            path_parts = f.split(os.sep)
            traffic_date = [p for p in path_parts if "traffic_date=" in p][0].split("=")[1]
            sensor_id = [p for p in path_parts if "sensor_id=" in p][0].split("=")[1]
            
            df = df.with_columns([
                pl.lit(traffic_date).alias("traffic_date"),
                pl.lit(sensor_id).alias("sensor_id")
            ])
            
            dfs.append(df)
        except Exception as e:
            continue

    return pl.concat(dfs).to_pandas() if dfs else None

# ----------------------------
# Live Stream Section
# ----------------------------
st.header("Live Traffic Monitoring")

live_df = load_live_data()

if live_df is not None:

    latest_df = (
        live_df
        .sort_values("window_start")
        .groupby("sensor_id")
        .tail(1)
    )

    st.subheader("Current Junction Status")

    table_height = (len(latest_df) * row_height) + header_height

    sorted_df = latest_df[
        ["sensor_id", "total_vehicles", "window_avg_speed", "congestion_index"]
    ].sort_values(by="congestion_index", ascending=False)

    st.dataframe(
        sorted_df,
        use_container_width=True,
        height=table_height
    )

    st.subheader("Congestion Trends")

    chart_df = (
        live_df.groupby("sensor_id")["congestion_index"]
        .mean()
        .reset_index()
        .set_index("sensor_id")
    )

    st.bar_chart(chart_df)

else:
    st.warning("No live stream data found.")


# ----------------------------
# Batch Report Section
# ----------------------------
st.header("Nightly Intervention Report")

report_df = load_batch_report()

if report_df is not None:

    report_table_height = (len(report_df) * row_height) + header_height

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Total Junctions",
            len(report_df)
        )

    with col2:
        flagged = len(
            report_df[
                report_df["intervention_required"] == "YES"
            ]
        )
        st.metric(
            "Need Intervention",
            flagged
        )

    with col3:
        busiest = report_df.sort_values(
            "hourly_volume",
            ascending=False
        ).iloc[0]["sensor_id"]

        st.metric(
            "Busiest Junction",
            busiest
        )

    st.dataframe(
        report_df,
        use_container_width=True,
        height=report_table_height
    )

else:
    st.warning("No batch report found.")



# ----------------------------
# Auto Refresh
# ----------------------------
time.sleep(5)
st.rerun()