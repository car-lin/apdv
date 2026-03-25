# DUBLIN BIKES: SURPLUS/DEFICIT OPTIMIZATION DASHBOARD
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import warnings
from sklearn.metrics.pairwise import haversine_distances
from scipy.optimize import linear_sum_assignment
from sklearn.cluster import DBSCAN

warnings.filterwarnings('ignore')

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Dublin Bikes Dashboard", layout="wide")

# ── Database connection ───────────────────────────────────────────────────────
# Reads from st.secrets — no hardcoded credentials, no manual URL encoding
@st.cache_resource
def get_engine():
    return create_engine(
        st.secrets["POSTGRES_URI"],
        connect_args={"sslmode": "require"},  # Required by Supabase
        pool_pre_ping=True,
        pool_recycle=300,
    )

engine = get_engine()

# ── Title & sidebar ───────────────────────────────────────────────────────────
st.title("DUBLIN BIKES: SURPLUS/DEFICIT OPTIMIZATION DASHBOARD")

st.sidebar.header("Time Range Selection")
start_hour = st.sidebar.slider("Start Hour", 0, 23, 7)
end_hour   = st.sidebar.slider("End Hour",   0, 23, 10)

if start_hour > end_hour:
    st.error("Invalid time period. Start hour must be less than End hour.")
    st.stop()

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data(start_h, end_h):
    query = text("""
        SELECT station_id, name, capacity, lat, lon, last_reported,
               num_bikes_available, num_docks_available, utilization, imbalance, hour
        FROM historical_stations
        WHERE DATE(last_reported) = '2024-09-01'
          AND hour BETWEEN :start AND :end
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"start": start_h, "end": end_h})
        df_rt = pd.read_sql("""
            SELECT station_id, name, capacity,
                   latitude  AS lat,
                   longitude AS lon,
                   num_bikes_available, num_docks_available, utilization
            FROM realtime_stations
            WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM realtime_stations)
        """, conn)
    return df, df_rt

df, df_rt = load_data(start_hour, end_hour)

if df.empty:
    st.warning("No data found for the selected time range.")
    st.stop()

# ── Peak analysis ─────────────────────────────────────────────────────────────
peak_data = df.groupby("station_id").agg(
    name        =("name",                "first"),
    capacity    =("capacity",            "first"),
    lat         =("lat",                 "first"),
    lon         =("lon",                 "first"),
    max_util    =("utilization",         "max"),
    avg_util    =("utilization",         "mean"),
    max_imbalance=("imbalance",          "max"),
    max_demand  =("num_bikes_available", "max"),
    min_supply  =("num_bikes_available", "min"),
    avg_docks   =("num_docks_available", "mean"),
).round(3)

for col in ("capacity", "max_util", "avg_util"):
    peak_data[col] = pd.to_numeric(peak_data[col], errors="coerce")

# Derived metrics
peak_util_95th          = df["utilization"].quantile(0.95)
peak_data["imbalance_score"] = (
    np.abs(peak_data["max_util"] - 0.5) * peak_data["capacity"]
)
peak_data["imbalance_score"] = (
    pd.to_numeric(peak_data["imbalance_score"], errors="coerce").fillna(0)
)

critical = peak_data[peak_data["imbalance_score"] > peak_data["capacity"] * 0.3]

peak_data["status"] = np.where(
    peak_data["max_util"] > 0.90, "SURPLUS",
    np.where(peak_data["avg_util"] < 0.10, "DEFICIT", "BALANCED"),
)

top5_surplus = peak_data[peak_data["status"] == "SURPLUS"].nlargest(5, "imbalance_score")
top5_deficit = peak_data[peak_data["status"] == "DEFICIT"].nlargest(5, "imbalance_score")

# ── KPI cards ─────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Peak Utilization Rate",  f"{peak_util_95th:.1%}")
col2.metric("Surplus Risk Stations",  len(peak_data[peak_data["max_util"] > 0.90]))
col3.metric("Deficit Risk Stations",  len(peak_data[peak_data["avg_util"] < 0.10]))
col4.metric("Avg Imbalance Score",    f"{peak_data['imbalance_score'].mean():.1f}")

st.markdown("---")

# ── Top 5 tables ──────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 5 Surplus Stations")
    surplus_display = top5_surplus[["name", "capacity", "max_util", "imbalance_score", "status"]].round(2).copy()
    surplus_display.columns = ["Station Name", "Capacity", "Max Utilization", "Imbalance Score", "Status"]
    st.dataframe(surplus_display, use_container_width=True)

with col2:
    st.subheader("Top 5 Deficit Stations")
    deficit_display = top5_deficit[["name", "capacity", "max_util", "imbalance_score", "status"]].round(2).copy()
    deficit_display.columns = ["Station Name", "Capacity", "Max Utilization", "Imbalance Score", "Status"]
    st.dataframe(deficit_display, use_container_width=True)

# ── Smart routes table ────────────────────────────────────────────────────────
row_ind = col_ind = None  # initialise so the routes map block can check safely

if len(top5_surplus) > 0 and len(top5_deficit) > 0:
    st.subheader("Optimized Rebalancing Suggestions")

    surplus_coords = top5_surplus[["lat", "lon"]].values
    deficit_coords = top5_deficit[["lat", "lon"]].values
    dist_matrix    = haversine_distances(np.radians(surplus_coords), np.radians(deficit_coords)) * 6371

    row_ind, col_ind = linear_sum_assignment(dist_matrix)

    routes_rows = []
    for i, j in zip(row_ind, col_ind):
        surplus = top5_surplus.iloc[i]
        deficit = top5_deficit.iloc[j]
        routes_rows.append({
            "From":     surplus["name"][:25],
            "To":       deficit["name"][:25],
            "Distance": f"{dist_matrix[i, j]:.1f} km",
            "Surplus":  f"+{surplus['imbalance_score']:.0f}",
            "Deficit":  f"-{deficit['imbalance_score']:.0f}",
        })
    st.dataframe(pd.DataFrame(routes_rows), use_container_width=True)

st.markdown("---")

# ── Visualizations ────────────────────────────────────────────────────────────

# 1. Utilization Heatmap
st.subheader("1. Utilization Heatmap")
peak_hours = df[df["hour"].between(start_hour, end_hour)]
fig1 = px.density_heatmap(
    peak_hours.groupby(["hour", "station_id"])["utilization"].mean().reset_index(),
    x="hour", y="station_id", z="utilization",
    title=f"Utilization Heatmap ({start_hour:02d}–{end_hour:02d} hours)",
    color_continuous_scale="RdYlGn_r",
)
st.plotly_chart(fig1, use_container_width=True)

# 2. Station Clusters
st.subheader("2. Station Clusters")
coords       = df.groupby("station_id")[["lat", "lon", "name", "capacity"]].first().reset_index()
coords_array = coords[["lat", "lon"]].values
db           = DBSCAN(eps=500 / 111_000, min_samples=3).fit(coords_array)
coords["cluster"] = db.labels_

fig2 = px.scatter_mapbox(
    coords, lat="lat", lon="lon",
    size="capacity", color="cluster",
    hover_name="name", hover_data=["capacity"],
    color_continuous_scale="Viridis",
    mapbox_style="open-street-map",
    zoom=12, height=500,
    title="Station Clusters (radius 500 m)",
)
st.plotly_chart(fig2, use_container_width=True)

# 3. Top 10 Critical Stations
st.subheader("3. Top 10 Critical Stations")
critical_stations = critical.nlargest(10, "imbalance_score")
fig_critical = px.bar(
    critical_stations, x="name", y="imbalance_score", color="max_util",
    title="Top 10 Critical Stations",
    color_continuous_scale="Reds", height=450,
)
fig_critical.update_layout(xaxis_tickangle=45, margin={"t": 50})
st.plotly_chart(fig_critical, use_container_width=True)

# 4. Surplus vs Deficit Map
st.subheader("4. Surplus (Green) vs Deficit (Red)")
fig3 = px.scatter_mapbox(
    peak_data, lat="lat", lon="lon",
    size="capacity", color="status",
    color_discrete_map={"SURPLUS": "green", "DEFICIT": "red", "BALANCED": "blue"},
    hover_name="name", hover_data=["max_util", "imbalance_score"],
    mapbox_style="carto-positron", zoom=12, height=500,
    title=f"Surplus vs Deficit — {start_hour:02d}–{end_hour:02d} hours",
)
st.plotly_chart(fig3, use_container_width=True)

# 5. Top 5 Bar Charts
col1, col2 = st.columns(2)
with col1:
    fig4 = px.bar(top5_surplus, x="name", y="imbalance_score",
                  title="Top 5 Surplus Stations", color="max_util",
                  color_continuous_scale="Greens")
    st.plotly_chart(fig4, use_container_width=True)
with col2:
    fig5 = px.bar(top5_deficit, x="name", y="imbalance_score",
                  title="Top 5 Deficit Stations", color="max_util",
                  color_continuous_scale="Reds")
    st.plotly_chart(fig5, use_container_width=True)

# 6. Optimized Rebalancing Routes Map
if row_ind is not None and col_ind is not None:
    st.subheader("5. Optimized Rebalancing Routes")
    fig6   = go.Figure()
    colors = px.colors.qualitative.Set1

    for idx, (i, j) in enumerate(zip(row_ind, col_ind)):
        surplus     = top5_surplus.iloc[i]
        deficit     = top5_deficit.iloc[j]
        distance_km = dist_matrix[i, j]
        color       = colors[idx % len(colors)]

        fig6.add_trace(go.Scattermapbox(
            lat=[surplus["lat"], deficit["lat"]],
            lon=[surplus["lon"], deficit["lon"]],
            mode="lines+markers",
            line=dict(width=10, color=color),
            marker=dict(size=15, color=color),
            name=f"{surplus['name'][:12]}→{deficit['name'][:12]} ({distance_km:.1f} km)",
            hovertemplate=(
                f"<b>%{{fullData.name}}</b><br>"
                f"Distance: {distance_km:.1f} km<extra></extra>"
            ),
        ))

    fig6.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=surplus_coords[:, 0].mean(), lon=surplus_coords[:, 1].mean()),
            zoom=12,
        ),
        height=500,
        title="Optimized Bike Rebalancing Routes",
        showlegend=True,
    )
    st.plotly_chart(fig6, use_container_width=True)

st.markdown("---")
