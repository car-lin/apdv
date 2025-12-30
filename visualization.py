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
import config

warnings.filterwarnings('ignore')

# Page config
st.set_page_config(page_title="Dublin Bikes Dashboard", layout="wide")

# Database connection
#engine = create_engine("postgresql://postgres:admin@localhost:5432/bikes")
@st.cache_resource
def get_engine():
    return create_engine(
        config.POSTGRES_URI,
        pool_pre_ping=True,
        pool_recycle=300
    )

engine = get_engine()

st.title("DUBLIN BIKES: SURPLUS/DEFICIT OPTIMIZATION DASHBOARD")

# Sidebar sliders 
st.sidebar.header("Time Range Selection")
start_hour = st.sidebar.slider("Start Hour", 0, 23, 7)
end_hour = st.sidebar.slider("End Hour", 0, 23, 10)

if start_hour > end_hour:
    st.error("Invalid Time period. The Start hour must be lesser than the End hour.")
    st.stop()

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
        df = pd.read_sql(query, conn, params={'start': start_h, 'end': end_h})
        df_rt = pd.read_sql("""
            SELECT station_id, name, capacity,
                   latitude AS lat, longitude AS lon,
                   num_bikes_available, num_docks_available, utilization
            FROM realtime_stations
            WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM realtime_stations)
        """, conn)

    return df, df_rt

df, df_rt = load_data(start_hour, end_hour)

if len(df) == 0:
    st.warning("No data for selected time range")
    st.stop()

# Peak analysis 
peak_data = df.groupby('station_id').agg({
    'name': 'first', 'capacity': 'first', 'lat': 'first', 'lon': 'first',
    'utilization': ['max', 'mean'], 'imbalance': 'max',
    'num_bikes_available': ['max', 'min'], 'num_docks_available': 'mean'
}).round(3)

peak_data.columns = ['name', 'capacity', 'lat', 'lon', 'max_util', 'avg_util', 
                    'max_imbalance', 'max_demand', 'min_supply', 'avg_docks']

# Ensure numeric
peak_data['capacity'] = pd.to_numeric(peak_data['capacity'], errors='coerce')
peak_data['max_util'] = pd.to_numeric(peak_data['max_util'], errors='coerce')
peak_data['avg_util'] = pd.to_numeric(peak_data['avg_util'], errors='coerce')

# 1. PEAK UTILIZATION RATE (95th percentile)
peak_util_95th = df['utilization'].quantile(0.95)

# 2. IMBALANCE SCORE
peak_data['imbalance_score'] = np.abs(peak_data['max_util'] - 0.5) * peak_data['capacity']
peak_data['imbalance_score'] = pd.to_numeric(peak_data['imbalance_score'], errors='coerce').fillna(0)

# 3. CRITICAL STATIONS (>30% deviation)
critical = peak_data[peak_data['imbalance_score'] > peak_data['capacity'] * 0.3]

# 4. SURPLUS/DEFICIT CLASSIFICATION 
peak_data['status'] = np.where(peak_data['max_util'] > 0.90, 'SURPLUS',
                     np.where(peak_data['avg_util'] < 0.10, 'DEFICIT', 'BALANCED'))

# TOP 5 SURPLUS/DEFICIT 
top5_surplus = peak_data[peak_data['status'] == 'SURPLUS'].nlargest(5, 'imbalance_score')
top5_deficit = peak_data[peak_data['status'] == 'DEFICIT'].nlargest(5, 'imbalance_score')

# === DASHBOARD DISPLAY ===

# KPI CARDS
col1, col2, col3, col4 = st.columns(4)
col1.metric("Peak Utilization Rate", f"{peak_util_95th:.1%}")
col2.metric("Surplus Risk Stations", len(peak_data[peak_data['max_util'] > 0.90]))
col3.metric("Deficit Risk Stations", len(peak_data[peak_data['avg_util'] < 0.10]))
col4.metric("Avg Imbalance Score", f"{peak_data['imbalance_score'].mean():.1f}")

st.markdown("---")

# Top 5 Surplus/Deficit TABLES
col1, col2 = st.columns(2)
with col1:
    st.subheader("Top 5 Surplus Stations")
    surplus_display = top5_surplus[['name', 'capacity', 'max_util', 'imbalance_score', 'status']].round(2)
    surplus_display.columns = ['Station Name', 'Capacity', 'Max Utilization', 'Imbalance Score', 'Status']
    st.dataframe(surplus_display, use_container_width=True)

with col2:
    st.subheader("Top 5 Deficit Stations")
    deficit_display = top5_deficit[['name', 'capacity', 'max_util', 'imbalance_score', 'status']].round(2)
    deficit_display.columns = ['Station Name', 'Capacity', 'Max Utilization', 'Imbalance Score', 'Status']
    st.dataframe(deficit_display, use_container_width=True)

# SMART ROUTES TABLE
if len(top5_surplus) > 0 and len(top5_deficit) > 0:
    st.subheader("Optimized Rebalancing suggestions:")
    surplus_coords = top5_surplus[['lat', 'lon']].values
    deficit_coords = top5_deficit[['lat', 'lon']].values
    dist_matrix = haversine_distances(np.radians(surplus_coords), np.radians(deficit_coords)) * 6371
    
    row_ind, col_ind = linear_sum_assignment(dist_matrix)
    
    routes_df = []
    for i, j in zip(row_ind, col_ind):
        surplus = top5_surplus.iloc[i]
        deficit = top5_deficit.iloc[j]
        routes_df.append({
            'From': surplus['name'][:25],
            'To': deficit['name'][:25],
            'Distance': f"{dist_matrix[i,j]:.1f}km",
            'Surplus': f"+{surplus['imbalance_score']:.0f}",
            'Deficit': f"-{deficit['imbalance_score']:.0f}"
        })
    st.dataframe(pd.DataFrame(routes_df), use_container_width=True)

st.markdown("---")

# VISUALIZATIONS

# 1. UTILIZATION HEATMAP
st.subheader("1. Utilization Heatmap")
peak_hours = df[df['hour'].between(start_hour, end_hour)]
fig1 = px.density_heatmap(peak_hours.groupby(['hour', 'station_id'])['utilization'].mean().reset_index(),
                         x='hour', y='station_id', z='utilization',
                         title=f"Utilization Heatmap ({start_hour:02d}-{end_hour:02d} hours)",
                         color_continuous_scale="RdYlGn_r")
st.plotly_chart(fig1, use_container_width=True)

# 2. CLUSTER MAP
st.subheader("2. Station Clusters")
coords = df.groupby('station_id')[['lat', 'lon', 'name', 'capacity']].first().reset_index()
coords_array = np.array(list(zip(coords['lat'], coords['lon'])))
cluster_radius = 500
db = DBSCAN(eps=cluster_radius/111000, min_samples=3).fit(coords_array)
coords['cluster'] = db.labels_

fig2 = px.scatter_mapbox(coords, lat="lat", lon="lon",
                        size="capacity", color="cluster",
                        hover_name="name", hover_data=['capacity'],
                        color_continuous_scale="Viridis",
                        mapbox_style="open-street-map",
                        zoom=12, height=500,
                        title=f"Clusters (Radius: {cluster_radius}m)")
st.plotly_chart(fig2, use_container_width=True)

# TOP 10 CRITICAL STATIONS BAR
st.subheader("3. Top 10 Critical Stations")
critical_stations = critical.nlargest(10, 'imbalance_score')
fig_critical = px.bar(critical_stations, x='name', y='imbalance_score', color='max_util',
                     title="Top 10 Critical Stations",
                     color_continuous_scale="Reds", height=450)
fig_critical.update_layout(xaxis_tickangle=45, margin={"t":50})
st.plotly_chart(fig_critical, use_container_width=True)

# 3. SURPLUS/DEFICIT MAP
st.subheader("4. Surplus (Green) vs Deficit (Red)")
fig3 = px.scatter_mapbox(peak_data, lat='lat', lon='lon',
                        size='capacity', color='status',
                        color_discrete_map={'SURPLUS': 'green', 'DEFICIT': 'red', 'BALANCED': 'blue'},
                        hover_name='name', hover_data=['max_util', 'imbalance_score'],
                        mapbox_style='carto-positron', zoom=12, height=500,
                        title=f"Surplus (Green) vs Deficit (Red) - {start_hour:02d}-{end_hour:02d} hours")
st.plotly_chart(fig3, use_container_width=True)

# 4. TOP 5 BAR CHARTS
col1, col2 = st.columns(2)
with col1:
    fig4 = px.bar(top5_surplus, x='name', y='imbalance_score', 
                 title="Top 5 Surplus Stations", color='max_util',
                 color_continuous_scale='Greens')
    st.plotly_chart(fig4, use_container_width=True)

with col2:
    fig5 = px.bar(top5_deficit, x='name', y='imbalance_score',
                 title="Top 5 Deficit Stations", color='max_util',
                 color_continuous_scale='Reds')
    st.plotly_chart(fig5, use_container_width=True)

# 5. OPTIMIZED REBALANCING ROUTES MAP
if len(top5_surplus) > 0 and len(top5_deficit) > 0:
    st.subheader("5. Optimized Rebalancing Routes")
    fig6 = go.Figure()
    colors = px.colors.qualitative.Set1
    
    for idx, (i, j) in enumerate(zip(row_ind, col_ind)):
        surplus = top5_surplus.iloc[i]
        deficit = top5_deficit.iloc[j]
        distance_km = dist_matrix[i, j]
        color = colors[idx % len(colors)]
        
        fig6.add_trace(go.Scattermapbox(
            lat=[surplus['lat'], deficit['lat']],
            lon=[surplus['lon'], deficit['lon']],
            mode='lines+markers',
            line=dict(width=10, color=color),
            marker=dict(size=15, color=color),
            name=f"{surplus['name'][:12]}→{deficit['name'][:12]} ({distance_km:.1f}km)",
            hovertemplate=f"<b>%{{fullData.name}}</b><br>Distance: {distance_km:.1f}km<extra></extra>"
        ))
    
    fig6.update_layout(
        mapbox=dict(style='carto-positron',
                    center=dict(lat=surplus_coords[:,0].mean(), lon=surplus_coords[:,1].mean()),
                    zoom=12),
        height=500, title="Optimized Bikes Rebalancing Routes", showlegend=True
    )
    st.plotly_chart(fig6, use_container_width=True)

st.markdown("---")
