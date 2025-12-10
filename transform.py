import pandas as pd
import numpy as np
import pymongo
import config

def clean_historical(df_raw_csv):
    print("   1.2 Cleaning the historical data...")

    df_csv = df_raw_csv.copy()

    print("   Filtering for 2024-09-01 only...")
    df_csv['last_reported'] = pd.to_datetime(df_csv['last_reported'], errors='coerce')
    df_csv = df_csv[df_csv['last_reported'].dt.date == pd.to_datetime(config.HISTORICAL_DATE).date()]
    print(f"      Filtered to {len(df_csv):,} rows for 2024-09-01")

    df_csv.columns = df_csv.columns.str.lower().str.replace(' ', '_')

    initial_rows = len(df_csv)
    df_csv = df_csv.dropna(subset=['station_id', 'capacity', 'lat', 'lon'])
    print(f"      Dropped {initial_rows - len(df_csv):,} rows (missing critical fields)")

    df_csv['station_id'] = df_csv['station_id'].astype(int)
    df_csv['num_bikes_available'] = pd.to_numeric(df_csv['num_bikes_available'], errors='coerce').fillna(0).astype(int)
    df_csv['num_docks_available'] = pd.to_numeric(df_csv['num_docks_available'], errors='coerce').fillna(0).astype(int)
    df_csv['capacity'] = pd.to_numeric(df_csv['capacity'], errors='coerce').fillna(0).astype(int)

    before_validation = len(df_csv)
    df_csv = df_csv[
        (df_csv['num_bikes_available'] >= 0) & 
        (df_csv['num_docks_available'] >= 0) & 
        (df_csv['capacity'] > 0) &
        (df_csv['num_bikes_available'] <= df_csv['capacity'])
    ]
    print(f"      Dropped {before_validation - len(df_csv):,} invalid business logic rows")

    df_csv['utilization'] = (df_csv['num_bikes_available'] / df_csv['capacity']).round(4)
    df_csv['imbalance'] = np.minimum(np.abs(df_csv['num_bikes_available'] - df_csv['capacity'] * 0.5),9.99).round(4)
    df_csv['hour'] = df_csv['last_reported'].dt.hour
    df_csv['weekday'] = df_csv['last_reported'].dt.day_name()

    print(f"      Final clean dataset: {len(df_csv):,} rows for 2024-09-01")
    return df_csv

def clean_realtime_data(mongo_uri=config.MONGO_URI):
    mongo_client = pymongo.MongoClient(mongo_uri)
    mongo_db = mongo_client["dublin_bikes"]
    raw_collection = mongo_db["raw_realtime_snapshots"]
    all_enriched_data = []

    raw_docs = list(raw_collection.find().sort('snapshot_num', 1).limit(20))
    for raw_doc in raw_docs:
        try:
            stations_info = raw_doc['info_raw']['data']['stations']
            station_lookup = {s['station_id']: s for s in stations_info}

            snapshot_id = raw_doc['snapshot_id']
            snapshot_data = []

            for station in raw_doc['status_raw']['data']['stations']:
                station_id = station['station_id']
                if station_id in station_lookup:
                    info = station_lookup[station_id]
                    row = {
                        'snapshot_id': snapshot_id,
                        'station_id': station_id,
                        'name': info.get('name', 'Unknown'),
                        'capacity': info.get('capacity', 0),
                        'latitude': info.get('lat', info.get('latitude', 0)),
                        'longitude': info.get('lon', info.get('longitude', 0)),
                        'num_bikes_available': station.get('num_bikes_available', 0),
                        'num_docks_available': station.get('num_docks_available', 0),
                        'is_installed': station.get('is_installed', True),
                        'is_renting': station.get('is_renting', True),
                        'is_returning': station.get('is_returning', True),
                        'last_reported': pd.to_datetime(station.get('last_reported', None), unit='s'),
                        'fetch_timestamp': pd.to_datetime(raw_doc['timestamp_utc']),
                        'api_source': 'cyclocity_gbfs',
                        'utilization': 0.0,
                        'status': station.get('status', 'active')
                    }
                    snapshot_data.append(row)

            all_enriched_data.extend(snapshot_data)
        except Exception as e:
            print(f"Error while cleaning: {e}")

    df_realtime = pd.DataFrame(all_enriched_data)
    print(f"\n      TOTAL: {len(df_realtime):,} rows across {df_realtime['snapshot_id'].nunique()} snapshots")

    df_realtime['utilization'] = (df_realtime['num_bikes_available'] / df_realtime['capacity']).fillna(0).round(4)
    df_realtime['snapshot_id'] = df_realtime['snapshot_id'].astype(int)
    df_realtime['station_id'] = df_realtime['station_id'].astype(int)
    df_realtime['num_bikes_available'] = df_realtime['num_bikes_available'].astype(int)
    df_realtime['num_docks_available'] = df_realtime['num_docks_available'].astype(int)
    df_realtime['capacity'] = df_realtime['capacity'].astype(int)
    return df_realtime
