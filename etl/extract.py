import pandas as pd
import requests
import time
from datetime import datetime
import pymongo
import config

def extract_historical_csv():
    print("   1.1 Extracting from data.gov.ie...")
    csv_url = config.CSV_URL
    df_raw_csv = pd.read_csv(csv_url)
    print(f"      Raw: {len(df_raw_csv):,} rows, {len(df_raw_csv.columns)} columns")
    return df_raw_csv

def fetch_and_store_gbfs_snapshots(mongo_uri=config.MONGO_URI, snapshots=config.GBFS_SNAPSHOTS):
    # MongoDB connection (RAW storage)
    mongo_client = pymongo.MongoClient(mongo_uri)
    mongo_db = mongo_client["dublin_bikes"]
    raw_collection = mongo_db["raw_realtime_snapshots"]
    raw_collection.delete_many({})
    print("   Cleared old snapshots from MongoDB")

    station_status_url = config.GBFS_STATUS_URL
    station_info_url = config.GBFS_INFO_URL
    print("   STEP 1: Collecting RAW snapshots → MongoDB...")

    for snapshot_num in range(snapshots):
        print(f"      Snapshot {snapshot_num+1}/{snapshots}...", end=" ")
        try:
            response_status = requests.get(station_status_url, timeout=10)
            response_status.raise_for_status()
            data_status = response_status.json()

            response_info = requests.get(station_info_url, timeout=10)
            response_info.raise_for_status()
            data_info = response_info.json()

            print(f"Status: {len(data_status['data']['stations'])}, Info: {len(data_info['data']['stations'])}", end=" → ")

            snapshot_id = int(datetime.now().timestamp())
            raw_doc = {
                'snapshot_id': snapshot_id,
                'snapshot_num': snapshot_num,
                'timestamp_utc': datetime.utcnow().isoformat(),
                'status_raw': data_status,
                'info_raw': data_info,
                'status_count': len(data_status['data']['stations']),
                'info_count': len(data_info['data']['stations'])
            }
            raw_collection.insert_one(raw_doc)
            print(f"Snapshot {snapshot_id} is inserted into MongoDB")

        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

        if snapshot_num < snapshots - 1:
            time.sleep(1)
