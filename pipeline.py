from sqlalchemy import create_engine
from extract import extract_historical_csv, fetch_and_store_gbfs_snapshots
from transform import clean_historical, clean_realtime_data
from load import load_historical_to_postgres, load_realtime_to_postgres
from schema import create_tables
import config

def run_pipeline():
    print("Starting Dublin Bikes ETL pipeline...")

    engine = create_engine(
        config.POSTGRES_URI,
        connect_args={"sslmode": "require"},
        pool_pre_ping=True,
        pool_recycle=300,
    )

    # Setup tables
    print("Step 1: Setting up database tables...")
    create_tables(engine)

    # Historical ETL
    print("Step 2: Running historical ETL...")
    df_raw = extract_historical_csv()
    df_clean = clean_historical(df_raw)
    load_historical_to_postgres(df_clean, engine)
    print(f"  Inserted {len(df_clean)} historical rows")

    # Realtime ETL
    print("Step 3: Running realtime ETL...")
    fetch_and_store_gbfs_snapshots()
    df_realtime = clean_realtime_data()
    load_realtime_to_postgres(df_realtime, engine)
    print(f"  Inserted {len(df_realtime)} realtime rows")

    print("ETL pipeline complete!")

if __name__ == "__main__":
    run_pipeline()
