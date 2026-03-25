from prefect import flow, task
from sqlalchemy import create_engine, text

from extract import extract_historical_csv, fetch_and_store_gbfs_snapshots
from transform import clean_historical, clean_realtime_data
from load import load_historical_to_postgres, load_realtime_to_postgres
from schema import create_tables
import config

POSTGRES_DSN = config.POSTGRES_URI


@task
def setup_database(engine):
    create_tables(engine)


@task
def run_historical_etl(engine):
    df_raw   = extract_historical_csv()
    df_clean = clean_historical(df_raw)
    load_historical_to_postgres(df_clean, engine)
    return len(df_clean)


@task
def run_realtime_etl(engine):
    fetch_and_store_gbfs_snapshots()
    df_clean = clean_realtime_data()
    load_realtime_to_postgres(df_clean, engine)
    return len(df_clean)


@flow
def full_dublin_bikes_etl():
    # engine is created once here and passed down to every task that needs it
    engine = create_engine(
        POSTGRES_DSN,
        connect_args={"sslmode": "require"},   # SSL required by Supabase
        pool_pre_ping=True,
        pool_recycle=300,
    )

    setup_database(engine)
    hist_rows     = run_historical_etl(engine)
    realtime_rows = run_realtime_etl(engine)

    print(f"ETL complete — {hist_rows} historical rows, {realtime_rows} realtime rows inserted.")


if __name__ == "__main__":
    full_dublin_bikes_etl()
