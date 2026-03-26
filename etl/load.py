from io import StringIO
from sqlalchemy import create_engine, text

'''def load_historical_to_postgres(df_csv, engine):
    print("   1.3 Bulk Insert to PostgreSQL...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE historical_stations RESTART IDENTITY CASCADE"))
        conn.commit()
    columns_to_store = ['station_id', 'name', 'capacity', 'lat', 'lon', 
                       'last_reported', 'num_bikes_available', 'num_docks_available',
                       'utilization', 'imbalance', 'hour', 'weekday']

    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()
        output = StringIO()
        df_csv[columns_to_store].to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cursor.copy_from(output, 'historical_stations', null='', columns=columns_to_store)
        connection.commit()
    finally:
        connection.close()
    print("The HISTORICAL CSV (2024-09-01) is successfully stored into POSTGRESQL")'''

def load_historical_to_postgres(df_csv, engine):
    print("   1.3 Bulk Insert to PostgreSQL...")

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE historical_stations RESTART IDENTITY CASCADE"))
        conn.commit()

    columns_to_store = [
        'station_id', 'name', 'capacity', 'lat', 'lon',
        'last_reported', 'num_bikes_available', 'num_docks_available',
        'utilization', 'imbalance', 'hour', 'weekday'
    ]

    df_csv[columns_to_store].to_sql(
        "historical_stations",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi"
    )

    print("Historical data successfully loaded")


def load_realtime_to_postgres(df_realtime, engine):
    print("   2.3 Bulk Insert 2000+ rows to PostgreSQL...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE realtime_stations RESTART IDENTITY CASCADE"))
        conn.commit()
    df_realtime.to_sql('realtime_stations', engine, if_exists='append', 
                      index=False, method='multi', chunksize=1000)
    print("20-SNAPSHOT GBFS data → Stored in MONGODB(RAW) → Cleaned and moved to POSTGRESQL is COMPLETE")
