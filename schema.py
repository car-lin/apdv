from sqlalchemy import create_engine, text

def create_tables(engine):

    historical_table_sql = """
    CREATE TABLE IF NOT EXISTS historical_stations (
        station_id INTEGER NOT NULL,
        name VARCHAR(255),
        capacity INTEGER NOT NULL,
        lat DOUBLE PRECISION NOT NULL,
        lon DOUBLE PRECISION NOT NULL,
        last_reported TIMESTAMP NOT NULL,
        num_bikes_available INTEGER NOT NULL,
        num_docks_available INTEGER NOT NULL,
        utilization NUMERIC(5,4),
        imbalance NUMERIC(6,2),
        hour INTEGER,
        weekday VARCHAR(20),
        ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (station_id, last_reported)
    );
    """

    realtime_table_sql = """
    CREATE TABLE IF NOT EXISTS realtime_stations (
        snapshot_id INTEGER NOT NULL,
        station_id INTEGER NOT NULL,
        name VARCHAR(255),
        capacity INTEGER NOT NULL,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        num_bikes_available INTEGER NOT NULL,
        num_docks_available INTEGER NOT NULL,
        is_installed BOOLEAN,
        is_renting BOOLEAN,
        is_returning BOOLEAN,
        last_reported TIMESTAMP,
        fetch_timestamp TIMESTAMP NOT NULL,
        api_source VARCHAR(50),
        utilization NUMERIC(5,4),
        status VARCHAR(20),
        ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (snapshot_id, station_id)
    );
    """

    print("Creating PostgreSQL Tables...")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS historical_stations CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS realtime_stations CASCADE"))
        conn.execute(text(historical_table_sql))
        conn.execute(text(realtime_table_sql))
        conn.commit()
    print("Tables Created!")
