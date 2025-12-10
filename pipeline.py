from prefect import flow, task
from sqlalchemy import create_engine, text
from extract import extract_historical_csv, fetch_and_store_gbfs_snapshots
from transform import clean_historical, clean_realtime_data
from load import load_historical_to_postgres, load_realtime_to_postgres
from schema import create_tables
from urllib.parse import urlparse
import config
import psycopg2


POSTGRES_DSN = config.POSTGRES_DSN
parsed = urlparse(POSTGRES_DSN)
db_name = parsed.path.lstrip('/')
try:
    engine = create_engine(POSTGRES_DSN)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(f"Database '{db_name}' exists")
except:
    print(f"Database '{db_name}' is missing so creating it...")
    # Create database
    master_db = POSTGRES_DSN.rsplit("/", 1)[0] + "/postgres"
    print(f"Master DB: {master_db}")
    
    # Parse for psycopg2 connect params
    master_parsed = urlparse(master_db)
    conn_params = {
        'host': master_parsed.hostname,
        'port': master_parsed.port or 5432,
        'user': master_parsed.username,
        'password': master_parsed.password,
        'database': 'postgres'
    }
    
    # RAW psycopg2 connection → No transactions!
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True  
    with conn.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    conn.close()
    
    print(f"Database '{db_name}' created!")
    engine = create_engine(POSTGRES_DSN)
    '''master_engine = create_engine("postgresql://postgres:admin@localhost:5432/postgres")
    with master_engine.connect() as conn:
        query = "CREATE DATABASE " + db_name
        conn.execute(text(query))
        conn.commit()
    print("Database '{db_name}' created!")
    engine = create_engine(POSTGRES_DSN)'''

@task
def setup_database(engine):
    create_tables(engine) 

@task
def run_historical_etl():
    df_raw = extract_historical_csv()
    df_clean = clean_historical(df_raw)
    load_historical_to_postgres(df_clean, engine)
    return len(df_clean)

@task
def run_realtime_etl():
    fetch_and_store_gbfs_snapshots()
    df_clean = clean_realtime_data()
    load_realtime_to_postgres(df_clean, engine)
    return len(df_clean)

@flow
def full_dublin_bikes_etl():
    engine = create_engine(POSTGRES_DSN)
    setup_database(engine)
    hist_rows = run_historical_etl()
    realtime_rows = run_realtime_etl()
    print(f"Inserted {hist_rows} historical rows, {realtime_rows} realtime rows")

if __name__ == "__main__":
    full_dublin_bikes_etl()
