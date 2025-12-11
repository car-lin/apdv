import os
# Configurations
POSTGRES_DSN = os.getenv("POSTGRES_URI", "postgresql://postgres:admin%40123@db.pmewpbajtowvustznmqs.supabase.co:5432/bikes")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://carline_db_user:admin%40123@bikessurplusdeficit.ahrspks.mongodb.net/?appName=bikesSurplusDeficit")

# Data Sources
CSV_URL = "https://data.smartdublin.ie/dataset/dublinbikes-api/resource/168f55b8-1c3d-4fd3-95b9-f92f388c772a/download"
GBFS_STATUS_URL = "https://api.cyclocity.fr/contracts/dublin/gbfs/station_status.json"
GBFS_INFO_URL = "https://api.cyclocity.fr/contracts/dublin/gbfs/station_information.json"

# ETL Settings
HISTORICAL_DATE = "2024-09-01"
GBFS_SNAPSHOTS = 20
