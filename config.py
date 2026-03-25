import os

# ── Configurations ────────────────────────────────────────────────────────────
# All secrets must be set as environment variables (locally via .env or shell
# export; on Streamlit Cloud via App Settings → Secrets).
# NEVER hard-code credentials here.

POSTGRES_URI = os.getenv("POSTGRES_URI")
MONGO_URI    = os.getenv("MONGO_URI")

if not POSTGRES_URI:
    raise EnvironmentError(
        "POSTGRES_URI environment variable is not set. "
        "Add it to your .streamlit/secrets.toml or Streamlit Cloud secrets."
    )
if not MONGO_URI:
    raise EnvironmentError(
        "MONGO_URI environment variable is not set. "
        "Add it to your .streamlit/secrets.toml or Streamlit Cloud secrets."
    )

# ── Data Sources ──────────────────────────────────────────────────────────────
CSV_URL          = "https://data.smartdublin.ie/dataset/dublinbikes-api/resource/168f55b8-1c3d-4fd3-95b9-f92f388c772a/download"
GBFS_STATUS_URL  = "https://api.cyclocity.fr/contracts/dublin/gbfs/station_status.json"
GBFS_INFO_URL    = "https://api.cyclocity.fr/contracts/dublin/gbfs/station_information.json"

# ── ETL Settings ──────────────────────────────────────────────────────────────
HISTORICAL_DATE  = "2024-09-01"
GBFS_SNAPSHOTS   = 20
