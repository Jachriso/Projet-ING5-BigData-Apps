# config.py
# Central project configuration: IMDb file URLs, data folder paths
# (raw / clean / enriched / gold) and global settings.
# Tip: keep the data/ folder outside OneDrive (e.g. C:\data\imdb) and set its
# path here to avoid syncing several GB of data.

from pathlib import Path

# Paths
ROOT_DIR = Path(__file__).resolve().parent

DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
ENRICHED_DIR = DATA_DIR / "enriched"
GOLD_DIR = DATA_DIR / "gold"

REPORTS_DIR = ROOT_DIR / "reports"
CLEAN_REPORT_FILE = REPORTS_DIR / "01_data_preparation.txt"

# IMDb source
IMDB_BASE_URL = "https://datasets.imdbws.com/"
IMDB_TABLES = [
    "name.basics",
    "title.akas",
    "title.basics",
    "title.crew",
    "title.episode",
    "title.principals",
    "title.ratings",
]

# Spark (local execution)
SPARK_DRIVER_MEMORY = "4g"
SPARK_SHUFFLE_PARTITIONS = 64
