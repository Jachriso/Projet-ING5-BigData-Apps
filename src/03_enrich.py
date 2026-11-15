# Step 3 - Enrichment: for feature-length films released between 1980 and 2025,
# adds a synopsis to the top 10 films per genre and year, and biographical
# information for 3 principals of each of these films.
# Source: Wikidata + English Wikipedia (human-authored, open licence).
# Results are written to data/enriched/ and reports/03_enrichment.txt.
# Paths and Spark settings come from config.py.


import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import response

import requests
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# Add the project root to the import path so that config.py can be imported
sys.path.append(str(Path(__file__).resolve().parent.parent))
import config
import helpers

# Scope
FIRST_YEAR, LAST_YEAR = 1980, 2025
MIN_FEATURE_RUNTIME = 40      # feature-length film, in minutes
TOP_N_FILMS = 10              # films per (genre, year), ranked by popularity (numVotes)
N_PRINCIPALS = 3              # principals enriched per film

# Sources (Wikimedia)
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_PAGE_URL = "https://en.wikipedia.org/wiki/"
# Wikimedia policy: every client must send a descriptive User-Agent with a contact
USER_AGENT = "ECE-BigData-IMDb-Enrichment/1.0 (student project; contact: jade.parrilla@edu.ece.fr)"
REQUEST_DELAY = 1.0           # seconds between two requests (rate limit)
MAX_RETRIES = 5
BATCH_SIZE = 50               # IMDb ids per Wikidata query

# Outputs
RAW_FILMS_FILE = config.ENRICHED_DIR / "raw" / "films.jsonl"
RAW_PEOPLE_FILE = config.ENRICHED_DIR / "raw" / "people.jsonl"
REPORT_FILE = config.REPORTS_DIR / "03_enrichment.txt"



def select_top_films(spark):
    """Top TOP_N_FILMS feature-length films per (genre, year), 1980-2025."""
    basics = helpers.read_clean(spark, "title.basics")
    ratings = helpers.read_clean(spark, "title.ratings")

    movies = (
        basics
        .filter((F.col("titleType") == "movie")
                & (F.col("runtimeMinutes") >= MIN_FEATURE_RUNTIME)
                & F.col("startYear").between(FIRST_YEAR, LAST_YEAR))
        .join(ratings, "tconst")                      # inner: no votes -> cannot be "top"
        .withColumn("genre", F.explode("genres"))     # one row per (film, genre)
    )

    by_genre_year = (Window.partitionBy("startYear", "genre").orderBy(F.desc("numVotes"), "tconst"))
    return (
        movies
        .withColumn("rank", F.row_number().over(by_genre_year))
        .filter(F.col("rank") <= TOP_N_FILMS)
        .select("tconst", "primaryTitle", "startYear", "genre", "numVotes", "rank")
    )


def select_principals(spark, films):
    """The first N_PRINCIPALS distinct people credited on each selected film."""
    principals = helpers.read_clean(spark, "title.principals")
    film_ids = films.select("tconst").distinct()

    # A person credited several times on a film keeps only their best position
    people = (
        principals.join(film_ids, "tconst")
        .groupBy("tconst", "nconst")
        .agg(F.min("ordering").alias("ordering"),
             F.min_by("category", "ordering").alias("category"))
    )

    by_film = Window.partitionBy("tconst").orderBy("ordering")
    return (
        people
        .withColumn("rank", F.row_number().over(by_film))
        .filter(F.col("rank") <= N_PRINCIPALS)
        .select("tconst", "nconst", "category", "ordering")
    )


def http_get(url, params):
    """GET request that respects the Wikimedia rate limits. Returns the JSON body.    """
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(REQUEST_DELAY)
        response = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
        
        if response.status_code == 200:
            return response.json()

        if response.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt)     # wait longer than usual: the server asked us to slow down
            continue                     # next loop turn = new attempt
        
        response.raise_for_status()      # any other error ends the program immediately
    
    raise RuntimeError(f"Giving up after {MAX_RETRIES} attempts: {url}")


def load_done_ids(path, key):
    """Ids already present in a raw JSONL file, so that a re-run resumes where
    it stopped instead of calling the APIs again."""
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        return {json.loads(line)[key] for line in f if line.strip()}


def append_jsonl(path, records):
    """Append records (dicts) to a JSONL file, one JSON object per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def wikidata_lookup(imdb_ids, extra_fields=""):
    """Map IMDb ids (tt... / nm...) to their Wikidata item and English Wikipedia page.

    Wikidata stores the IMDb id in property P345. One SPARQL query handles a
    whole batch of ids. Example query:

        SELECT ?imdb ?item ?article WHERE {
          VALUES ?imdb { "tt0111161" "nm0000151" }
          ?item wdt:P345 ?imdb .
          OPTIONAL { ?article schema:about ?item ;
                              schema:isPartOf <https://en.wikipedia.org/> . }
        }

    For people, `extra_fields` can add birth date (P569), death date (P570),
    birth place (P19) and occupations (P106).

    Returns {imdb_id: {"wikidata_id": ..., "wikipedia_title": ..., ...}}.
    """
    # TODO: build the query, call http_get(WIKIDATA_SPARQL_URL,
    #       {"query": query, "format": "json"}), parse results.bindings
    ...


def fetch_synopsis(wikipedia_title):
    """Plot section of a film's Wikipedia article, as plain text.

    TODO:
      1. action=parse&page=<title>&prop=sections -> index of the "Plot" section
      2. action=parse&page=<title>&section=<index>&prop=wikitext (or the
         TextExtracts API) -> text of that section
    Returns (synopsis or None, source_url).
    """
    ...


def fetch_biography(wikipedia_titles):
    """Introduction of each person's Wikipedia article, as plain text.

    The TextExtracts API returns the intro of up to 20 pages per request:
    action=query&prop=extracts&exintro=1&explaintext=1&titles=A|B|C
    Returns {wikipedia_title: biography text}.
    """
    # TODO
    ...


def gather_films(tconsts):
    """Fetch the synopsis of each film and append one record per film to
    RAW_FILMS_FILE. Skip films already in the file.

    Record: {tconst, wikidata_id, wikipedia_title, synopsis,
             source_url, note, retrieved_at}
    - source_url: lineage, URL the information comes from (mandatory)
    - note: filled when information is missing,
            e.g. "No English Wikipedia article" or "No Plot section"
    """
    # TODO: for each batch of BATCH_SIZE ids not yet done:
    #       wikidata_lookup -> fetch_synopsis -> append_jsonl
    ...


def gather_people(nconsts):
    """Same as gather_films for people: one record per person in RAW_PEOPLE_FILE.

    Record: {nconst, wikidata_id, wikipedia_title, birth_date, death_date,
             birth_place, occupations, biography, source_url, note, retrieved_at}
    """
    # TODO
    ...


# C. Cleaning and schema enforcement (Spark)

FILM_SCHEMA = StructType([
    StructField("tconst", StringType()),
    StructField("wikidata_id", StringType()),
    StructField("wikipedia_title", StringType()),
    StructField("synopsis", StringType()),
    StructField("source_url", StringType()),
    StructField("note", StringType()),
    StructField("retrieved_at", StringType()),
])

PEOPLE_SCHEMA = StructType([
    StructField("nconst", StringType()),
    StructField("wikidata_id", StringType()),
    StructField("wikipedia_title", StringType()),
    StructField("birth_date", StringType()),
    StructField("death_date", StringType()),
    StructField("birth_place", StringType()),
    StructField("occupations", ArrayType(StringType())),
    StructField("biography", StringType()),
    StructField("source_url", StringType()),
    StructField("note", StringType()),
    StructField("retrieved_at", StringType()),
])


def load_raw(spark, path, schema):
    """Read a raw JSONL file with an explicit schema (no type guessing)."""
    return spark.read.schema(schema).json(path.as_posix())


def clean_films(df):
    """TODO: trim texts, empty strings -> null, drop duplicated tconst,
    retrieved_at -> timestamp, note = "No synopsis available" when synopsis is null."""
    ...


def clean_people(df):
    """TODO: same as clean_films, plus birth_date / death_date -> date."""
    ...


# Pipeline

def main():
    spark = (
        SparkSession.builder.appName("02_enrich")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        helpers.write(report, "IMDb - Data enrichment")
        helpers.write(report)

        # A. Selection
        films = select_top_films(spark)
        principals = select_principals(spark, films)

        # B. Gathering. toLocalIterator() streams the ids to Python instead of
        #    loading them all at once: the same code works on the full dataset.
        tconsts = (row.tconst for row in films.select("tconst").distinct().toLocalIterator())
        nconsts = (row.nconst for row in principals.select("nconst").distinct().toLocalIterator())
        gather_films(tconsts)
        gather_people(nconsts)

        # C. Cleaning, joins with IMDb and Parquet output
        film_info = clean_films(load_raw(spark, RAW_FILMS_FILE, FILM_SCHEMA))
        people_info = clean_people(load_raw(spark, RAW_PEOPLE_FILE, PEOPLE_SCHEMA))
        helpers.save(films.join(film_info, "tconst", "left"), "top_films")
        helpers.save(principals.join(people_info, "nconst", "left"), "top_films_principals")

        # TODO: write coverage figures to the report
        #       (number of films / people, % with synopsis / biography, top notes)

    spark.stop()


if __name__ == "__main__":
    main()
