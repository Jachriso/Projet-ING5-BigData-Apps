# Step 3 - Enrichment: for feature-length films released between 1980 and 2025,
# adds a synopsis to the top 10 films per genre and year, and biographical
# information for 3 principals of each of these films.
# Source: Wikidata + English Wikipedia (human-authored, open licence).
# Results are written to data/enriched/ and reports/03_enrichment.txt.
# Paths and Spark settings come from config.py.


import json
import re
import sys
import time
from datetime import datetime, timezone
from itertools import batched
from pathlib import Path
from urllib.parse import quote, unquote

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
WIKIDATA_PAGE_URL = "https://www.wikidata.org/wiki/"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_PAGE_URL = "https://en.wikipedia.org/wiki/"
# Wikimedia policy: every client must send a descriptive User-Agent with a contact
USER_AGENT = "ECE-BigData-IMDb-Enrichment/1.0 (student project; contact: jade.parrilla@edu.ece.fr)"
REQUEST_DELAY = 0.3           # seconds between two requests (rate limit)
MAX_RETRIES = 5
BATCH_SIZE = 50               # IMDb ids per Wikidata query
EXTRACTS_PER_REQUEST = 20     # max pages per TextExtracts request (API limit)

# Section titles accepted as a film synopsis, in order of preference
SYNOPSIS_SECTIONS = ["Plot", "Plot summary", "Synopsis", "Premise", "Story"]

# Outputs
RAW_FILMS_FILE = config.ENRICHED_DIR / "raw" / "films.jsonl"
RAW_PEOPLE_FILE = config.ENRICHED_DIR / "raw" / "people.jsonl"
REPORT_FILE = config.REPORTS_DIR / "03_enrichment.txt"


# A. Selection (Spark)

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


# B. Gathering (outside Spark: plain Python + HTTP)

def http_get(url, params):
    """GET request that respects the Wikimedia rate limits. Returns the JSON body."""
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(REQUEST_DELAY)
        try:
            response = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(2 ** attempt)     # no answer at all: wait, then retry
            continue

        if response.status_code == 200:
            return response.json()

        if response.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt)     # wait longer than usual: the server asked us to slow down
            continue                     # next loop turn = new attempt

        response.raise_for_status()      # any other error (404, 400...): retrying will not help

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


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def wikipedia_url(title, revision=None):
    """Permanent link to a Wikipedia page (to the exact revision when known)."""
    url = WIKIPEDIA_PAGE_URL + quote(title.replace(" ", "_"))
    return f"{url}?oldid={revision}" if revision else url


def wikidata_lookup(imdb_ids, people=False):
    """Map IMDb ids (tt... / nm...) to their Wikidata item and English Wikipedia page.

    Wikidata stores the IMDb id in property P345: one SPARQL query handles a
    whole batch of ids. For people, the query also returns birth date (P569),
    death date (P570), birth place (P19) and occupations (P106).

    Returns {imdb_id: {"wikidata_id", "wikidata_url", "wikipedia_title", ...}}.
    Ids without a Wikidata item are absent from the result.
    """
    values = " ".join(f'"{i}"' for i in imdb_ids)
    person_fields = """
      OPTIONAL { ?item wdt:P569 ?birth . }
      OPTIONAL { ?item wdt:P570 ?death . }
      OPTIONAL { ?item wdt:P19 ?place . ?place rdfs:label ?placeLabel .
                 FILTER(LANG(?placeLabel) = "en") }
      OPTIONAL { ?item wdt:P106 ?occupation . ?occupation rdfs:label ?occupationLabel .
                 FILTER(LANG(?occupationLabel) = "en") }
    """ if people else ""
    query = f"""
    SELECT ?imdb ?item ?article ?birth ?death ?placeLabel ?occupationLabel WHERE {{
      VALUES ?imdb {{ {values} }}
      ?item wdt:P345 ?imdb .
      OPTIONAL {{ ?article schema:about ?item ;
                           schema:isPartOf <https://en.wikipedia.org/> . }}
      {person_fields}
    }}"""
    data = http_get(WIKIDATA_SPARQL_URL, {"query": query, "format": "json"})

    # One result row per combination of values (e.g. one per occupation):
    # merge them into one entry per IMDb id
    results = {}
    for row in data["results"]["bindings"]:
        value = lambda name: row[name]["value"] if name in row else None
        imdb_id = value("imdb")
        wikidata_id = value("item").rsplit("/", 1)[-1]
        article = value("article")
        entry = results.setdefault(imdb_id, {
            "wikidata_id": wikidata_id,
            "wikidata_url": WIKIDATA_PAGE_URL + wikidata_id,
            "wikipedia_title": None,
            "birth_date": None, "death_date": None, "birth_place": None,
            "occupations": [],
        })
        if article and not entry["wikipedia_title"]:
            entry["wikipedia_title"] = unquote(article.rsplit("/wiki/", 1)[-1]).replace("_", " ")
        if value("birth") and not entry["birth_date"]:
            entry["birth_date"] = value("birth")[:10]       # "1956-07-09T00:00:00Z" -> "1956-07-09"
        if value("death") and not entry["death_date"]:
            entry["death_date"] = value("death")[:10]
        if value("placeLabel") and not entry["birth_place"]:
            entry["birth_place"] = value("placeLabel")
        occupation = value("occupationLabel")
        if occupation and occupation not in entry["occupations"]:
            entry["occupations"].append(occupation)
    return results


def fetch_synopsis(wikipedia_title):
    """Plot section of a film's Wikipedia article, as plain text.

    The TextExtracts API returns the whole article as plain text, with section
    titles written "== Plot ==". The synopsis is the text between the first
    accepted section title (SYNOPSIS_SECTIONS) and the next "== ... ==" title.
    Returns (synopsis or None, source_url).
    """
    data = http_get(WIKIPEDIA_API_URL, {
        "action": "query", "format": "json", "formatversion": 2, "redirects": 1,
        "prop": "extracts|info", "explaintext": 1, "exsectionformat": "wiki",
        "titles": wikipedia_title,
    })
    page = data["query"]["pages"][0]
    if page.get("missing"):
        return None, wikipedia_url(wikipedia_title)
    source_url = wikipedia_url(page["title"], page.get("lastrevid"))

    text = page.get("extract", "")
    for section in SYNOPSIS_SECTIONS:
        match = re.search(rf"^==\s*{re.escape(section)}\s*==\s*$(.*?)(?=^==[^=]|\Z)",
                          text, flags=re.MULTILINE | re.DOTALL | re.IGNORECASE)
        if match and match.group(1).strip():
            return match.group(1).strip(), source_url
    return None, source_url


def fetch_biography(wikipedia_titles):
    """Introduction of each person's Wikipedia article, as plain text.

    The TextExtracts API returns the intro of up to 20 pages per request.
    Returns {requested title: (biography or None, source_url)}.
    """
    results = {}
    for chunk in batched(wikipedia_titles, EXTRACTS_PER_REQUEST):
        params = {
            "action": "query", "format": "json", "formatversion": 2, "redirects": 1,
            "prop": "extracts|info", "exintro": 1, "explaintext": 1, "exlimit": "max",
            "titles": "|".join(chunk),
        }
        pages, renamed = {}, {}
        while True:                                    # the API may answer in several parts
            data = http_get(WIKIPEDIA_API_URL, params)
            query = data.get("query", {})
            for change in query.get("normalized", []) + query.get("redirects", []):
                renamed[change["from"]] = change["to"]
            for page in query.get("pages", []):
                known = pages.setdefault(page["title"], page)
                if page.get("extract"):
                    known["extract"] = page["extract"]
            if "continue" not in data:
                break
            params = {**params, **data["continue"]}

        for title in chunk:
            final = title
            while final in renamed:                    # follow normalisation, then redirect
                final = renamed[final]
            page = pages.get(final, {})
            biography = (page.get("extract") or "").strip() or None
            source_url = (wikipedia_url(page["title"], page.get("lastrevid"))
                          if page and not page.get("missing") else wikipedia_url(title))
            results[title] = (biography, source_url)
    return results


def gather_films(tconsts):
    """Fetch the synopsis of each film and append one record per film to
    RAW_FILMS_FILE. Skip films already in the file.

    Record: {tconst, wikidata_id, wikidata_url, wikipedia_title, synopsis,
             source_url, note, retrieved_at}
    - source_url: lineage, URL the synopsis comes from (Wikipedia revision)
    - note: filled when information is missing
    A film whose requests fail is not written, so a re-run tries it again.
    """
    done = load_done_ids(RAW_FILMS_FILE, "tconst")
    todo = (t for t in tconsts if t not in done)
    count = len(done)
    for batch in batched(todo, BATCH_SIZE):
        try:
            lookup = wikidata_lookup(batch)
        except (requests.RequestException, RuntimeError) as error:
            print(f"  Wikidata lookup failed for {len(batch)} films, skipped: {error}")
            continue

        records = []
        for tconst in batch:
            info = lookup.get(tconst, {})
            record = {
                "tconst": tconst,
                "wikidata_id": info.get("wikidata_id"),
                "wikidata_url": info.get("wikidata_url"),
                "wikipedia_title": info.get("wikipedia_title"),
                "synopsis": None, "source_url": info.get("wikidata_url", WIKIDATA_SPARQL_URL),
                "note": None, "retrieved_at": now_utc(),
            }
            if not info:
                record["note"] = "No Wikidata item for this IMDb id"
            elif not info["wikipedia_title"]:
                record["note"] = "No English Wikipedia article"
            else:
                try:
                    synopsis, source_url = fetch_synopsis(info["wikipedia_title"])
                except (requests.RequestException, RuntimeError) as error:
                    print(f"  {tconst}: request failed, will be retried on next run ({error})")
                    continue
                record["synopsis"], record["source_url"] = synopsis, source_url
                if synopsis is None:
                    record["note"] = "No Plot section in the Wikipedia article"
            records.append(record)

        append_jsonl(RAW_FILMS_FILE, records)
        count += len(records)
        print(f"  films gathered: {count:,}", flush=True)


def gather_people(nconsts):
    """Same as gather_films for people: one record per person in RAW_PEOPLE_FILE.

    Record: {nconst, wikidata_id, wikidata_url, wikipedia_title, birth_date,
             death_date, birth_place, occupations, biography, source_url,
             note, retrieved_at}
    - wikidata_url: lineage of the facts (dates, place, occupations)
    - source_url: lineage of the biography (Wikipedia revision)
    """
    done = load_done_ids(RAW_PEOPLE_FILE, "nconst")
    todo = (n for n in nconsts if n not in done)
    count = len(done)
    for batch in batched(todo, BATCH_SIZE):
        try:
            lookup = wikidata_lookup(batch, people=True)
            titles = [i["wikipedia_title"] for i in lookup.values() if i["wikipedia_title"]]
            biographies = fetch_biography(titles)
        except (requests.RequestException, RuntimeError) as error:
            print(f"  lookup failed for {len(batch)} people, skipped: {error}")
            continue

        records = []
        for nconst in batch:
            info = lookup.get(nconst, {})
            title = info.get("wikipedia_title")
            biography, source_url = biographies.get(title, (None, info.get("wikidata_url", WIKIDATA_SPARQL_URL)))
            record = {
                "nconst": nconst,
                "wikidata_id": info.get("wikidata_id"),
                "wikidata_url": info.get("wikidata_url"),
                "wikipedia_title": title,
                "birth_date": info.get("birth_date"),
                "death_date": info.get("death_date"),
                "birth_place": info.get("birth_place"),
                "occupations": info.get("occupations", []),
                "biography": biography,
                "source_url": source_url,
                "note": None,
                "retrieved_at": now_utc(),
            }
            if not info:
                record["note"] = "No Wikidata item for this IMDb id"
            elif not title:
                record["note"] = "No English Wikipedia article (Wikidata facts only)"
            elif biography is None:
                record["note"] = "Empty Wikipedia introduction"
            records.append(record)

        append_jsonl(RAW_PEOPLE_FILE, records)
        count += len(records)
        print(f"  people gathered: {count:,}", flush=True)


# C. Cleaning and schema enforcement (Spark)

FILM_SCHEMA = StructType([
    StructField("tconst", StringType()),
    StructField("wikidata_id", StringType()),
    StructField("wikidata_url", StringType()),
    StructField("wikipedia_title", StringType()),
    StructField("synopsis", StringType()),
    StructField("source_url", StringType()),
    StructField("note", StringType()),
    StructField("retrieved_at", StringType()),
])

PEOPLE_SCHEMA = StructType([
    StructField("nconst", StringType()),
    StructField("wikidata_id", StringType()),
    StructField("wikidata_url", StringType()),
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


def clean_text(df, columns):
    """Trim texts, collapse runs of blank lines, and turn empty strings into null."""
    for c in columns:
        cleaned = F.trim(F.regexp_replace(F.col(c), r"\n\s*\n+", "\n\n"))
        df = df.withColumn(c, F.when(cleaned == "", None).otherwise(cleaned))
    return df


def fill_lineage(df):
    """Every record must cite a source: when nothing was found on Wikidata,
    the source is the Wikidata query service that was searched."""
    return df.withColumn("source_url", F.coalesce(F.col("source_url"), F.lit(WIKIDATA_SPARQL_URL)))


def clean_films(df):
    """Clean the gathered film records and enforce their types."""
    df = clean_text(df, ["wikipedia_title", "synopsis", "note"])
    return (
        fill_lineage(df)
        .dropDuplicates(["tconst"])
        .withColumn("retrieved_at", F.try_to_timestamp("retrieved_at"))
        .withColumn("note", F.when(F.col("synopsis").isNull() & F.col("note").isNull(),
                                   "No synopsis available").otherwise(F.col("note")))
    )


def clean_people(df):
    """Clean the gathered people records and enforce their types."""
    df = clean_text(df, ["wikipedia_title", "birth_place", "biography", "note"])
    return (
        fill_lineage(df)
        .dropDuplicates(["nconst"])
        .withColumn("birth_date", F.try_to_date("birth_date"))
        .withColumn("death_date", F.try_to_date("death_date"))
        .withColumn("retrieved_at", F.try_to_timestamp("retrieved_at"))
        .withColumn("note", F.when(F.col("biography").isNull() & F.col("note").isNull(),
                                   "No biography available").otherwise(F.col("note")))
    )


def report_coverage(report, title, df, text_column):
    """Write how many records have the enriched text, and the most frequent notes."""
    total = df.count()
    with_text = df.filter(F.col(text_column).isNotNull()).count()
    helpers.write(report, title)
    helpers.write(report, f"  records: {total:,}")
    helpers.write(report, f"  with {text_column}: {with_text:,} ({100 * with_text / total if total else 0:.1f} %)")
    for row in df.groupBy("note").count().orderBy(F.desc("count")).collect():
        if row["note"]:
            helpers.write(report, f"  note '{row['note']}': {row['count']:,}")
    helpers.write(report)


# Pipeline

def main():
    spark = (
        SparkSession.builder.appName("03_enrich")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        helpers.write(report, "IMDb - Data enrichment")
        helpers.write(report)

        # A. Selection (cached: reused for the ids and for the final joins)
        films = select_top_films(spark).cache()
        principals = select_principals(spark, films).cache()
        helpers.write(report, f"Selected (film, genre) rows: {films.count():,}")
        helpers.write(report, f"Selected (film, principal) rows: {principals.count():,}")
        helpers.write(report)

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

        report_coverage(report, "Films (synopsis)", film_info, "synopsis")
        report_coverage(report, "Principals (biography)", people_info, "biography")

    spark.stop()


if __name__ == "__main__":
    main()
