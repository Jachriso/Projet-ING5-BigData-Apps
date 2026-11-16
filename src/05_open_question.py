# 05_open_question.py
# Step 5 - Open question: which film genres travel best internationally once
# popularity is taken into account, and has this changed between 1980 and 2025?
#
# International reach of a film = number of distinct regions where it has a
# title in title.akas (a proxy for international distribution). Popular films
# have far more regional titles, so each film is compared with the films of the
# same popularity (vote bucket) and decade: export index = regions / expected.
# The answer is built in steps, each one relying on the previous one:
#   1. raw reach and its dependence on popularity
#   2. genre ranking with the popularity-adjusted export index
#   3. robustness of that ranking to IMDb's machine-translated titles
#   4. evolution of the ranking by decade
#   5. link between reach and rating
#   6. sanity check on the most exported films
# Results are written to reports/05_open_question.txt.
#
# Run from the project root: python src/05_open_question.py

import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config
from helpers import write, read_clean

REPORT_FILE = config.REPORTS_DIR / "05_open_question.txt"

# Scope (same film definition as the enrichment step)
FIRST_YEAR, LAST_YEAR = 1980, 2025
MIN_FEATURE_RUNTIME = 40          # feature-length film, in minutes
MIN_GENRE_FILMS = 500             # genres with fewer films are left out
MIN_CELL_FILMS = 100              # minimum films for a (genre, decade) value

# Regions
EXCLUDED_REGIONS = ["XWW"]        # "World-wide" is not a country
# IMDb adds machine-translated display titles for these 7 regions as a block
# (mostly on popular films). Used only for the robustness check.
AUTO_BLOCK_REGIONS = ["IN", "DE", "JP", "FR", "ES", "IT", "PT"]


# A. Data

def select_films(spark):
    """Rated feature-length films 1980-2025, with their decade and vote bucket."""
    basics = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")
    return (
        basics
        .filter((F.col("titleType") == "movie")
                & (F.col("runtimeMinutes") >= MIN_FEATURE_RUNTIME)
                & F.col("startYear").between(FIRST_YEAR, LAST_YEAR)
                & F.col("genres").isNotNull())
        .join(ratings, "tconst")
        .select("tconst", "primaryTitle", "startYear", "genres", "numVotes", "averageRating")
        .withColumn("decade", (F.floor(F.col("startYear") / 10) * 10).cast("int"))
        # Popularity bucket: 1-9, 10-99, ..., 100,000+ votes
        .withColumn("votes_bucket", F.least(F.floor(F.log10("numVotes")), F.lit(5)).cast("int"))
    )


def count_regions(spark, films):
    """Distinct regions per film, in one pass over title.akas (three variants)."""
    akas = (
        read_clean(spark, "title.akas")
        .filter(F.col("region").isNotNull() & ~F.col("region").isin(EXCLUDED_REGIONS))
        .select(F.col("titleId").alias("tconst"), "region")
        .join(films.select("tconst"), "tconst", "left_semi")
    )
    in_block = F.col("region").isin(AUTO_BLOCK_REGIONS)
    return akas.groupBy("tconst").agg(
        F.countDistinct("region").alias("regions"),
        F.countDistinct(F.when(~in_block, F.col("region"))).alias("regions_excl_block"),
        F.countDistinct(F.when(in_block, F.col("region"))).alias("block_regions"),
    )


def add_index(df, column, name):
    """Export index: regions of the film / mean regions of the films with the
    same vote bucket and decade (1.00 = as expected, 1.20 = 20 % more)."""
    expected = (df.groupBy("votes_bucket", "decade")
                  .agg(F.avg(column).alias("_expected")))
    return (df.join(expected, ["votes_bucket", "decade"])
              .withColumn(name, F.col(column) / F.col("_expected"))
              .drop("_expected"))


def by_genre(df):
    """One row per (film, genre)."""
    return df.withColumn("genre", F.explode("genres"))


# B. Report sections

def show(report, df):
    """Write a small DataFrame as an aligned text table (no pandas needed)."""
    plain = {"startYear", "adjusted_rank", "raw_rank", "export_quintile"}   # no thousands separator
    rows = df.collect()
    cells = [df.columns] + [["" if v is None
                             else f"{v:,}" if isinstance(v, int) and c not in plain
                             else str(v)
                             for c, v in zip(df.columns, row)] for row in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(df.columns))]
    for r in cells:
        write(report, "  " + "  ".join(v.rjust(w) for v, w in zip(r, widths)))
    write(report)


def section_scope(report, films, akas_regions):
    total = films.count()
    no_region = films.filter(F.col("regions") == 0).count()
    write(report, "Scope")
    write(report, f"  Feature-length rated films {FIRST_YEAR}-{LAST_YEAR}: {total:,}")
    write(report, f"  Films with no regional title: {no_region:,} ({100 * no_region / total:.1f} %)")
    write(report, f"  Films with at least one regional title: {akas_regions:,}")
    write(report, f"  Excluded pseudo-regions: {', '.join(EXCLUDED_REGIONS)}")
    write(report)


def section_popularity(report, films):
    corr_votes = films.select(F.corr("regions", F.log10("numVotes"))).first()[0]
    write(report, "Step 1. Raw international reach and popularity")
    write(report, f"  corr(regions, log10(numVotes)) = {corr_votes:.2f}")
    show(report, films.groupBy("votes_bucket").agg(
            F.count("*").alias("films"),
            F.round(F.avg("regions"), 1).alias("avg_regions"),
            F.expr("percentile(regions, 0.5)").alias("median_regions"),
            F.round(100 * F.avg((F.col("block_regions") == len(AUTO_BLOCK_REGIONS)).cast("int")), 1)
             .alias("pct_auto_block"))
         .withColumn("votes", F.concat(F.pow(10, "votes_bucket").cast("long"), F.lit("+")))
         .orderBy("votes_bucket")
         .select("votes", "films", "avg_regions", "median_regions", "pct_auto_block"))


def section_genres(report, films):
    """Raw ranking vs popularity-adjusted ranking."""
    g = (by_genre(films).groupBy("genre").agg(
            F.count("*").alias("films"),
            F.expr("percentile(numVotes, 0.5)").cast("long").alias("median_votes"),
            F.round(F.avg("regions"), 1).alias("avg_regions"),
            F.round(F.avg("export_index"), 2).alias("export_index"))
         .filter(F.col("films") >= MIN_GENRE_FILMS))
    g = (g.withColumn("raw_rank", F.rank().over(_order("avg_regions")))
          .withColumn("adjusted_rank", F.rank().over(_order("export_index")))
          .orderBy("adjusted_rank"))
    write(report, f"Step 2. Genres ranked once popularity and decade are taken into account "
                  f"(genres with >= {MIN_GENRE_FILMS} films)")
    write(report, "  avg_regions = raw mean; export_index = popularity- and decade-adjusted")
    show(report, g.select("adjusted_rank", "genre", "films", "median_votes",
                          "avg_regions", "raw_rank", "export_index"))
    return [r.genre for r in g.select("genre").collect()]


def _order(column):
    """Descending ranking over a small table (one row per genre)."""
    return Window.orderBy(F.desc(column))


def section_decades(report, films, genres):
    """Export index per genre and decade: has the ranking changed?"""
    decades = list(range((FIRST_YEAR // 10) * 10, LAST_YEAR + 1, 10))
    cells = (by_genre(films).filter(F.col("genre").isin(genres))
             .groupBy("genre", "decade")
             .agg(F.count("*").alias("films"), F.avg("export_index").alias("idx"))
             .withColumn("idx", F.when(F.col("films") >= MIN_CELL_FILMS, F.round("idx", 2))))
    table = (cells.groupBy("genre").pivot("decade", decades).agg(F.first("idx"))
             .withColumn("_order", F.array_position(F.array(*[F.lit(g) for g in genres]), F.col("genre")))
             .orderBy("_order").drop("_order"))
    for d in decades:
        table = table.withColumnRenamed(str(d), f"{d}s")
    write(report, "Step 4. Evolution of the export index by decade (blank = fewer than "
                  f"{MIN_CELL_FILMS} films in the cell)")
    show(report, table)


def section_robustness(report, films):
    """Same index computed three ways to neutralise IMDb's machine-translated block."""
    full_block = F.col("block_regions") == len(AUTO_BLOCK_REGIONS)
    variants = {
        "all_regions": add_index(films, "regions", "v"),
        "without_block_regions": add_index(films, "regions_excl_block", "v"),
        "without_block_films": add_index(films.filter(~full_block), "regions", "v"),
    }
    result = None
    for name, df in variants.items():
        g = (by_genre(df).groupBy("genre")
             .agg(F.count("*").alias("films"), F.round(F.avg("v"), 2).alias(name))
             .filter(F.col("films") >= MIN_GENRE_FILMS))
        result = g if result is None else result.join(g.drop("films"), "genre", "left")
    blocked = films.filter(full_block).count()
    write(report, "Step 3. Robustness of the ranking to the machine-translated block of 7 regions "
                  f"({', '.join(AUTO_BLOCK_REGIONS)})")
    write(report, f"  Films with the full block: {blocked:,}")
    show(report, result.orderBy(F.desc("all_regions")))


def section_rating(report, films):
    """Do films that travel further get better ratings?"""
    c_raw = films.select(F.corr("regions", "averageRating")).first()[0]
    c_idx = films.select(F.corr("export_index", "averageRating")).first()[0]
    write(report, "Step 5. International reach and rating")
    write(report, f"  corr(regions, averageRating)      = {c_raw:+.2f}")
    write(report, f"  corr(export_index, averageRating) = {c_idx:+.2f}")
    quintiles = films.approxQuantile("export_index", [0.2, 0.4, 0.6, 0.8], 0.001)
    q = F.lit(1)
    for i, bound in enumerate(quintiles, start=2):
        q = F.when(F.col("export_index") >= bound, F.lit(i)).otherwise(q)
    show(report, films.withColumn("export_quintile", q).groupBy("export_quintile").agg(
            F.count("*").alias("films"),
            F.round(F.avg("export_index"), 2).alias("avg_index"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"))
         .orderBy("export_quintile"))


def section_examples(report, films, genres):
    """Most exported film of each of the top 3 genres, as a sanity check."""
    write(report, "Step 6. Sanity check: most exported films of the top 3 genres")
    w =Window.partitionBy("genre").orderBy(F.desc("regions"), F.desc("numVotes"))
    show(report, by_genre(films).filter(F.col("genre").isin(genres[:3]))
         .withColumn("r", F.row_number().over(w)).filter(F.col("r") <= 3)
         .select("genre", "primaryTitle", "startYear", "numVotes", "regions",
                 F.round("export_index", 2).alias("export_index"))
         .orderBy(F.array_position(F.array(*[F.lit(g) for g in genres[:3]]), F.col("genre")), "r"))


# Pipeline

def main():
    spark = (
        SparkSession.builder.appName("05_open_question")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    films = select_films(spark)
    regions = count_regions(spark, films).cache()
    films = (films.join(regions, "tconst", "left")
             .fillna(0, ["regions", "regions_excl_block", "block_regions"]))
    films = add_index(films, "regions", "export_index").cache()

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        write(report, "IMDb - Open question: which genres travel best internationally?")
        write(report)
        section_scope(report, films, regions.count())
        section_popularity(report, films)
        genres = section_genres(report, films)
        section_robustness(report, films)
        section_decades(report, films, genres)
        section_rating(report, films)
        section_examples(report, films, genres)

    spark.stop()


if __name__ == "__main__":
    main()
