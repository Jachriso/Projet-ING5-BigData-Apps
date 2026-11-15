# 03_explore.py
# Step 3 - Exploration: joins, aggregates and indicators on the cleaned
# IMDb data. Results are written to reports/03_exploration.txt.
#
# Run from the project root: python notebooks/03_explore.py
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config

REPORT_FILE = config.REPORTS_DIR / "03_exploration.txt"
MIN_FEATURE_RUNTIME = 40  # feature-length film (AMPAS / BFI definition)

Z_THRESHOLD = 2  # "notably different" = more than 2 standard deviations

FIRST_YEAR, LAST_YEAR = 1980, 2025
TOP_N = 3


def write(report, text=""):
    """Print a line to the terminal and write it to the report."""
    print(text)
    report.write(text + "\n")
    report.flush()


def read_clean(spark, name):
    path = (config.CLEAN_DIR / f"{name}.parquet").as_posix()
    return spark.read.parquet(path)


def build_movies(spark):
    """Feature-length movies with their rating (one row per movie)."""
    basics = read_clean(spark, "title.basics")
    ratings = read_clean(spark, "title.ratings")
    return (
        basics
        .filter((F.col("titleType") == "movie") & (F.col("runtimeMinutes") >= MIN_FEATURE_RUNTIME))
        .join(ratings, "tconst", "left")
        .select("tconst", "primaryTitle", "startYear", "runtimeMinutes", "genres", "averageRating", "numVotes")
    )


def explode_genres(movies):
    """One row per (movie, genre)."""
    return (movies
            .withColumn("genre", F.explode("genres"))
            .drop("genres"))


def q1_runtime_outliers(movie_genres, report):
    """Return movies whose runtime is unusually long or short for their genre, 
    i.e. more than Z_THRESHOLD standard deviations away from the genre average."""
    stats = movie_genres.groupBy("genre").agg(
        F.avg("runtimeMinutes").alias("avg_runtime"),
        F.stddev("runtimeMinutes").alias("std_runtime"),
    )
    outliers = (
        movie_genres.join(stats, "genre")
        .withColumn("z_score", (F.col("runtimeMinutes") - F.col("avg_runtime")) / F.col("std_runtime"))
        .filter(F.abs(F.col("z_score")) > Z_THRESHOLD)
    )
    write(report, "\nQ1. Feature-length movies with an unusual runtime for their genre")
    write(report, f"  Outliers (|z| > {Z_THRESHOLD}): {outliers.count():,}")
    summary = (outliers.groupBy("genre")
               .agg(F.count("*").alias("outlier_count"), F.round(F.first("avg_runtime"), 1).alias("avg_runtime"))
               .orderBy(F.desc("outlier_count")))
    write(report, summary.toPandas().to_string(index=False))
    write(report)
    write(report, "  Most extreme examples:")
    top = (outliers.orderBy(F.desc(F.abs("z_score")))
           .select("primaryTitle", "startYear", "genre", "runtimeMinutes", F.round("z_score", 1).alias("z"))
           .limit(15))
    write(report, top.toPandas().to_string(index=False))
    write(report)


def q2_top_genres_per_year(movie_genres, report):
    """Return the 3 (TOP_N) most popular genres of each year, popularity being
    the total number of votes received by the genre's movies that year."""
    by_year = Window.partitionBy("startYear").orderBy(F.desc("total_votes"))
    top = (
        movie_genres
        .filter(F.col("startYear").between(FIRST_YEAR, LAST_YEAR))
        .groupBy("startYear", "genre")
        .agg(F.sum("numVotes").alias("total_votes"),
             F.count("*").alias("movie_count"))
        .withColumn("rank", F.row_number().over(by_year))
        .filter(F.col("rank") <= TOP_N)
        .orderBy("startYear", "rank")
    )
    write(report, f"\nQ2. Top {TOP_N} genres per year ({FIRST_YEAR}-{LAST_YEAR}), by total votes")
    write(report, top.toPandas().to_string(index=False))
    write(report)
    return top


def main():
    spark = (
        SparkSession.builder.appName("03_explore")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    movies = build_movies(spark).cache()
    movie_genres = explode_genres(movies).cache()

    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        write(report, "IMDb - Data exploration")
        write(report)
        write(report, f"Feature-length movies: {movies.count():,}")
        write(report, f"(movie, genre) rows:   {movie_genres.count():,}")
        
        # Questions, one function per question
        q1_runtime_outliers(movie_genres, report)
        top_genres = q2_top_genres_per_year(movie_genres, report)

    spark.stop()


if __name__ == "__main__":
    main()