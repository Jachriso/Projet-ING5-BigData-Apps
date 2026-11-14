# 03_explore.py
# Step 3 - Exploration: joins, aggregates and indicators on the cleaned
# IMDb data. Results are written to reports/03_exploration.txt.
#
# Run from the project root: python src/03_transform.py
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config

REPORT_FILE = config.REPORTS_DIR / "03_exploration.txt"
MIN_FEATURE_RUNTIME = 40  # feature-length film (AMPAS / BFI definition)


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
        .filter((F.col("titleType") == "movie")
                & (F.col("runtimeMinutes") >= MIN_FEATURE_RUNTIME))
        .join(ratings, "tconst", "left")
        .select("tconst", "primaryTitle", "startYear", "runtimeMinutes",
                "genres", "averageRating", "numVotes")
    )


def explode_genres(movies):
    """One row per (movie, genre)."""
    return (movies
            .withColumn("genre", F.explode("genres"))
            .drop("genres"))


def main():
    spark = (
        SparkSession.builder.appName("03_transform")
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
        # Questions will be added here, one function per question

    spark.stop()


if __name__ == "__main__":
    main()