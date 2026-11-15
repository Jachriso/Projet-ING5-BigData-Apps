# 01_clean.py
# Step 1 - Cleaning: reads data/raw/, computes the % of missing values,
# cleans each table with its own function and writes the result as Parquet
# to data/clean/. Results are written to reports/01_data_preparation.txt.
#
# Run from the project root: python src/01_clean.py

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the project root to the import path so that config.py can be imported
sys.path.append(str(Path(__file__).resolve().parent.parent))
import config
import helpers


# Pipeline steps

def read_raw(spark, name):
    # quote="": IMDb does not use quotes as delimiters. Without this option,
    # a title containing " can merge several lines into one.
    path = (config.RAW_DIR / f"{name}.tsv.gz").as_posix()
    return spark.read.csv(path, sep="\t", header=True, nullValue="\\N", quote="")


def report_nulls(df, name, report):
    """Write the % of null / empty values per column and return [(%, table, column)]."""
    total = df.count()
    row = df.select([
        F.round(
            F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)) / total * 100,
            2,
        ).alias(c)
        for c in df.columns
    ]).first()

    helpers.write(report, f"{name} ({total:,} rows)")
    for c in df.columns:
        helpers.write(report, f"  {c:<20} {row[c]:>6.2f} %")
    helpers.write(report)
    return [(row[c], name, c) for c in df.columns]


def clean_common(df, keys):
    """Cleaning shared by all tables."""
    # Empty strings -> null: a single representation of a missing value
    df = df.select([
        F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)).alias(c)
        for c in df.columns
    ])
    # Rows without a primary key are unusable
    df = df.dropna(subset=keys)
    # Duplicated primary keys: keep only one row
    return df.dropDuplicates(keys)


# Table-specific cleaning

def clean_name_basics(df):
    df = clean_common(df, ["nconst"])
    df = helpers.to_int(df, ["birthYear", "deathYear"])
    df = helpers.to_list(df, ["primaryProfession", "knownForTitles"])
    # Death before birth: flag it, and keep neither of the two years
    df = df.withColumn("dq_death_before_birth",
                       F.coalesce(F.col("deathYear") < F.col("birthYear"), F.lit(False)))
    for c in ["birthYear", "deathYear"]:
        df = df.withColumn(c, F.when(F.col("dq_death_before_birth"), None).otherwise(F.col(c)))
    return df


def clean_title_basics(df):
    df = clean_common(df, ["tconst"])
    df = helpers.to_int(df, ["isAdult", "startYear", "endYear", "runtimeMinutes"])
    df = helpers.to_list(df, ["genres"])
    df = df.withColumn("isAdult", F.col("isAdult") == 1)
    # Zero or negative runtime is impossible -> null
    df = df.withColumn("runtimeMinutes",
                       F.when(F.col("runtimeMinutes") <= 0, None).otherwise(F.col("runtimeMinutes")))
    # End year before start year: flag it, and remove the end year
    df = df.withColumn("dq_end_before_start",
                       F.coalesce(F.col("endYear") < F.col("startYear"), F.lit(False)))
    df = df.withColumn("endYear",
                       F.when(F.col("dq_end_before_start"), None).otherwise(F.col("endYear")))
    return df


def clean_title_akas(df):
    df = clean_common(df, ["titleId", "ordering"])
    df = helpers.to_int(df, ["ordering", "isOriginalTitle"])
    df = df.withColumn("isOriginalTitle", F.col("isOriginalTitle") == 1)
    # In title.akas, list items are separated by the \x02 character
    return helpers.to_list(df, ["types", "attributes"], sep="\x02")


def clean_title_crew(df):
    df = clean_common(df, ["tconst"])
    return helpers.to_list(df, ["directors", "writers"])


def clean_title_episode(df):
    df = clean_common(df, ["tconst"])
    return helpers.to_int(df, ["seasonNumber", "episodeNumber"])


def clean_title_principals(df):
    df = clean_common(df, ["tconst", "ordering"])
    df = helpers.to_int(df, ["ordering"])
    # characters is stored as JSON: ["Self"] -> list
    return df.withColumn("characters", F.from_json(F.col("characters"), "array<string>"))


def clean_title_ratings(df):
    df = clean_common(df, ["tconst"])
    df = helpers.to_int(df, ["numVotes"])
    df = df.withColumn("averageRating", F.col("averageRating").try_cast("double"))
    # Rating outside the IMDb scale (1 to 10) -> null
    return df.withColumn("averageRating",
                         F.when(F.col("averageRating").between(1, 10), F.col("averageRating")))


CLEANERS = {
    "name.basics": clean_name_basics,
    "title.basics": clean_title_basics,
    "title.akas": clean_title_akas,
    "title.crew": clean_title_crew,
    "title.episode": clean_title_episode,
    "title.principals": clean_title_principals,
    "title.ratings": clean_title_ratings,
}


def save(df, name):
    path = (config.CLEAN_DIR / f"{name}.parquet").as_posix()
    df.write.mode("overwrite").parquet(path)


def main():
    spark = (
        SparkSession.builder.appName("01_clean")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    # "w": the file is created, or overwritten if it already exists.
    # The "with" block closes and saves it automatically at the end.
    with open(config.CLEAN_REPORT_FILE, "w", encoding="utf-8") as report:
        helpers.write(report, "IMDb - Data preparation")
        helpers.write(report)
        helpers.write(report, "Q1. Percentage of null / missing / empty values per column")
        helpers.write(report)

        all_nulls = []
        for name, clean in CLEANERS.items():
            df = read_raw(spark, name)
            all_nulls += report_nulls(df, name, report)
            df = clean(df)
            helpers.save(df, name)

        helpers.write(report, "Q2. Five columns with the highest percentage of null values")
        for pct, name, c in sorted(all_nulls, reverse=True)[:5]:
            helpers.write(report, f"  {name}.{c:<20} {pct:>6.2f} %")

    spark.stop()


if __name__ == "__main__":
    main()
