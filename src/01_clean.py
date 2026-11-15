# 01_clean.py
# Step 1 - Cleaning: reads data/raw/, measures data quality, cleans each table
# with its own function and writes the result as Parquet to data/clean/.
# Answers to the "Data preparation" questions (Q1 to Q4) are written to
# reports/01_data_preparation.txt.
#
# Run from the project root: python src/01_clean.py

import sys
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the project root to the import path so that config.py can be imported
sys.path.append(str(Path(__file__).resolve().parent.parent))
import config

CURRENT_YEAR = date.today().year
SERIES_TYPES = ["tvSeries", "tvMiniSeries"]

# Primary key and numeric columns of each table (used by the quality checks)
KEYS = {
    "name.basics": ["nconst"],
    "title.basics": ["tconst"],
    "title.akas": ["titleId", "ordering"],
    "title.crew": ["tconst"],
    "title.episode": ["tconst"],
    "title.principals": ["tconst", "ordering"],
    "title.ratings": ["tconst"],
}
NUMERIC = {
    "name.basics": {"birthYear": "int", "deathYear": "int"},
    "title.basics": {"isAdult": "int", "startYear": "int", "endYear": "int", "runtimeMinutes": "int"},
    "title.akas": {"ordering": "int", "isOriginalTitle": "int"},
    "title.episode": {"seasonNumber": "int", "episodeNumber": "int"},
    "title.principals": {"ordering": "int"},
    "title.ratings": {"averageRating": "double", "numVotes": "int"},
}


# Helpers

def write(report, text=""):
    """Print a line to the terminal and write it to the report."""
    print(text, flush=True)
    report.write(text + "\n")
    report.flush()


def to_int(df, columns):
    """Cast columns to integer (unconvertible values become null)."""
    for c in columns:
        df = df.withColumn(c, F.col(c).try_cast("int"))
    return df


def to_list(df, columns, sep=","):
    """Split text columns "a,b,c" into lists ["a", "b", "c"]."""
    for c in columns:
        df = df.withColumn(c, F.split(F.col(c), sep))
    return df


def is_missing(c):
    """Null (\\N in the file) or empty string."""
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def num(c, kind="int"):
    """Raw text column read as a number (null when not convertible)."""
    return F.col(c).try_cast(kind)


def pct(part, total):
    return 100 * part / total if total else 0.0


def quality_rules():
    """Inconsistent values inside each table, checked on the raw data.
    (description, condition, what the cleaning does with these values)"""
    return {
        "name.basics": [
            ("deathYear < birthYear", num("deathYear") < num("birthYear"),
             "both years set to null, flagged in dq_death_before_birth"),
            (f"birthYear > {CURRENT_YEAR}", num("birthYear") > CURRENT_YEAR, "kept"),
            (f"deathYear > {CURRENT_YEAR}", num("deathYear") > CURRENT_YEAR, "kept"),
        ],
        "title.basics": [
            ("runtimeMinutes <= 0", num("runtimeMinutes") <= 0, "set to null"),
            ("runtimeMinutes > 1440 (over 24 hours)", num("runtimeMinutes") > 1440, "kept"),
            ("endYear < startYear", num("endYear") < num("startYear"),
             "endYear set to null, flagged in dq_end_before_start"),
            ("endYear set on a title that is not a series",
             num("endYear").isNotNull() & ~F.col("titleType").isin(SERIES_TYPES), "kept"),
            (f"startYear > {CURRENT_YEAR} (announced titles)", num("startYear") > CURRENT_YEAR, "kept"),
            ("isAdult not in (0, 1)", ~num("isAdult").isin(0, 1), "becomes false"),
        ],
        "title.akas": [
            ("isOriginalTitle not in (0, 1)", ~num("isOriginalTitle").isin(0, 1), "becomes false"),
        ],
        "title.episode": [
            ("episode is its own parent (tconst = parentTconst)",
             F.col("tconst") == F.col("parentTconst"), "kept"),
            ("negative seasonNumber or episodeNumber",
             (num("seasonNumber") < 0) | (num("episodeNumber") < 0), "kept"),
        ],
        "title.ratings": [
            ("averageRating not between 1 and 10",
             ~num("averageRating", "double").between(1, 10), "set to null"),
            ("numVotes < 0", num("numVotes") < 0, "kept"),
        ],
    }


# Pipeline steps

def read_raw(spark, name):
    # quote="": IMDb does not use quotes as delimiters. Without this option,
    # a title containing " can merge several lines into one.
    path = (config.RAW_DIR / f"{name}.tsv.gz").as_posix()
    return spark.read.csv(path, sep="\t", header=True, nullValue="\\N", quote="")


def profile(df, name, rules):
    """One single pass on the raw data: number of rows, missing values per
    column (Q1), missing keys, invalid numbers and inconsistent values (Q3)."""
    keys_missing = None
    for k in KEYS[name]:
        keys_missing = is_missing(k) if keys_missing is None else keys_missing | is_missing(k)

    aggs = [F.count(F.lit(1)).alias("__total"),
            F.count(F.when(keys_missing, 1)).alias("__keys_missing")]
    aggs += [F.count(F.when(is_missing(c), 1)).alias(f"missing:{c}") for c in df.columns]
    aggs += [F.count(F.when(~is_missing(c) & num(c, kind).isNull(), 1)).alias(f"badtype:{c}")
             for c, kind in NUMERIC.get(name, {}).items()]
    aggs += [F.count(F.when(cond, 1)).alias(f"rule:{i}") for i, (_, cond, _) in enumerate(rules)]
    return df.agg(*aggs).first().asDict()


def report_nulls(prof, columns, name, report):
    """Write the number and % of null / empty values per column (Q1)."""
    total = prof["__total"]
    write(report, f"{name} ({total:,} rows)")
    for c in columns:
        missing = prof[f"missing:{c}"]
        write(report, f"  {c:<20} {missing:>12,}  {pct(missing, total):>6.2f} %")
    write(report)
    return [(pct(prof[f"missing:{c}"], total), name, c) for c in columns]


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
    df = to_int(df, ["birthYear", "deathYear"])
    df = to_list(df, ["primaryProfession", "knownForTitles"])
    # Death before birth: flag it, and keep neither of the two years
    df = df.withColumn("dq_death_before_birth",
                       F.coalesce(F.col("deathYear") < F.col("birthYear"), F.lit(False)))
    for c in ["birthYear", "deathYear"]:
        df = df.withColumn(c, F.when(F.col("dq_death_before_birth"), None).otherwise(F.col(c)))
    return df


def clean_title_basics(df):
    df = clean_common(df, ["tconst"])
    df = to_int(df, ["isAdult", "startYear", "endYear", "runtimeMinutes"])
    df = to_list(df, ["genres"])
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
    df = to_int(df, ["ordering", "isOriginalTitle"])
    df = df.withColumn("isOriginalTitle", F.col("isOriginalTitle") == 1)
    # In title.akas, list items are separated by the \x02 character
    return to_list(df, ["types", "attributes"], sep="\x02")


def clean_title_crew(df):
    df = clean_common(df, ["tconst"])
    return to_list(df, ["directors", "writers"])


def clean_title_episode(df):
    df = clean_common(df, ["tconst"])
    return to_int(df, ["seasonNumber", "episodeNumber"])


def clean_title_principals(df):
    df = clean_common(df, ["tconst", "ordering"])
    df = to_int(df, ["ordering"])
    # characters is stored as JSON: ["Self"] -> list
    return df.withColumn("characters", F.from_json(F.col("characters"), "array<string>"))


def clean_title_ratings(df):
    df = clean_common(df, ["tconst"])
    df = to_int(df, ["numVotes"])
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


def read_clean(spark, name):
    path = (config.CLEAN_DIR / f"{name}.parquet").as_posix()
    return spark.read.parquet(path)


# Checks between tables (Q3), on the cleaned data

def count_orphans(df, column, ref, ref_column, explode=False):
    """Values of df.column that do not exist in ref.ref_column.
    Returns (orphans, total non-null values)."""
    values = df.select((F.explode(column) if explode else F.col(column)).alias("k"))
    values = values.filter(F.col("k").isNotNull())
    known = ref.select(F.col(ref_column).alias("k"))
    # left_anti join: keeps only the values that have no match in ref
    orphans = values.join(known, "k", "left_anti").count()
    return orphans, values.count()


def relation_checks(spark):
    """Foreign keys with missing parent records and cross-table inconsistencies."""
    t = {name: read_clean(spark, name) for name in CLEANERS}
    basics, names = t["title.basics"], t["name.basics"]
    principals, crew, episode = t["title.principals"], t["title.crew"], t["title.episode"]

    orphan_checks = [
        ("title.principals.tconst -> title.basics", principals, "tconst", basics, "tconst", False),
        ("title.principals.nconst -> name.basics", principals, "nconst", names, "nconst", False),
        ("title.ratings.tconst -> title.basics", t["title.ratings"], "tconst", basics, "tconst", False),
        ("title.crew.tconst -> title.basics", crew, "tconst", basics, "tconst", False),
        ("title.crew.directors -> name.basics", crew, "directors", names, "nconst", True),
        ("title.crew.writers -> name.basics", crew, "writers", names, "nconst", True),
        ("title.episode.tconst -> title.basics", episode, "tconst", basics, "tconst", False),
        ("title.episode.parentTconst -> title.basics", episode, "parentTconst", basics, "tconst", False),
        ("title.akas.titleId -> title.basics", t["title.akas"], "titleId", basics, "tconst", False),
        ("name.basics.knownForTitles -> title.basics", names, "knownForTitles", basics, "tconst", True),
    ]
    results = []
    for label, df, column, ref, ref_column, explode in orphan_checks:
        print(f"  checking {label}", flush=True)
        orphans, total = count_orphans(df, column, ref, ref_column, explode)
        results.append((f"Missing parent record: {label}", orphans, total))

    print("  checking cross-table consistency", flush=True)
    credited = principals.select("nconst").distinct()
    died_before_born = credited.join(names.filter("dq_death_before_birth").select("nconst"), "nconst")
    results.append(("Principals (distinct people) who died before they were born",
                    died_before_born.count(), credited.count()))

    credits = (
        principals.select("tconst", "nconst")
        .join(names.filter(F.col("birthYear").isNotNull()).select("nconst", "birthYear"), "nconst")
        .join(basics.filter(F.col("startYear").isNotNull()).select("tconst", "startYear"), "tconst")
    )
    results.append(("Principal credits on a title released before the person was born",
                    credits.filter(F.col("startYear") < F.col("birthYear")).count(), credits.count()))

    episodes = episode.join(basics.select("tconst", "titleType"), "tconst")
    results.append(("Episodes whose titleType is not 'tvEpisode'",
                    episodes.filter(F.col("titleType") != "tvEpisode").count(), episodes.count()))
    return results


def main():
    spark = (
        SparkSession.builder.appName("01_clean")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    rules = quality_rules()

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    # "w": the file is created, or overwritten if it already exists.
    # The "with" block closes and saves it automatically at the end.
    with open(config.CLEAN_REPORT_FILE, "w", encoding="utf-8") as report:
        write(report, "IMDb - Data preparation")
        write(report)
        write(report, "Q1. Number and percentage of null / missing / empty values per column")
        write(report)

        all_nulls, profiles, clean_rows = [], {}, {}
        for name, clean in CLEANERS.items():
            df = read_raw(spark, name)
            profiles[name] = profile(df, name, rules.get(name, []))
            all_nulls += report_nulls(profiles[name], df.columns, name, report)
            save(clean(df), name)
            clean_rows[name] = read_clean(spark, name).count()

        write(report, "Q2. Five columns with the highest percentage of null values")
        for value, name, c in sorted(all_nulls, reverse=True)[:5]:
            write(report, f"  {name + '.' + c:<35} {value:>6.2f} %")
        write(report)

        write(report, "Q3. Other data quality issues")
        write(report)
        write(report, "Primary keys")
        for name, prof in profiles.items():
            duplicated = prof["__total"] - prof["__keys_missing"] - clean_rows[name]
            write(report, f"  {name:<18} key ({', '.join(KEYS[name])}): "
                          f"{prof['__keys_missing']:,} missing, {duplicated:,} duplicated")
        write(report)
        write(report, "Values that are not valid numbers")
        for name, prof in profiles.items():
            for c in NUMERIC.get(name, {}):
                write(report, f"  {name + '.' + c:<35} {prof[f'badtype:{c}']:>12,}")
        write(report)
        write(report, "Inconsistent values inside a table")
        for name, prof in profiles.items():
            for i, (description, _, _) in enumerate(rules.get(name, [])):
                n = prof[f"rule:{i}"]
                write(report, f"  {name:<16} {description:<52} {n:>10,} ({pct(n, prof['__total']):.4f} %)")
        write(report)
        write(report, "Relations between tables")
        for label, n, total in relation_checks(spark):
            write(report, f"  {label}")
            write(report, f"      {n:,} / {total:,} ({pct(n, total):.4f} %)")
        write(report)

        write(report, "Q4. Handling applied by the pipeline")
        write(report)
        write(report, f"  {'table':<18}{'raw rows':>14}{'clean rows':>14}{'dropped':>12}")
        for name, prof in profiles.items():
            dropped = prof["__total"] - clean_rows[name]
            write(report, f"  {name:<18}{prof['__total']:>14,}{clean_rows[name]:>14,}{dropped:>12,}")
        write(report)
        write(report, "  Inconsistent values and what the cleaning does with them")
        for name, prof in profiles.items():
            for i, (description, _, action) in enumerate(rules.get(name, [])):
                write(report, f"  {name:<16} {description:<52} {prof[f'rule:{i}']:>10,}  -> {action}")
        write(report)
        write(report, "  - '\\N' and empty strings are stored as null; other null values are kept.")
        write(report, "  - Rows without a primary key are dropped; duplicated keys keep one row.")
        write(report, "  - Numeric columns are cast with try_cast: invalid numbers become null.")
        write(report, "  - List columns are split into arrays; characters is parsed as JSON.")
        write(report, "  - Missing parent records are kept and only counted (inner joins drop them later).")

    spark.stop()


if __name__ == "__main__":
    main()
