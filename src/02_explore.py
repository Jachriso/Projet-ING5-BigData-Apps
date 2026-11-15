# 02_explore.py
# Step 2 - Exploration : joins, aggregates and indicators on the cleaned
# IMDb data. Results are written to reports/02_exploration.txt.
#
# Run from the project root: python src/02_explore.py

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config
import helpers

REPORT_FILE = config.REPORTS_DIR / "02_exploration.txt"
MIN_FEATURE_RUNTIME = 40  # feature-length film (AMPAS / BFI definition)

Z_THRESHOLD = 2  # "notably different" = more than 2 standard deviations

FIRST_YEAR, LAST_YEAR = 1980, 2025
TOP_N = 3

HIT_RATING, FLOP_AVG = 8.0, 6.0
MIN_OTHER_MOVIES = 2
MIN_VOTES = 100  # ignore movies rated by too few people

MIN_EPISODES, MIN_SEASONS = 10, 3
EARLY_SEASONS = 2

SUCCESS_RATING, SUCCESS_VOTES = 7.0, 10_000

MIN_CAREER_YEARS, MIN_CAREER_MOVIES = 20, 15
MIN_CAREER_VOTES = 1_000
MIN_RATING_DROP = 0.5  # early average must beat late average by this much

MIN_ROLE_MOVIES = 3  # min movies as director, writer or actor for that role to count


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
    helpers.write(report, "\nQ1. Feature-length movies with an unusual runtime for their genre")
    helpers.write(report, f"  Outliers (|z| > {Z_THRESHOLD}): {outliers.count():,}")
    summary = (outliers.groupBy("genre")
               .agg(F.count("*").alias("outlier_count"), F.round(F.first("avg_runtime"), 1).alias("avg_runtime"))
               .orderBy(F.desc("outlier_count")))
    helpers.write(report, summary.toPandas().to_string(index=False))
    helpers.write(report)
    helpers.write(report, "  Most extreme examples:")
    top = (outliers.orderBy(F.desc(F.abs("z_score")))
           .select("primaryTitle", "startYear", "genre", "runtimeMinutes", F.round("z_score", 1).alias("z"))
           .limit(15))
    helpers.write(report, top.toPandas().to_string(index=False))
    helpers.write(report)


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
    helpers.write(report, f"\nQ2. Top {TOP_N} genres per year ({FIRST_YEAR}-{LAST_YEAR}), by total votes")
    helpers.write(report, top.toPandas().to_string(index=False))
    helpers.write(report)
    return top


def q3_top_vs_rest(movie_genres, top_genres, report):
    """Compare, for each year, the top 3 (TOP_N) genres with all the other genres :
    share of total votes and average rating."""
    flags = top_genres.select("startYear", "genre", F.lit(True).alias("is_top"))
    rows = (
        movie_genres
        .filter(F.col("startYear").between(FIRST_YEAR, LAST_YEAR))
        .join(flags, ["startYear", "genre"], "left")
        .withColumn("is_top", F.coalesce("is_top", F.lit(False)))
    )
    is_top = F.col("is_top")
    per_year = (
        rows.groupBy("startYear")
        .agg(
            F.round(100 * F.sum(F.when(is_top, F.col("numVotes"))) / F.sum("numVotes"), 1).alias("top_vote_share_pct"),
            F.round(F.avg(F.when(is_top, F.col("averageRating"))), 2).alias("top_avg_rating"),
            F.round(F.avg(F.when(~is_top, F.col("averageRating"))), 2).alias("rest_avg_rating"),
        )
        .orderBy("startYear")
    )
    helpers.write(report, f"\nQ3. Top {TOP_N} genres vs the other genres, per year")
    helpers.write(report, per_year.toPandas().to_string(index=False))
    overall = rows.groupBy("is_top").agg(
        F.count("*").alias("movie_genre_rows"),
        F.sum("numVotes").alias("total_votes"),
        F.round(F.avg("averageRating"), 2).alias("avg_rating"),
    )
    helpers.write(report)
    helpers.write(report, f"  Overall {FIRST_YEAR}-{LAST_YEAR}:")
    helpers.write(report, overall.toPandas().to_string(index=False))
    helpers.write(report)


def q4_one_hit_directors(spark, movies, report):
    """Return directors with exactly one movie rated >= 8 (HIT_RATING) whose
    other movies (at least 2, MIN_OTHER_MOVIES) average <= 6 (FLOP_AVG)."""
    crew = read_clean(spark, "title.crew")
    names = read_clean(spark, "name.basics").select("nconst", "primaryName")
    rated = movies.filter(F.col("numVotes") >= MIN_VOTES)
    directed = (
        crew.select("tconst", F.explode("directors").alias("nconst"))
        .join(rated, "tconst")
    )
    is_hit = F.col("averageRating") >= HIT_RATING
    per_director = directed.groupBy("nconst").agg(
        F.sum(F.when(is_hit, 1).otherwise(0)).alias("hit_count"),
        F.first(F.when(is_hit, F.col("primaryTitle")), ignorenulls=True).alias("hit_title"),
        F.max(F.when(is_hit, F.col("averageRating"))).alias("hit_rating"),
        F.sum(F.when(~is_hit, 1).otherwise(0)).alias("other_count"),
        F.round(F.avg(F.when(~is_hit, F.col("averageRating"))), 2).alias("other_avg"),
    )
    result = (
        per_director
        .filter((F.col("hit_count") == 1) & (F.col("other_count") >= MIN_OTHER_MOVIES) & (F.col("other_avg") <= FLOP_AVG))
        .join(names, "nconst", "left")
        .select("primaryName", "hit_title", "hit_rating", "other_count", "other_avg")
        .orderBy(F.desc("hit_rating"))
    )
    helpers.write(report, f"\nQ4. Directors with one movie >= {HIT_RATING} and >= {MIN_OTHER_MOVIES} others averaging <= {FLOP_AVG}")
    helpers.write(report, f"  Directors found: {result.count():,}")
    helpers.write(report, result.limit(20).toPandas().to_string(index=False))
    helpers.write(report)


def q5_series_decline(spark, report):
    """Return TV series (>= 3 seasons of >= 10 episodes) whose worst-rated
    episode is in the last season and best-rated episode in seasons 1-2."""
    episodes = (read_clean(spark, "title.episode")
                .filter(F.col("seasonNumber").isNotNull()))
    ratings = read_clean(spark, "title.ratings")
    titles = read_clean(spark, "title.basics").select("tconst", "primaryTitle")

    seasons = (episodes.groupBy("parentTconst", "seasonNumber")
               .agg(F.count("*").alias("episode_count"))
               .filter(F.col("episode_count") >= MIN_EPISODES))
    series = (seasons.groupBy("parentTconst")
              .agg(F.count("*").alias("season_count"), F.max("seasonNumber").alias("last_season"))
              .filter(F.col("season_count") >= MIN_SEASONS))
    rated = (
        episodes
        .join(seasons.select("parentTconst", "seasonNumber"), ["parentTconst", "seasonNumber"])
        .join(series, "parentTconst")
        .join(ratings, "tconst")
    )
    worst = Window.partitionBy("parentTconst").orderBy("averageRating", F.desc("numVotes"))
    best = Window.partitionBy("parentTconst").orderBy(F.desc("averageRating"), F.desc("numVotes"))
    per_series = (
        rated
        .withColumn("worst_rank", F.row_number().over(worst))
        .withColumn("best_rank", F.row_number().over(best))
        .groupBy("parentTconst", "season_count", "last_season")
        .agg(F.max(F.when(F.col("worst_rank") == 1, F.col("seasonNumber"))).alias("worst_season"),
             F.max(F.when(F.col("best_rank") == 1, F.col("seasonNumber"))).alias("best_season"))
    )
    result = (
        per_series
        .filter((F.col("worst_season") == F.col("last_season")) & (F.col("best_season") <= EARLY_SEASONS))
        .join(titles, F.col("parentTconst") == F.col("tconst"))
        .select("primaryTitle", "season_count", "best_season", "worst_season")
        .orderBy(F.desc("season_count"))
    )
    total, found = per_series.count(), result.count()
    helpers.write(report, f"\nQ5. Series whose worst episode is in the last season "
                          f"and best episode in seasons 1-{EARLY_SEASONS}")
    helpers.write(report, f"  Series analysed: {total:,}  |  matching: {found:,} ({100 * found / total:.1f}%)")
    helpers.write(report, result.limit(20).toPandas().to_string(index=False))
    helpers.write(report)


REGION_LANGUAGE = {
    "US": "en", "GB": "en", "AU": "en", "CA": "en", "IE": "en", "NZ": "en",
    "FR": "fr", "BE": "fr", "DE": "de", "AT": "de", "IT": "it", "ES": "es",
    "MX": "es", "AR": "es", "PT": "pt", "BR": "pt", "JP": "ja", "KR": "ko",
    "CN": "zh", "TW": "zh", "HK": "yue", "IN": "hi", "TR": "tr", "RU": "ru",
    "SE": "sv", "DK": "da", "NO": "no", "FI": "fi", "PL": "pl", "IR": "fa",
}


def original_language(spark):
    """Estimate each title's original language from the akas matching the
    original title : their language if given, otherwise the main language of
    their region. IMDb datasets do not provide the original language."""
    akas = read_clean(spark, "title.akas")
    basics = read_clean(spark, "title.basics").select("tconst", "originalTitle")
    region_lang = F.create_map(*[F.lit(x) for kv in REGION_LANGUAGE.items() for x in kv])
    counts = (
        akas.join(basics, (akas["titleId"] == basics["tconst"]) & (F.lower(akas["title"]) == F.lower(basics["originalTitle"])))
        .withColumn("lang", F.coalesce(F.col("language"), region_lang[F.col("region")]))
        .filter(F.col("lang").isNotNull())
        .groupBy("tconst", "lang").agg(F.count("*").alias("n"))
    )
    by_title = Window.partitionBy("tconst").orderBy(F.desc("n"), "lang")
    return (counts.withColumn("rank", F.row_number().over(by_title))
            .filter(F.col("rank") == 1)
            .select("tconst", F.col("lang").alias("language")))


def q6_actors_multilingual_success(spark, movies, report):
    """Return actors with successful movies (rating >= SUCCESS_RATING and
    votes >= SUCCESS_VOTES) in at least 2 and 3 original languages."""
    hits = movies.filter((F.col("averageRating") >= SUCCESS_RATING) & (F.col("numVotes") >= SUCCESS_VOTES))
    hits_lang = hits.join(original_language(spark), "tconst")
    actors = (read_clean(spark, "title.principals")
              .filter(F.col("category").isin("actor", "actress"))
              .select("tconst", "nconst").distinct())
    names = read_clean(spark, "name.basics").select("nconst", "primaryName")
    per_actor = (
        actors.join(hits_lang, "tconst")
        .groupBy("nconst")
        .agg(F.countDistinct("language").alias("language_count"),
             F.countDistinct("tconst").alias("hit_count"),
             F.concat_ws(", ", F.array_sort(F.collect_set("language"))).alias("languages"))
        .filter(F.col("language_count") >= 2)
        .cache()
    )
    n_hits, n_with_lang = hits.count(), hits_lang.count()
    helpers.write(report, f"\nQ6. Actors with successful movies (>= {SUCCESS_RATING}, "
                          f">= {SUCCESS_VOTES:,} votes) in several original languages")
    helpers.write(report, f"  Successful movies: {n_hits:,}  |  with a known language: "
                          f"{n_with_lang:,} ({100 * n_with_lang / n_hits:.1f}%)")
    helpers.write(report, f"  Actors with >= 2 languages: {per_actor.count():,}")
    helpers.write(report, f"  Actors with >= 3 languages: "
                  f"{per_actor.filter(F.col('language_count') >= 3).count():,}")
    top = (per_actor.join(names, "nconst", "left")
           .select("primaryName", "language_count", "hit_count", "languages")
           .orderBy(F.desc("language_count"), F.desc("hit_count")))
    helpers.write(report, top.limit(20).toPandas().to_string(index=False))
    helpers.write(report)


def q7_early_peak(spark, movies, report):
    """Return actors (>= 20 years, >= 15 movies) whose movies in the first
    half of their career are rated clearly higher than in the second half."""
    principals = (read_clean(spark, "title.principals")
                  .filter(F.col("category").isin("actor", "actress"))
                  .select("tconst", "nconst").distinct())
    names = read_clean(spark, "name.basics").select("nconst", "primaryName")
    played = principals.join(movies.filter((F.col("numVotes") >= MIN_CAREER_VOTES) & F.col("startYear").isNotNull()), "tconst")

    by_actor = Window.partitionBy("nconst")
    best = by_actor.orderBy(F.desc("averageRating"), F.desc("numVotes"))
    played = (played
              .withColumn("first_year", F.min("startYear").over(by_actor))
              .withColumn("last_year", F.max("startYear").over(by_actor))
              .withColumn("is_best", F.row_number().over(best) == 1))
    mid = (F.col("first_year") + F.col("last_year")) / 2
    is_early = F.col("startYear") <= mid

    career = played.groupBy("nconst", "first_year", "last_year").agg(
        F.count("*").alias("movie_count"),
        F.round(F.avg(F.when(is_early, F.col("averageRating"))), 2).alias("early_avg"),
        F.round(F.avg(F.when(~is_early, F.col("averageRating"))), 2).alias("late_avg"),
        F.max(F.when(F.col("is_best"), F.col("startYear"))).alias("best_year"),
        F.first(F.when(F.col("is_best"), F.col("primaryTitle")), ignorenulls=True).alias("best_title"),
    ).filter((F.col("last_year") - F.col("first_year") >= MIN_CAREER_YEARS) & (F.col("movie_count") >= MIN_CAREER_MOVIES))

    result = (career
              .withColumn("drop", F.round(F.col("early_avg") - F.col("late_avg"), 2))
              .filter((F.col("drop") >= MIN_RATING_DROP) & (F.col("best_year") <= (F.col("first_year") + F.col("last_year")) / 2))
              .join(names, "nconst"))

    total, early = career.count(), result.count()
    helpers.write(report, f"\nQ7. Actors ({MIN_CAREER_YEARS}+ years, {MIN_CAREER_MOVIES}+ movies) whose popularity peaked early")
    helpers.write(report, f"  {early:,} of {total:,} eligible actors ({100 * early / total:.1f} %) peaked early")
    pdf = (result.orderBy(F.desc("drop"))
           .select("primaryName", "movie_count", "first_year", "last_year", "early_avg", "late_avg", "drop", "best_title")
           .limit(20).toPandas())
    helpers.write(report, pdf.to_string(index=False))
    helpers.write(report)
    return result


def q8_multi_talented(spark, movies, report):
    """Return people with >= 2 roles (director, writer, actor), each in
    >= 3 movies (MIN_ROLE_MOVIES), and compare their ratings when they hold
    several roles on the same movie versus a single role."""
    crew = read_clean(spark, "title.crew")
    principals = read_clean(spark, "title.principals")
    names = read_clean(spark, "name.basics").select("nconst", "primaryName")
    rated = (movies.filter(F.col("numVotes") >= MIN_CAREER_VOTES).select("tconst", "averageRating", "numVotes"))

    roles = (
        crew.select("tconst", F.explode("directors").alias("nconst"), F.lit("director").alias("role"))
        .unionByName(crew.select("tconst", F.explode("writers").alias("nconst"), F.lit("writer").alias("role")))
        .unionByName(principals.filter(F.col("category").isin("actor", "actress")).select("tconst", "nconst", F.lit("actor").alias("role")))
        .distinct()
        .join(rated.select("tconst"), "tconst")
    )

    talented = (
        roles.groupBy("nconst", "role").agg(F.count("*").alias("n"))
        .filter(F.col("n") >= MIN_ROLE_MOVIES)
        .groupBy("nconst")
        .agg(F.count("*").alias("role_count"), F.concat_ws("+", F.array_sort(F.collect_list("role"))).alias("talents"))
        .filter(F.col("role_count") >= 2)
        .cache()
    )

    per_movie = (
        roles.join(talented.select("nconst"), "nconst")
        .groupBy("nconst", "tconst")
        .agg(F.count("*").alias("n_roles"), F.concat_ws("+", F.array_sort(F.collect_list("role"))).alias("combo"))
        .join(rated, "tconst")
    )
    is_multi = F.col("n_roles") > 1

    helpers.write(report, f"\nQ8. Multi-talented people (>= 2 roles, each in >= {MIN_ROLE_MOVIES} movies)")
    helpers.write(report, f"  Multi-talented people: {talented.count():,}")
    helpers.write(report, talented.groupBy("talents").count().orderBy(F.desc("count")).toPandas().to_string(index=False))

    helpers.write(report, "\n  Ratings by role combination on the same movie:")
    by_combo = (per_movie.groupBy("combo")
                .agg(F.count("*").alias("movie_roles"),
                     F.round(F.avg("averageRating"), 2).alias("avg_rating"),
                     F.round(F.avg("numVotes")).cast("long").alias("avg_votes"))
                .orderBy(F.desc("movie_roles")))
    helpers.write(report, by_combo.toPandas().to_string(index=False))

    per_person = (
        per_movie.groupBy("nconst")
        .agg(F.round(F.avg(F.when(is_multi, F.col("averageRating"))), 2).alias("multi_avg"),
             F.round(F.avg(F.when(~is_multi, F.col("averageRating"))), 2).alias("single_avg"),
             F.sum(F.when(is_multi, 1).otherwise(0)).alias("multi_movies"))
        .filter(F.col("multi_avg").isNotNull() & F.col("single_avg").isNotNull())
        .withColumn("gain", F.round(F.col("multi_avg") - F.col("single_avg"), 2))
        .cache()
    )
    n, better = per_person.count(), per_person.filter(F.col("gain") > 0).count()
    avg_gain = per_person.agg(F.avg("gain")).first()[0]
    helpers.write(report, f"\n  Same person, multi-role vs single-role movies: {n:,} people")
    helpers.write(report, f"  Better rated when holding several roles: {better:,} ({100 * better / n:.1f} %)"
                  f"  |  average gain: {avg_gain:+.2f}")
    top = (per_person.join(names, "nconst").join(talented, "nconst")
           .select("primaryName", "talents", "multi_movies", "multi_avg", "single_avg", "gain")
           .orderBy(F.desc("multi_movies")).limit(20))
    helpers.write(report, top.toPandas().to_string(index=False))
    helpers.write(report)


def main():
    spark = (
        SparkSession.builder.appName("02_explore")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    movies = build_movies(spark).cache()
    movie_genres = explode_genres(movies).cache()

    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        helpers.write(report, "IMDb - Data exploration")
        helpers.write(report)
        helpers.write(report, f"Feature-length movies: {movies.count():,}")
        helpers.write(report, f"(movie, genre) rows:   {movie_genres.count():,}")
        
        # Questions, one function per question
        q1_runtime_outliers(movie_genres, report)
        top_genres = q2_top_genres_per_year(movie_genres, report)
        q3_top_vs_rest(movie_genres, top_genres, report)
        q4_one_hit_directors(spark, movies, report)
        q5_series_decline(spark, report)
        q6_actors_multilingual_success(spark, movies, report)
        q7_early_peak(spark, movies, report)
        q8_multi_talented(spark, movies, report)

    spark.stop()


if __name__ == "__main__":
    main()