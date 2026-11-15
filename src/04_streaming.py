# 04_streaming.py - Structured Streaming : Wikipedia edits on pages matching IMDb titles.
# Run : python src/04_streaming.py   (Kafka and 04_wiki_producer.py must be running)

import sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config
import helpers

WATERMARK, WINDOW = "10 minutes", "5 minutes"
SCHEMA = StructType([
    StructField("title", StringType()), StructField("type", StringType()),
    StructField("user", StringType()), StructField("bot", BooleanType()),
    StructField("timestamp", LongType()), StructField("server_url", StringType()),
])

spark = (SparkSession.builder.appName("04_streaming")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
         .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Static side: well-known films (>= 1,000 votes) and the people credited in them
MIN_VOTES = 1_000
ratings = helpers.read_clean(spark, "title.ratings").filter(F.col("numVotes") >= MIN_VOTES)
known = (helpers.read_clean(spark, "title.basics").filter(F.col("titleType") == "movie")
         .join(ratings.select("tconst"), "tconst"))
films = known.select(F.lower("primaryTitle").alias("key"), F.lit("film").alias("kind"),
                     F.col("tconst").alias("imdb_id"))
cast = (helpers.read_clean(spark, "title.principals").select("tconst", "nconst")
        .join(known.select("tconst"), "tconst").select("nconst").distinct())
people = (helpers.read_clean(spark, "name.basics").join(cast, "nconst")
          .select(F.lower("primaryName").alias("key"), F.lit("person").alias("kind"),
                  F.col("nconst").alias("imdb_id")))
imdb_keys = films.unionByName(people).dropDuplicates(["key"]).cache()
print(f"IMDb keys used for matching: {imdb_keys.count():,}")

events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "wiki-changes").option("startingOffsets", "latest").load()
          .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
          .withColumn("event_time", F.col("timestamp").cast("timestamp"))
          .withColumn("key", F.lower(F.regexp_replace("title", r" \(.*film\)$", ""))))

matches = events.join(imdb_keys, "key")   # stream-static join

counts = (matches.withWatermark("event_time", WATERMARK)
          .groupBy(F.window("event_time", WINDOW), "kind")
          .agg(F.count("*").alias("edits"), F.approx_count_distinct("title").alias("pages")))

(matches.select("event_time", "title", "kind", "imdb_id", "type", "user", "bot")
 .writeStream.format("console").option("truncate", False).outputMode("append").start())
(counts.writeStream.format("console").option("truncate", False).outputMode("update").start())
spark.streams.awaitAnyTermination()