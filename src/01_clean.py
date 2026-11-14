# 01_clean.py
# Étape 1 — Nettoyage : lit data/raw/, remplace les "\N" par null, corrige les types,
# découpe les colonnes listes (genres, primaryProfession, knownForTitles, directors...)
# et écrit le résultat en Parquet dans data/clean/.

import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *


def read_raw(spark, name):
    df = spark.read.csv(f"data/raw/{name}.tsv.gz", sep="\t", header=True, nullValue="\\N")
    
    return df

def report_nulls(df, name):
    print(f"Null values in {name}:")
    total = df.count()

    df.select([
        F.round(
            F.count(F.when(F.col(c).isNull() | (F.col(c) == ""), c)) / total * 100,
            2,
        ).alias(c)
        for c in df.columns
    ]).show()

def clean(df):
    """Étape 3 : nettoyer (types, \\N, listes...)."""
    ...

def save(df, name):
    df.write.mode("overwrite").parquet(f"data/clean/{name}.parquet")

def main():
    spark = SparkSession.builder.appName("01_clean").getOrCreate()
    
    TABLES = ["name.basics", "title.basics", "title.akas", "title.crew",
          "title.episode", "title.principals", "title.ratings"]

    for name in TABLES:
        df = read_raw(spark, name)
        report_nulls(df, name)
        df = clean(df)
        save(df, name)


    spark.stop()


if __name__ == "__main__":
    main()
