from logging import config
import pyspark.sql.functions as F

def write(report, text=""):
    """Print a line to the terminal and write it to the report."""
    print(text)
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

def read_clean(spark, name):
    path = (config.CLEAN_DIR / f"{name}.parquet").as_posix()
    return spark.read.parquet(path)


def save(df, name):
    path = (config.ENRICHED_DIR / f"{name}.parquet").as_posix()
    df.write.mode("overwrite").parquet(path)