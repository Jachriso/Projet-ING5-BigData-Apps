import config


def write(report, text=""):
    """Print a line to the terminal and write it to the report."""
    print(text, flush=True)
    report.write(text + "\n")
    report.flush()


def save(path, df, name):
    """Write a DataFrame as Parquet to <path>/<name>.parquet (path is a config folder)."""
    path = (path / f"{name}.parquet").as_posix()
    df.write.mode("overwrite").parquet(path)


def read_clean(spark, name):
    path = (config.CLEAN_DIR / f"{name}.parquet").as_posix()
    return spark.read.parquet(path)
