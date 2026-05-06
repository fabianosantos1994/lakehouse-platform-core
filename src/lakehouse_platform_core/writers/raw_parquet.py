from pyspark.sql import DataFrame


def write_raw_parquet_full(dataframe: DataFrame, bronze_path: str) -> None:
    (
        dataframe.write
        .mode("overwrite")
        .parquet(bronze_path)
    )
