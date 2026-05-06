from pyspark.sql import DataFrame, SparkSession


def write_iceberg_table_full(
    spark: SparkSession,
    dataframe: DataFrame,
    target_table: str,
) -> None:
    namespace = _derive_namespace(target_table)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    (
        dataframe.writeTo(target_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )


def _derive_namespace(target_table: str) -> str:
    table_parts = target_table.split(".")

    if len(table_parts) < 2:
        raise ValueError(f"Target table must include a namespace: {target_table}")

    return ".".join(table_parts[:-1])
