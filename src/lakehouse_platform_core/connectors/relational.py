from pyspark.sql import DataFrame, SparkSession

from lakehouse_platform_core.connections.resolver import ResolvedConnection


POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"


def read_postgres_table_full(
    spark: SparkSession,
    connection: ResolvedConnection,
    source_table: str,
) -> DataFrame:
    if connection.database_type != "postgres":
        raise ValueError("V1 relational connector only supports postgres")

    return (
        spark.read
        .format("jdbc")
        .option("url", connection.jdbc_url)
        .option("dbtable", source_table)
        .option("user", connection.username)
        .option("password", connection.password)
        .option("driver", POSTGRES_JDBC_DRIVER)
        .load()
    )
