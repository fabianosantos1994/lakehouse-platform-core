from pyspark.sql import SparkSession

from lakehouse_platform_core.connections.resolver import resolve_connection
from lakehouse_platform_core.connectors.relational import read_postgres_table_full
from lakehouse_platform_core.ingestion.derivation import derive_table_paths
from lakehouse_platform_core.ingestion.models import IngestionConfig, TableConfig
from lakehouse_platform_core.writers.raw_parquet import write_raw_parquet_full


def run_bronze_full_load(
    spark: SparkSession,
    config: IngestionConfig,
    table: TableConfig,
) -> str:
    connection = resolve_connection(
        connection_id=config.source.connection_id,
        database_type=config.source.database_type,
    )
    dataframe = read_postgres_table_full(
        spark=spark,
        connection=connection,
        source_table=table.source.table,
    )
    derived_paths = derive_table_paths(config, table)

    write_raw_parquet_full(
        dataframe=dataframe,
        bronze_path=derived_paths.bronze_path,
    )

    return derived_paths.bronze_path
