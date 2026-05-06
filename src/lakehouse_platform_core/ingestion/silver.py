from pyspark.sql import SparkSession

from lakehouse_platform_core.ingestion.derivation import derive_table_paths
from lakehouse_platform_core.ingestion.models import IngestionConfig, TableConfig
from lakehouse_platform_core.writers.iceberg import write_iceberg_table_full


def run_silver_full_load(
    spark: SparkSession,
    config: IngestionConfig,
    table: TableConfig,
) -> str:
    derived_paths = derive_table_paths(config, table)
    dataframe = spark.read.parquet(derived_paths.bronze_path)
    target_table = table.target.silver_table

    write_iceberg_table_full(
        spark=spark,
        dataframe=dataframe,
        target_table=target_table,
    )

    return target_table
