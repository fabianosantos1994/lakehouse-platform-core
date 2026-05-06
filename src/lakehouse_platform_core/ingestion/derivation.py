from dataclasses import dataclass

from lakehouse_platform_core.ingestion.models import IngestionConfig, TableConfig


ENGINE_EXTRACT = "spark"
BRONZE_BASE_PATH = "s3a://lakehouse/bronze"
SILVER_BASE_PATH = "s3a://lakehouse/silver"


@dataclass(frozen=True)
class DerivedTablePaths:
    schema_table_name: str
    bronze_path: str
    silver_path: str


def derive_table_paths(config: IngestionConfig, table: TableConfig) -> DerivedTablePaths:
    schema_table_name = derive_schema_table_name(table)
    engine_source = config.source.database_type
    source_database = config.source.database_name

    bronze_path = (
        f"{BRONZE_BASE_PATH}/"
        f"{engine_source}/"
        f"{ENGINE_EXTRACT}/"
        f"{source_database}/"
        f"{schema_table_name}/"
    )
    silver_path = (
        f"{SILVER_BASE_PATH}/"
        f"{engine_source}/"
        f"{ENGINE_EXTRACT}/"
        f"{source_database}/"
        f"{schema_table_name}/"
    )

    return DerivedTablePaths(
        schema_table_name=schema_table_name,
        bronze_path=bronze_path,
        silver_path=silver_path,
    )


def derive_schema_table_name(table: TableConfig) -> str:
    return table.source.table.replace(".", "_")
