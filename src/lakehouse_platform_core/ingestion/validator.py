from lakehouse_platform_core.ingestion.models import IngestionConfig


def validate_ingestion_config(config: IngestionConfig) -> None:
    if not config.job.name:
        raise ValueError("job.name is required")

    if not config.job.schedule:
        raise ValueError("job.schedule is required")

    if config.source.type != "relational":
        raise ValueError("V1 only supports source.type=relational")

    if config.source.database_type != "postgres":
        raise ValueError("V1 only supports source.database_type=postgres")

    if not config.source.database_name:
        raise ValueError("source.database_name is required")

    if not config.source.connection_id:
        raise ValueError("source.connection_id is required")

    if not config.tables:
        raise ValueError("tables must contain at least one table")

    table_names = set()

    for table in config.tables:
        if not table.name:
            raise ValueError("tables[].name is required")

        if table.name in table_names:
            raise ValueError(f"Duplicate table name: {table.name}")

        table_names.add(table.name)

        if not table.source.table:
            raise ValueError(f"tables[].source.table is required for table: {table.name}")

        if not table.target.silver_table:
            raise ValueError(f"tables[].target.silver_table is required for table: {table.name}")

        if table.load.type != "full":
            raise ValueError(f"V1 only supports tables[].load.type=full for table: {table.name}")
