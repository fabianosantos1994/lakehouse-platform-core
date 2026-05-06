from lakehouse_platform_core.ingestion.bronze import run_bronze_full_load
from lakehouse_platform_core.ingestion.models import IngestionConfig, TableConfig
from lakehouse_platform_core.ingestion.silver import run_silver_full_load
from lakehouse_platform_core.spark.session import build_spark_session


def run_ingestion_step(
    step: str,
    config: IngestionConfig,
    table: TableConfig,
) -> str:
    spark = build_spark_session(app_name=f"{config.job.name}_{table.name}_{step}")

    try:
        if step == "bronze":
            return run_bronze_full_load(
                spark=spark,
                config=config,
                table=table,
            )

        if step == "silver":
            return run_silver_full_load(
                spark=spark,
                config=config,
                table=table,
            )

        raise ValueError(f"Unsupported ingestion step: {step}")
    finally:
        spark.stop()
