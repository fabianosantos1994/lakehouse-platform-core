from lakehouse_platform_core.ingestion.bronze import run_bronze_full_load
from lakehouse_platform_core.ingestion.models import IngestionConfig, TableConfig
from lakehouse_platform_core.ingestion.silver import run_silver_full_load
from lakehouse_platform_core.logging.logger import get_logger
from lakehouse_platform_core.spark.session import build_spark_session


logger = get_logger(__name__)


def run_ingestion_step(
    step: str,
    config: IngestionConfig,
    table: TableConfig,
) -> str:
    spark = build_spark_session(app_name=f"{config.job.name}_{table.name}_{step}")

    logger.info(
        "Starting ingestion step: step=%s table=%s job=%s",
        step,
        table.name,
        config.job.name,
    )

    try:
        if step == "bronze":
            result = run_bronze_full_load(
                spark=spark,
                config=config,
                table=table,
            )
        elif step == "silver":
            result = run_silver_full_load(
                spark=spark,
                config=config,
                table=table,
            )
        else:
            raise ValueError(f"Unsupported ingestion step: {step}")

        logger.info(
            "Completed ingestion step: step=%s table=%s job=%s result=%s",
            step,
            table.name,
            config.job.name,
            result,
        )

        return result
    except Exception:
        logger.exception(
            "Failed ingestion step: step=%s table=%s job=%s",
            step,
            table.name,
            config.job.name,
        )
        raise
    finally:
        spark.stop()
