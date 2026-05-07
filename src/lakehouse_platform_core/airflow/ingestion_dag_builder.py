from airflow import DAG

from lakehouse_platform_core.airflow.dag_factory import DagFactory
from lakehouse_platform_core.ingestion.loader import load_ingestion_config


class IngestionDagBuilder:
    def __init__(self, dag_factory: DagFactory | None = None) -> None:
        self.dag_factory = dag_factory or DagFactory()

    def build(self, config_path: str) -> DAG:
        config = load_ingestion_config(config_path)
        dag = self.dag_factory.create_dag(
            dag_id=config.job.name,
            schedule=config.job.schedule,
            tags=["ingestion", "v1"],
        )

        for table in config.tables:
            bronze_task = self.dag_factory.create_command_task(
                dag=dag,
                task_id=f"bronze_{table.name}",
                command=_build_command(
                    config_path=config_path,
                    table_name=table.name,
                    step="bronze",
                ),
            )
            silver_task = self.dag_factory.create_command_task(
                dag=dag,
                task_id=f"silver_{table.name}",
                command=_build_command(
                    config_path=config_path,
                    table_name=table.name,
                    step="silver",
                ),
            )

            bronze_task >> silver_task

        return dag


def _build_command(config_path: str, table_name: str, step: str) -> str:
    cli_command = (
        "python -m lakehouse_platform_core.cli.ingestion "
        f"--config {config_path} "
        f"--table {table_name} "
        f"--step {step}"
    )

    spark_command = (
        "export PURCHASE_POSTGRES_HOST=postgres && "
        "export PURCHASE_POSTGRES_PORT=5432 && "
        "export PURCHASE_POSTGRES_DATABASE=purchase && "
        "export PURCHASE_POSTGRES_USERNAME=postgres && "
        "export PURCHASE_POSTGRES_PASSWORD=postgres && "
        "export S3_ACCESS_KEY=minio && "
        "export S3_SECRET_KEY=minio123 && "
        "export PYTHONPATH=/opt/project/lakehouse-platform-core/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip && "
        f"{cli_command}"
    )

    return f"docker exec spark bash -lc '{spark_command}'"
