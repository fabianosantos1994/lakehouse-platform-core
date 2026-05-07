from airflow import DAG

from lakehouse_platform_core.airflow.dag_factory import DagFactory
from lakehouse_platform_core.ingestion.loader import load_ingestion_config


class IngestionDagBuilder:
    def __init__(
        self,
        dag_factory: DagFactory | None = None,
        spark_container_name: str = "spark",
        pythonpath: str = "/opt/project/lakehouse-platform-core/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
    ) -> None:
        self.dag_factory = dag_factory or DagFactory()
        self.spark_container_name = spark_container_name
        self.pythonpath = pythonpath

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
                    spark_container_name=self.spark_container_name,
                    pythonpath=self.pythonpath,
                ),
            )
            silver_task = self.dag_factory.create_command_task(
                dag=dag,
                task_id=f"silver_{table.name}",
                command=_build_command(
                    config_path=config_path,
                    table_name=table.name,
                    step="silver",
                    spark_container_name=self.spark_container_name,
                    pythonpath=self.pythonpath,
                ),
            )

            bronze_task >> silver_task

        return dag


def _build_command(
    config_path: str,
    table_name: str,
    step: str,
    spark_container_name: str,
    pythonpath: str,
) -> str:
    cli_command = (
        "python -m lakehouse_platform_core.cli.ingestion "
        f"--config {config_path} "
        f"--table {table_name} "
        f"--step {step}"
    )

    spark_command = (
        f"export PYTHONPATH={pythonpath} && "
        f"{cli_command}"
    )

    return f"docker exec {spark_container_name} bash -lc '{spark_command}'"
