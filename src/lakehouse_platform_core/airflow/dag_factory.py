from collections.abc import Sequence
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


class DagFactory:
    def __init__(
        self,
        owner: str = "lakehouse",
        retries: int = 0,
        start_date: datetime | None = None,
        catchup: bool = False,
    ) -> None:
        self.owner = owner
        self.retries = retries
        self.start_date = start_date or datetime(2024, 1, 1)
        self.catchup = catchup

    def create_dag(
        self,
        dag_id: str,
        schedule: str | None,
        tags: Sequence[str] | None = None,
    ) -> DAG:
        return DAG(
            dag_id=dag_id,
            default_args={
                "owner": self.owner,
                "retries": self.retries,
            },
            start_date=self.start_date,
            schedule=schedule,
            catchup=self.catchup,
            tags=list(tags or []),
        )

    def create_command_task(
        self,
        dag: DAG,
        task_id: str,
        command: str,
    ) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=command,
            dag=dag,
        )
