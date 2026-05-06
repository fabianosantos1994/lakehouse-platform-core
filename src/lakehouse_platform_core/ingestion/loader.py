from pathlib import Path
from typing import Any

import yaml

from lakehouse_platform_core.ingestion.models import (
    IngestionConfig,
    JobConfig,
    LoadConfig,
    SourceConfig,
    TableConfig,
    TableSourceConfig,
    TableTargetConfig,
)
from lakehouse_platform_core.ingestion.validator import validate_ingestion_config


def load_ingestion_config(path: str) -> IngestionConfig:
    data = _load_yaml(path)
    _validate_top_level_sections(data)

    config = IngestionConfig(
        job=JobConfig(
            name=data["job"]["name"],
            schedule=data["job"]["schedule"],
        ),
        source=SourceConfig(
            type=data["source"]["type"],
            database_type=data["source"]["database_type"],
            database_name=data["source"]["database_name"],
            connection_id=data["source"]["connection_id"],
        ),
        tables=[
            TableConfig(
                name=table["name"],
                source=TableSourceConfig(
                    table=table["source"]["table"],
                ),
                target=TableTargetConfig(
                    silver_table=table["target"]["silver_table"],
                ),
                load=LoadConfig(
                    type=table["load"]["type"],
                ),
            )
            for table in data["tables"]
        ],
    )

    validate_ingestion_config(config)

    return config


def _validate_top_level_sections(data: dict[str, Any]) -> None:
    required_sections = ("job", "source", "tables")

    for section in required_sections:
        if section not in data:
            raise ValueError(f"Missing required top-level section: {section}")


def _load_yaml(path: str) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    if not isinstance(data, dict):
        raise ValueError("Ingestion YAML must be a mapping")

    return data
