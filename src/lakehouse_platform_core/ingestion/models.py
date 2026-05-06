from dataclasses import dataclass


@dataclass(frozen=True)
class JobConfig:
    name: str
    schedule: str


@dataclass(frozen=True)
class SourceConfig:
    type: str
    database_type: str
    database_name: str
    connection_id: str


@dataclass(frozen=True)
class TableSourceConfig:
    table: str


@dataclass(frozen=True)
class TableTargetConfig:
    silver_table: str


@dataclass(frozen=True)
class LoadConfig:
    type: str


@dataclass(frozen=True)
class TableConfig:
    name: str
    source: TableSourceConfig
    target: TableTargetConfig
    load: LoadConfig


@dataclass(frozen=True)
class IngestionConfig:
    job: JobConfig
    source: SourceConfig
    tables: list[TableConfig]
