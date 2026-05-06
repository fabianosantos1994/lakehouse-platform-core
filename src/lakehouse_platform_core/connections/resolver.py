import os
from dataclasses import dataclass


SUPPORTED_DATABASE_TYPE = "postgres"


@dataclass(frozen=True)
class ResolvedConnection:
    connection_id: str
    database_type: str
    host: str
    port: str
    database: str
    username: str
    password: str
    jdbc_url: str


def resolve_connection(connection_id: str, database_type: str) -> ResolvedConnection:
    if database_type != SUPPORTED_DATABASE_TYPE:
        raise ValueError("V1 only supports database_type=postgres")

    env_prefix = connection_id.upper()

    host = _required_env(env_prefix, "HOST")
    port = _required_env(env_prefix, "PORT")
    database = _required_env(env_prefix, "DATABASE")
    username = _required_env(env_prefix, "USERNAME")
    password = _required_env(env_prefix, "PASSWORD")

    return ResolvedConnection(
        connection_id=connection_id,
        database_type=database_type,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        jdbc_url=f"jdbc:postgresql://{host}:{port}/{database}",
    )


def _required_env(prefix: str, name: str) -> str:
    env_name = f"{prefix}_{name}"
    value = os.getenv(env_name)

    if not value:
        raise ValueError(f"Missing required environment variable: {env_name}")

    return value
