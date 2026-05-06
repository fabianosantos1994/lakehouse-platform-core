import argparse

from lakehouse_platform_core.ingestion.loader import load_ingestion_config
from lakehouse_platform_core.ingestion.models import IngestionConfig, TableConfig
from lakehouse_platform_core.ingestion.runner import run_ingestion_step


SUPPORTED_STEPS = ("bronze", "silver")


def main() -> None:
    args = _parse_args()
    config = load_ingestion_config(args.config)
    table = _find_table(config, args.table)
    result = run_ingestion_step(
        step=args.step,
        config=config,
        table=table,
    )

    print(result)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="lakehouse-ingestion")
    parser.add_argument("--config", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--step", required=True, choices=SUPPORTED_STEPS)

    return parser.parse_args()


def _find_table(config: IngestionConfig, table_name: str) -> TableConfig:
    for table in config.tables:
        if table.name == table_name:
            return table

    raise ValueError(f"Table not found in ingestion config: {table_name}")


if __name__ == "__main__":
    main()
