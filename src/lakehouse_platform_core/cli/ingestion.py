import argparse

from lakehouse_platform_core.ingestion.loader import load_ingestion_config
from lakehouse_platform_core.ingestion.models import IngestionConfig, TableConfig
from lakehouse_platform_core.ingestion.runner import run_ingestion_step


SUPPORTED_STEPS = ("bronze", "silver")


def main() -> None:
    args = _parse_args()
    config = load_ingestion_config(args.config)

    if args.list_tables:
        _print_available_tables(config)
        return

    if not args.table or not args.step:
        raise ValueError("--table and --step are required unless --list-tables is used")

    table = _find_table(config, args.table)
    result = run_ingestion_step(
        step=args.step,
        config=config,
        table=table,
    )

    print("Step completed successfully.")
    print(f"Step: {args.step}")
    print(f"Table: {table.name}")
    print(f"Result: {result}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="lakehouse-ingestion")
    parser.add_argument("--config", required=True)
    parser.add_argument("--table")
    parser.add_argument("--step", choices=SUPPORTED_STEPS)
    parser.add_argument("--list-tables", action="store_true")
    return parser.parse_args()


def _print_available_tables(config: IngestionConfig) -> None:
    for table in config.tables:
        print(table.name)


def _find_table(config: IngestionConfig, table_name: str) -> TableConfig:
    for table in config.tables:
        if table.name == table_name:
            return table

    available_tables = ", ".join(table.name for table in config.tables)
    raise ValueError(
        f"Table not found in ingestion config: {table_name}. "
        f"Available tables: {available_tables}"
    )


if __name__ == "__main__":
    main()