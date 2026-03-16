from __future__ import annotations

import argparse
from pathlib import Path

from .etl import run_etl
from .exporter import export_tables
from .paths import get_paths
from .plotting import generate_charts


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="retail-etl", description="Retail sales ETL and analysis")
    sub = parser.add_subparsers(dest="command", required=True)

    run_all = sub.add_parser("run-all", help="Run full ETL pipeline, exports, and charts")
    run_all.add_argument(
        "--csv-path",
        type=Path,
        default=None,
        help="Optional path to retail_sales.csv (defaults to data/raw/retail_sales.csv)",
    )
    run_all.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Optional destination SQLite DB path (defaults to data/db/retail.db)",
    )

    export_cmd = sub.add_parser("export", help="Export mart tables to files")
    export_cmd.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="SQLite DB path (defaults to data/db/retail.db)",
    )

    plot_cmd = sub.add_parser("plot", help="Generate Plotly charts from marts")
    plot_cmd.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="SQLite DB path (defaults to data/db/retail.db)",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    paths = get_paths()

    if args.command == "run-all":
        csv_path = args.csv_path
        db_path = args.db_path
        if csv_path is None:
            csv_path = paths.raw_dir / "retail_sales.csv"
        if db_path is None:
            db_path = paths.db_dir / "retail.db"
        db_path = run_etl(csv_path, db_path)
        export_tables(db_path=db_path)
        generate_charts(db_path=db_path)
        print(f"Full pipeline completed. SQLite DB: {db_path}")
        print(f"Exports written under: {paths.exports_dir}")
        print(f"Charts written under: {paths.charts_dir}")

    elif args.command == "export":
        db_path = args.db_path or (paths.db_dir / "retail.db")
        export_tables(db_path=db_path)
        print(f"Exports written under: {paths.exports_dir}")

    elif args.command == "plot":
        db_path = args.db_path or (paths.db_dir / "retail.db")
        generate_charts(db_path=db_path)
        print(f"Charts written under: {paths.charts_dir}")


if __name__ == "__main__":
    main()

