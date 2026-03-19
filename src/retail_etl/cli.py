from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .etl import run_etl
from .exporter import export_tables
from .logging_config import configure_logging, get_logger
from .paths import get_paths
from .plotting import generate_charts
from .settings import Settings

logger = get_logger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="retail-etl", description="Retail sales ETL and analysis")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Download raw dataset file from Kaggle")
    ingest.add_argument("--dataset", required=True, help="Kaggle dataset identifier (owner/dataset)")
    ingest.add_argument("--filename", required=True, help="Filename inside the Kaggle dataset")
    ingest.add_argument("--force", action="store_true", help="Force re-download")

    monitor = sub.add_parser("monitor", help="Check for updates and refresh ETL if needed")
    monitor.add_argument("--dataset", required=True, help="Kaggle dataset identifier (owner/dataset)")
    monitor.add_argument("--filename", required=True, help="Filename (local raw file name)")
    monitor.add_argument("--allow-incremental", action="store_true", help="Allow incremental load when safe")
    monitor.add_argument(
        "--download",
        action="store_true",
        help="Download latest file from Kaggle before comparing fingerprints",
    )

    watch = sub.add_parser("watch", help="Poll for updates every N seconds (local monitoring)")
    watch.add_argument("--dataset", required=True, help="Kaggle dataset identifier (owner/dataset)")
    watch.add_argument("--filename", required=True, help="Filename (local raw file name)")
    watch.add_argument("--interval-seconds", type=int, default=300, help="Polling interval in seconds")
    watch.add_argument(
        "--pull",
        action="store_true",
        help="Download from Kaggle on each iteration before fingerprint check",
    )

    run_all = sub.add_parser("run-all", help="Run full pipeline (ingest, monitor, exports, charts)")
    run_all.add_argument("--dataset", default=None, help="Kaggle dataset identifier (owner/dataset)")
    run_all.add_argument("--filename", default=None, help="Filename inside Kaggle dataset (e.g. retail_sales.csv)")
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
    if argv is None:
        argv = sys.argv[1:]

    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--log-level", default=None, help="Logging level (default: RETAIL_ETL_LOG_LEVEL or INFO)")
    known, rest = pre.parse_known_args(argv)
    settings = Settings.load()
    configure_logging(known.log_level or settings.log_level)

    args = _parse_args(rest)
    paths = get_paths()

    if args.command == "ingest":
        from .ingest_kaggle import download_dataset_file

        fp = download_dataset_file(
            dataset=args.dataset,
            filename=args.filename,
            dest_path=paths.raw_dir / args.filename,
            force=args.force,
        )
        print(f"Downloaded: {fp.path} ({fp.size_bytes} bytes)")
        print(f"sha256: {fp.sha256}")

    elif args.command == "monitor":
        from .monitor import check_for_update

        result = check_for_update(
            dataset=args.dataset,
            filename=args.filename,
            allow_incremental=args.allow_incremental,
            download_first=args.download,
        )
        print(f"Monitor result: {result}")

    elif args.command == "watch":
        from .monitor import check_for_update

        interval = getattr(args, "interval_seconds", 300)
        while True:
            result = check_for_update(
                dataset=args.dataset,
                filename=args.filename,
                allow_incremental=True,
                download_first=args.pull,
            )
            print(f"[watch] {result}")
            time.sleep(interval)

    elif args.command == "run-all":
        db_path = args.db_path or settings.db_path

        if args.dataset and args.filename:
            from .ingest_kaggle import download_dataset_file
            from .monitor import check_for_update

            download_dataset_file(
                dataset=args.dataset,
                filename=args.filename,
                dest_path=paths.raw_dir / args.filename,
                force=False,
            )
            check_for_update(
                dataset=args.dataset,
                filename=args.filename,
                allow_incremental=True,
                db_path=db_path,
            )
        else:
            csv_path = args.csv_path or settings.raw_csv_default
            run_etl(csv_path, db_path, mode="full")

        export_tables(db_path=db_path)
        generate_charts(db_path=db_path)
        print(f"Full pipeline completed. SQLite DB: {db_path}")
        print(f"Exports written under: {paths.exports_dir}")
        print(f"Charts written under: {paths.charts_dir}")

    elif args.command == "export":
        db_path = args.db_path or settings.db_path
        export_tables(db_path=db_path)
        print(f"Exports written under: {paths.exports_dir}")

    elif args.command == "plot":
        db_path = args.db_path or settings.db_path
        generate_charts(db_path=db_path)
        print(f"Charts written under: {paths.charts_dir}")


if __name__ == "__main__":
    main()
