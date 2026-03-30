"""ממשק שורת פקודה ל־ETL, ייצוא וגרפים."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .etl import run_etl
from .exporter import export_tables
from .utils import configure_logging, get_logger
from .paths import get_paths
from .plotting import generate_charts
from .settings import Settings

logger = get_logger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="retail-etl", description="ניתוח מכירות קמעונאות — ETL ודוחות")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="הורדת קובץ גולמי מקגל")
    ingest.add_argument("--dataset", required=True, help="מזהה מערך (בעלים/שם)")
    ingest.add_argument("--filename", required=True, help="שם הקובץ בתוך המערך")
    ingest.add_argument("--force", action="store_true", help="להוריד מחדש גם אם הקובץ קיים")

    monitor = sub.add_parser("monitor", help="בדיקת עדכון ורענון ETL לפי הצורך")
    monitor.add_argument("--dataset", required=True, help="מזהה מערך (בעלים/שם)")
    monitor.add_argument("--filename", required=True, help="שם הקובץ המקומי ב־data/raw")
    monitor.add_argument("--allow-incremental", action="store_true", help="לאפשר טעינה מצטברת כשזה בטוח")
    monitor.add_argument(
        "--download",
        action="store_true",
        help="להוריד מקגל לפני השוואת טביעת אצבע",
    )

    watch = sub.add_parser("watch", help="בדיקה חוזרת כל N שניות")
    watch.add_argument("--dataset", required=True, help="מזהה מערך (בעלים/שם)")
    watch.add_argument("--filename", required=True, help="שם הקובץ המקומי")
    watch.add_argument("--interval-seconds", type=int, default=300, help="מרווח בשניות בין בדיקות")
    watch.add_argument(
        "--pull",
        action="store_true",
        help="בכל מחזור: הורדה מקגל לפני בדיקה",
    )

    run_all = sub.add_parser("run-all", help="הרצת צינור מלא (אופציונלי: קגל, ייצוא, גרפים)")
    run_all.add_argument("--dataset", default=None, help="מזהה מערך (בעלים/שם)")
    run_all.add_argument("--filename", default=None, help="שם קובץ במערך (למשל online_retail.csv)")
    run_all.add_argument(
        "--csv-path",
        type=Path,
        default=None,
        help="נתיב ל־CSV (ברירת מחדל: data/raw/online_retail.csv)",
    )
    run_all.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="נתיב ל־SQLite (ברירת מחדל: data/db/retail.db)",
    )

    export_cmd = sub.add_parser("export", help="ייצוא טבלאות mart לקבצים")
    export_cmd.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="נתיב מסד SQLite (ברירת מחדל: data/db/retail.db)",
    )

    plot_cmd = sub.add_parser("plot", help="יצירת גרפי Plotly מה־marts")
    plot_cmd.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="נתיב מסד SQLite (ברירת מחדל: data/db/retail.db)",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--log-level", default=None, help="רמת לוג (ברירת מחדל: RETAIL_ETL_LOG_LEVEL או INFO)")
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
        print(f"הורדה הושלמה: {fp.path} ({fp.size_bytes} בתים)")
        print(f"sha256: {fp.sha256}")

    elif args.command == "monitor":
        from .monitor import check_for_update

        result = check_for_update(
            dataset=args.dataset,
            filename=args.filename,
            allow_incremental=args.allow_incremental,
            download_first=args.download,
        )
        print(f"תוצאת ניטור: {result}")

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
            print(f"[מעקב] {result}")
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
        print(f"צינור הושלם. SQLite: {db_path}")
        print(f"ייצואים: {paths.exports_dir}")
        print(f"גרפים: {paths.charts_dir}")

    elif args.command == "export":
        db_path = args.db_path or settings.db_path
        export_tables(db_path=db_path)
        print(f"ייצואים: {paths.exports_dir}")

    elif args.command == "plot":
        db_path = args.db_path or settings.db_path
        generate_charts(db_path=db_path)
        print(f"גרפים: {paths.charts_dir}")


if __name__ == "__main__":
    main()
