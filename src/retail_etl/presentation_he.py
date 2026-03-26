"""תוכן מצגת ואדריכלות בעברית לדשבורד Streamlit."""

from __future__ import annotations

from pathlib import Path

# עיצוב RTL כללי (מוזרק ל־Streamlit)
RTL_STYLE = """
<style>
    .main .block-container {
        direction: rtl;
        text-align: right;
        font-family: "Segoe UI", "David", "Arial Hebrew", "Noto Sans Hebrew", sans-serif;
    }
    [data-testid="stSidebar"] {
        direction: rtl;
        text-align: right;
        font-family: "Segoe UI", "David", "Arial Hebrew", "Noto Sans Hebrew", sans-serif;
    }
    [data-testid="stSidebar"] .stMarkdown { text-align: right; }
    h1, h2, h3, h4 { text-align: right; }
    .stMetric label, .stMetric [data-testid="stMetricValue"] { direction: rtl; unicode-bidi: plaintext; }
    div[data-testid="stExpander"] details summary { text-align: right; }
</style>
"""


def project_root_from_app(app_file: Path) -> Path:
    """שורש הפרויקט כאשר מועבר נתיב ל־app.py."""
    return app_file.resolve().parent


def render_architecture_presentation(st, root: Path) -> None:
    """מצגת שלב־אחר־שלב: מבנה תיקיות ותפקיד כל קובץ (פרטים בתוך expander)."""
    st.markdown(
        """
<div dir="rtl" style="direction:rtl;text-align:right;line-height:1.75;">
<h2>מצגת — אדריכלות הפיתוח</h2>
<p>
המסך הזה בנוי כ־<b>שלבים</b>: קודם התמונה הכללית, אחר כך כל תיקייה, ולבסוף פירוט קובץ־קובץ.
פתח/י את ה־expanders רק במה שרוצים להציג — כך אפשר גם להרצה קצרה וגם להרצה מלאה.
</p>
</div>
""",
        unsafe_allow_html=True,
    )

    steps = [
        (
            "שלב 1 — מבט על כל הפרויקט",
            """
<div dir="rtl" style="direction:rtl;text-align:right;">
<p>שורש הפרויקט מכיל את נקודת הכניסה לדשבורד, הגדרות אריזה, בדיקות, קוד החבילה, ונתונים מקומיים.</p>
<ul>
<li><code>app.py</code> — אפליקציית Streamlit (ממשק בלבד).</li>
<li><code>src/retail_etl/</code> — לוגיקת ETL, ניטור, אנליטיקה, טעינת SQL.</li>
<li><code>tests/</code> — בדיקות יחידה (pytest).</li>
<li><code>data/raw/</code> — קובץ CSV גולמי.</li>
<li><code>data/db/</code> — קובץ SQLite (<code>retail.db</code>).</li>
<li><code>Dockerfile</code> — הרצה בקונטיינר על פורט 8501.</li>
<li><code>requirements.txt</code> — תלויות זמן ריצה.</li>
<li><code>pyproject.toml</code> — הגדרת חבילה (setuptools).</li>
<li><code>.streamlit/</code> — הגדרות שרת Streamlit.</li>
</ul>
</div>
""",
        ),
        (
            "שלב 2 — זרימת נתונים (E → T → L → הצגה)",
            """
<div dir="rtl" style="direction:rtl;text-align:right;">
<ol>
<li><b>חילוץ</b>: קריאת CSV מקומי או הורדה מקגל (<code>ingest_kaggle.py</code>).</li>
<li><b>טרנספורמציה</b>: ניקוי והעשרה ב־Pandas (<code>etl.py</code> — <code>clean_sales</code>).</li>
<li><b>טעינה</b>: כתיבה ל־SQLite — staging ואז marts (<code>etl.py</code>).</li>
<li><b>מטא־דאטה</b>: מצב מקור, ריצות, התראות (<code>meta.py</code> + קבצי <code>sql/meta_*.sql</code>).</li>
<li><b>ניטור</b>: השוואת טביעת אצבע לקובץ, זיהוי שינוי סכימה (<code>monitor.py</code>).</li>
<li><b>אנליטיקה</b>: שאילתות SQL מקבצים + עיבוד ב־Pandas (<code>analytics.py</code>).</li>
<li><b>ממשק</b>: <code>app.py</code> קורא לשכבת האנליטיקה ומציג Plotly.</li>
</ol>
</div>
""",
        ),
    ]
    for title, html in steps:
        with st.expander(title, expanded=True):
            st.markdown(html, unsafe_allow_html=True)

    st.markdown(
        """
<div dir="rtl" style="direction:rtl;text-align:right;">
<h3>שלב 3 — פירוט לפי תיקייה וקובץ</h3>
<p>להלן רשימה מדויקת. כל קובץ עם תיאור קצר + (בתוך ה־expander) פירוט מורחב.</p>
</div>
""",
        unsafe_allow_html=True,
    )

    # --- שורש פרויקט ---
    with st.expander("תיקייה: שורש הפרויקט", expanded=False):
        st.markdown(
            """
<div dir="rtl" style="direction:rtl;text-align:right;">
<ul>
<li><code>app.py</code> — דשבורד Streamlit: טעינת טבלאות, גרפים, מצגת, ניטור ידני.</li>
<li><code>Dockerfile</code> — בניית image: Python 3.12, התקנת requirements, הרצת Streamlit.</li>
<li><code>.dockerignore</code> — מה לא נכנס ל־build context (חוסך זמן וגודל).</li>
<li><code>requirements.txt</code> — pandas, plotly, streamlit, kaggle וכו׳.</li>
<li><code>requirements-dev.txt</code> — pytest לפיתוח.</li>
<li><code>pyproject.toml</code> — מצא חבילות תחת <code>src</code>.</li>
<li><code>pytest.ini</code> — נתיב Python לבדיקות.</li>
<li><code>README.md</code> — תיעוד פרויקט (אם קיים).</li>
<li><code>.env.example</code> — דוגמה למשתני סביבה.</li>
</ul>
</div>
""",
            unsafe_allow_html=True,
        )

    # --- src/retail_etl ---
    with st.expander("תיקייה: src/retail_etl/ (החבילה הראשית)", expanded=True):
        st.markdown(
            """
<div dir="rtl" style="direction:rtl;text-align:right;">
<p>כל לוגיקת העסק מרוכזת כאן. הממשק לא אמור להכיל SQL ארוך — רק קריאות לשכבות האלה.</p>
</div>
""",
            unsafe_allow_html=True,
        )
        pkg_files: list[tuple[str, str, str]] = [
            (
                "__init__.py",
                "מאתחל חבילה (ריק / ייצוא עתידי).",
                "קובץ סטנדרטי של Python package. כרגע ללא לוגיקה — מאפשר ייבוא <code>retail_etl.*</code>.",
            ),
            (
                "paths.py",
                "נתיבי פרויקט אחידים.",
                "<code>ProjectPaths</code> ו־<code>get_paths()</code>: מחזירים נתיבים ל־data/raw, data/db, reports וכו׳. מונע נתיבים קשיחים מפוזרים.",
            ),
            (
                "settings.py",
                "הגדרות ממשתני סביבה.",
                "<code>Settings.load()</code>: נתיב DB, קובץ CSV ברירת מחדל, רמת לוג, ברירות מחדל לקגל. תומך ב־12-factor.",
            ),
            (
                "utils.py",
                "עזרי לוג.",
                "<code>configure_logging</code> ו־<code>get_logger</code>: הגדרת פורמט לוג אחיד ל־CLI ולספרייה.",
            ),
            (
                "db_security.py",
                "רשימות טבלאות מותרות + בדיקת שם טבלה.",
                "<code>EXPECTED_RAW_COLUMNS</code> לבדיקת כותרות CSV. <code>assert_read_table</code> / <code>assert_export_table</code> — מונעים שימוש בטבלאות שלא ברשימה.",
            ),
            (
                "sql_loader.py",
                "טעינת קבצי SQL מהדיסק.",
                "<code>load_sql(name)</code> קורא מ־<code>sql/</code> עם מטמון LRU. מפריד בין לוגיקת Python לבין טקסט SQL.",
            ),
            (
                "etl.py",
                "ליבת ETL.",
                "<code>load_raw_csv</code>, <code>clean_sales</code>, <code>write_sqlite</code>, <code>run_etl</code> (מלא/מצטבר), <code>rebuild_marts</code>. עסקאות SQLite עם rollback. קבצי SQL: <code>etl_*.sql</code>, <code>marts_*.sql</code>.",
            ),
            (
                "meta.py",
                "טבלאות מטא ב־SQLite.",
                "מצב מקור, סכימה, ריצות צינור, התראות. כל פעולה דרך <code>meta_*.sql</code>.",
            ),
            (
                "ingest_kaggle.py",
                "הורדה מקגל.",
                "אימות slug, טביעת אצבע קובץ (SHA256), ייבוא עצלן של KaggleApi כדי לא לכשל בהפעלה בלי credentials.",
            ),
            (
                "monitor.py",
                "זיהוי עדכונים והחלטת רענון.",
                "משווה fingerprint, בודק עמודות מול ציפייה, כותב מצב ואלרטים, יכול להפעיל ETL.",
            ),
            (
                "analytics.py",
                "שכבת אנליטיקה עסקית.",
                "<code>RetailAnalytics</code>: KPI, הכנסה לפי יום שבוע, התפלגות חשבוניות, RFM. SQL בקבצי <code>analytics_*.sql</code>; חישובי ציון ב־Pandas.",
            ),
            (
                "exporter.py",
                "ייצוא טבלאות mart לפורמטים שונים.",
                "בודק שם טבלה מול allowlist לפני קריאה.",
            ),
            (
                "plotting.py",
                "יצירת גרפי Plotly לקבצים.",
                "טוען מ־SQLite ושומר HTML/PNG לתיקיית דוחות.",
            ),
            (
                "cli.py",
                "ממשק שורת פקודה (argparse).",
                "פקודות: ingest, monitor, watch, run-all וכו׳. נקודת כניסה: <code>python -m retail_etl.cli</code>.",
            ),
            (
                "presentation_he.py",
                "תוכן מצגת בעברית.",
                "מודול זה: עיצוב RTL וטקסטי אדריכלות לדשבורד — מפריד תוכן הצגה מקוד הממשק.",
            ),
        ]
        for name, short, long in pkg_files:
            with st.expander(f"קובץ: retail_etl/{name}", expanded=False):
                st.markdown(
                    f'<div dir="rtl" style="direction:rtl;text-align:right;"><p><b>בקצרה:</b> {short}</p>'
                    f"<p><b>פירוט:</b> {long}</p></div>",
                    unsafe_allow_html=True,
                )

    # --- sql ---
    with st.expander("תיקייה: src/retail_etl/sql/ (כל השאילתות)", expanded=False):
        st.markdown(
            """
<div dir="rtl" style="direction:rtl;text-align:right;">
<p>כל קובץ כאן הוא שאילתה או סקריפט SQL טהור. Python רק טוען אותו בזמן ריצה.</p>
</div>
""",
            unsafe_allow_html=True,
        )
        sql_index: list[tuple[str, str]] = [
            ("marts_monthly.sql", "אגרגציה חודשית → בסיס לגרף קו הכנסות."),
            ("marts_product.sql", "סיכום לפי מוצר (קוד + תיאור)."),
            ("marts_country.sql", "סיכום לפי מדינה."),
            ("marts_customer.sql", "סיכום לפי לקוח."),
            ("analytics_kpis.sql", "KPI כלליים מה־staging."),
            ("analytics_weekday.sql", "הכנסה לפי מספר יום בשבוע (SQLite strftime)."),
            ("analytics_invoice_distribution.sql", "סכום לפי מספר חשבונית."),
            ("analytics_rfm_max_date.sql", "תאריך חשבונית מקסימלי לחישוב recency."),
            ("analytics_rfm_customers.sql", "בסיס RFM לפי לקוח."),
            ("etl_init_staging.sql", "יצירת טבלת staging נקייה."),
            ("etl_create_unique_index.sql", "אינדקס ייחודי לדילול כפילויות במצב מצטבר."),
            ("etl_drop_staging.sql", "מחיקת טבלה/אינדקס לפני טעינה מלאה."),
            ("etl_insert_incremental.sql", "INSERT OR IGNORE לשורות חדשות."),
            ("etl_select_max_invoice_date.sql", "תאריך אחרון לסינון צ'אנקים במצב מצטבר."),
            ("meta_init_tables.sql", "יצירת כל טבלאות המטא."),
            ("meta_upsert_source_state.sql", "עדכון מצב קובץ מקור."),
            ("meta_get_source_state.sql", "קריאת מצב מקור."),
            ("meta_add_alert.sql", "הוספת התראה."),
            ("meta_clear_alerts_all.sql", "ביטול כל ההתראות הפעילות."),
            ("meta_clear_alerts_by_kind.sql", "ביטול התראות לפי סוג."),
            ("meta_list_active_alerts.sql", "רשימת התראות פעילות."),
            ("meta_start_run.sql", "תחילת ריצת צינור."),
            ("meta_finish_run.sql", "סיום ריצה עם סטטוס ושגיאה."),
            ("meta_upsert_schema_state.sql", "שמירת צילום עמודות/טיפוסים."),
            ("meta_get_last_success.sql", "הרצה אחרונה שהצליחה."),
        ]
        for fname, desc in sql_index:
            with st.expander(f"SQL: {fname}", expanded=False):
                st.markdown(
                    f'<div dir="rtl" style="direction:rtl;text-align:right;"><p>{desc}</p></div>',
                    unsafe_allow_html=True,
                )
                sql_path = root / "src" / "retail_etl" / "sql" / fname
                if sql_path.is_file():
                    st.code(sql_path.read_text(encoding="utf-8"), language="sql")

    # --- tests ---
    with st.expander("תיקייה: tests/", expanded=False):
        st.markdown(
            """
<div dir="rtl" style="direction:rtl;text-align:right;">
<ul>
<li><code>test_clean_sales.py</code> — בדיקות לפונקציית הניקוי ב־<code>etl.py</code>.</li>
<li><code>test_sql_guard.py</code> — בדיקות ש־allowlist חוסם שמות טבלאות לא חוקיים.</li>
</ul>
</div>
""",
            unsafe_allow_html=True,
        )

    # --- data ---
    with st.expander("תיקיות: data/raw ו־data/db", expanded=False):
        st.markdown(
            """
<div dir="rtl" style="direction:rtl;text-align:right;">
<ul>
<li><code>data/raw/retail_sales.csv</code> — נתוני מקור לדוגמה (לא חובה לקומיט אם מוגדר ב־.gitignore).</li>
<li><code>data/db/retail.db</code> — מסד SQLite אחרי הרצת ETL: staging, marts, meta.</li>
<li><code>data/exports/</code> — יעד אופציונלי לייצואים מה־CLI.</li>
</ul>
</div>
""",
            unsafe_allow_html=True,
        )


def weekday_hebrew_name(en: str) -> str:
    """מיפוי שמות יום באנגלית (מ־analytics) לעברית קצרה."""
    m = {
        "Sun": "יום א׳",
        "Mon": "יום ב׳",
        "Tue": "יום ג׳",
        "Wed": "יום ד׳",
        "Thu": "יום ה׳",
        "Fri": "יום ו׳",
        "Sat": "שבת",
    }
    return m.get(en, en)
