"""דשבורד Streamlit לניתוח מכירות קמעונאות — ממשק בעברית."""

from __future__ import annotations

import inspect
import os
import traceback
from pathlib import Path

import pandas as pd
import plotly.express as px
import sqlite3
import streamlit as st

from retail_etl.analytics import RetailAnalytics
from retail_etl.db_security import assert_read_table
from retail_etl.meta import connect as meta_connect, get_last_success, get_source_state, list_active_alerts
from retail_etl.monitor import check_for_update
from retail_etl.presentation_he import (
    RTL_STYLE,
    project_root_from_app,
    render_architecture_presentation,
    weekday_hebrew_name,
)
from retail_etl.settings import Settings
from retail_etl.utils import configure_logging


def _default_kaggle_dataset() -> str:
    try:
        sec = st.secrets
        if hasattr(sec, "get"):
            v = sec.get("RETAIL_KAGGLE_DATASET", "")
            return str(v) if v else ""
    except Exception:
        pass
    return os.environ.get("RETAIL_KAGGLE_DATASET", "").strip()


def _default_kaggle_filename() -> str:
    try:
        sec = st.secrets
        if hasattr(sec, "get"):
            v = sec.get("RETAIL_KAGGLE_FILENAME", "")
            if v:
                return str(v)
    except Exception:
        pass
    return os.environ.get("RETAIL_KAGGLE_FILENAME", "retail_sales.csv").strip() or "retail_sales.csv"


def get_connection(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=30.0)


@st.cache_data(ttl=60)
def load_table(db_path: Path, table: str) -> pd.DataFrame:
    safe = assert_read_table(table)
    with get_connection(db_path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {safe}", conn)


@st.cache_data(ttl=60)
def load_dataset_overview(db_path: Path) -> dict:
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT CustomerID) AS customers,
                COUNT(DISTINCT Country) AS countries,
                MIN(InvoiceDate) AS min_date,
                MAX(InvoiceDate) AS max_date
            FROM stg_sales_clean
            """
        ).fetchone()
    return {
        "rows_count": int(row[0]),
        "customers": int(row[1]),
        "countries": int(row[2]),
        "min_date": row[3],
        "max_date": row[4],
    }


def main() -> None:
    configure_logging(os.environ.get("RETAIL_ETL_LOG_LEVEL", "INFO"))

    st.set_page_config(
        page_title="ניתוח מכירות קמעונאות | דשבורד",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(RTL_STYLE, unsafe_allow_html=True)

    settings = Settings.load()
    db_path = settings.db_path
    root = project_root_from_app(Path(__file__))

    st.title("דשבורד מכירות קמעונאות")
    st.caption("זרימת נתונים, ניטור, אנליטיקה וגרפים אינטראקטיביים.")

    if not db_path.exists():
        st.error(
            f"לא נמצא מסד SQLite בנתיב `{db_path}`. "
            "הרץ: `python -m retail_etl.cli run-all` או הגדר `RETAIL_ETL_DB_PATH`."
        )
        return

    st.sidebar.header("הגדרות")
    audience = st.sidebar.radio(
        "קהל יעד",
        options=["הנהלה", "טכני"],
        index=0,
        help="במצב הנהלה: פחות קוד. במצב טכני: מוצג גם מקור הפונקציה get_rfm.",
    )
    dataset = st.sidebar.text_input(
        "מזהה מערך בקגל (בעלים/שם)",
        value=_default_kaggle_dataset(),
        placeholder="לדוגמה: bekkarmerwan/retail-sales-dataset-sample-transactions",
    )
    filename = st.sidebar.text_input("שם קובץ במערך", value=_default_kaggle_filename())
    allow_incremental = st.sidebar.checkbox("לאפשר רענון מצטבר כשזה בטוח", value=True)
    download_from_kaggle = st.sidebar.checkbox(
        "להוריד מקגל לפני בדיקה",
        value=False,
        help="דורש הרשאות קגל. דורס את הקובץ המקומי ב־data/raw.",
    )

    with st.spinner("טוען טבלאות mart…"):
        try:
            monthly = load_table(db_path, "mart_sales_monthly")
            products = load_table(db_path, "mart_product_summary")
            customers = load_table(db_path, "mart_customer_summary")
            countries = load_table(db_path, "mart_country_summary")
        except Exception as e:  # noqa: BLE001
            st.error(f"שגיאה בטעינת טבלאות: {e}")
            return

    analytics = RetailAnalytics(db_path)

    @st.cache_data(ttl=60)
    def get_kpis() -> dict[str, float | str]:
        return analytics.get_kpis()

    @st.cache_data(ttl=3600)
    def get_weekday_revenue() -> pd.DataFrame:
        return analytics.get_revenue_by_weekday()

    @st.cache_data(ttl=3600)
    def get_invoice_distribution() -> pd.DataFrame:
        return analytics.get_invoice_revenue_distribution()

    @st.cache_data(ttl=3600)
    def get_rfm_df() -> pd.DataFrame:
        return analytics.get_rfm(q=5)

    (
        tab_intro,
        tab_arch,
        tab_overview,
        tab_products,
        tab_customers,
        tab_countries,
        tab_deep,
        tab_table,
        tab_explain,
    ) = st.tabs(
        [
            "פתיחה",
            "מצגת — אדריכלות",
            "סקירה כללית",
            "מוצרים",
            "לקוחות",
            "מדינות",
            "ניתוח מעמיק",
            "הטבלה המרכזית",
            "סיכום הפרויקט",
        ]
    )

    with tab_intro:
        st.subheader("סקירת הנתונים")
        st.markdown(
            """
<div dir="rtl" style="direction:rtl;text-align:right;line-height:1.75;">
<p>הדשבורד מציג עסקאות בסגנון <b>קמעונאות אונליין</b> מהקובץ <code>retail_sales.csv</code>.</p>
<ul>
<li>כל שורה היא שורת חשבונית (מוצר × כמות × מחיר יחידה).</li>
<li>ניתן לפלח לפי <b>מדינה</b> ולפי <b>מזהה לקוח</b>.</li>
</ul>
<p><b>ניקוי שבוצע</b>: לקוחות חסרים, תאריכים פגומים, כמויות/מחירים לא חיוביים.</p>
</div>
""",
            unsafe_allow_html=True,
        )

        try:
            info = load_dataset_overview(db_path)
        except Exception as e:  # noqa: BLE001
            st.error(f"לא ניתן לטעון סיכום מה־staging: {e}")
            return

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("שורות נקיות (staging)", f"{info['rows_count']:,}")
        c2.metric("לקוחות ייחודיים", f"{info['customers']:,}")
        c3.metric("מדינות", f"{info['countries']:,}")
        c4.metric("טווח תאריכים", f"{info['min_date']} ← {info['max_date']}")

        st.subheader("מצב ניטור")
        with meta_connect(db_path) as conn:
            source_state = get_source_state(conn)
            last_success = get_last_success(conn)
            alerts = list_active_alerts(conn)

        m1, m2, m3 = st.columns(3)
        m1.metric("רענון אחרון שהצליח", last_success["finished_at"] if last_success else "—")
        m2.metric("מצב הריצה", last_success["mode"] if last_success else "—")
        m3.metric("התראות פעילות", str(len(alerts)))

        st.caption(f"מסד: `{db_path}` · CSV ברירת מחדל: `{settings.raw_csv_default}`")

        if source_state:
            sha = source_state.get("sha256") or "—"
            st.caption(
                f"טביעת אצבע: `{sha[:16]}…` | גודל {source_state.get('size_bytes')} בתים | "
                f"עודכן {source_state.get('updated_at')}"
            )

        if alerts:
            st.error("התראות פעילות")
            st.dataframe(alerts, use_container_width=True)

        if st.button("בדיקה עכשיו", type="primary"):
            if not dataset.strip():
                st.warning("הזן מזהה מערך (בעלים/שם) בסרגל הצד.")
            else:
                try:
                    result = check_for_update(
                        dataset=dataset.strip(),
                        filename=filename.strip(),
                        allow_incremental=allow_incremental,
                        db_path=db_path,
                        download_first=download_from_kaggle,
                    )
                    st.success(f"תוצאה: `{result}`")
                except Exception as e:  # noqa: BLE001
                    st.error(str(e))
                    with st.expander("מעקב שגיאה"):
                        st.code(traceback.format_exc())
                st.cache_data.clear()
                st.rerun()

        st.subheader("תמונת הזרימה")
        st.markdown(
            """
<div dir="rtl" style="direction:rtl;text-align:right;">
<p><b>חילוץ</b> ← CSV / קגל · <b>טרנספורמציה</b> ← ניקוי ב־Pandas ·
<b>טעינה</b> ← SQLite (staging + marts) · <b>הצגה</b> ← דשבורד זה.</p>
</div>
""",
            unsafe_allow_html=True,
        )

    with tab_arch:
        render_architecture_presentation(st, root)

    with tab_overview:
        kpis = get_kpis()
        total_revenue = float(monthly["revenue"].sum())
        total_units = float(monthly["units"].sum())
        total_invoices = float(monthly["invoices"].sum())

        col1, col2, col3 = st.columns(3)
        col1.metric("סה״כ הכנסות", f"{total_revenue:,.0f}")
        col2.metric("סה״כ יחידות", f"{total_units:,.0f}")
        col3.metric("סה״כ חשבוניות", f"{total_invoices:,.0f}")

        col4, col5, col6 = st.columns(3)
        col4.metric("ממוצע לחשבונית", f"{kpis['avg_invoice_value']:,.2f}")
        col5.metric("ממוצע שורות לחשבונית", f"{kpis['avg_lines_per_invoice']:,.2f}")
        col6.metric("ממוצע הוצאה ללקוח", f"{kpis['avg_spend_per_customer']:,.2f}")

        col7, col8, _ = st.columns(3)
        col7.metric("חלק בריטניה מההכנסה", f"{kpis['uk_revenue_share']*100:.1f}%")
        col8.metric("מוצרים ייחודיים", f"{int(kpis['products']):,}")

        st.subheader("הכנסה חודשית")
        fig = px.line(
            monthly,
            x="year_month",
            y="revenue",
            title="הכנסה לאורך זמן (לפי חודש)",
            labels={"year_month": "חודש", "revenue": "הכנסה"},
        )
        fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(
            '<div dir="rtl" style="text-align:right;"><b>תובנה:</b> עונתיות — לתכנן מלאי וקמפיינים סביב שיאים.</div>',
            unsafe_allow_html=True,
        )

        st.subheader("הכנסה לפי יום בשבוע")
        weekday = get_weekday_revenue().copy()
        x_col = "weekday"
        if not weekday.empty and "weekday" in weekday.columns:
            weekday["יום"] = weekday["weekday"].map(weekday_hebrew_name)
            x_col = "יום"
        weekday_fig = px.bar(
            weekday,
            x=x_col,
            y="revenue",
            title="הכנסה לפי יום בשבוע",
            labels={"revenue": "הכנסה", "יום": "יום בשבוע", "weekday": "יום בשבוע"},
        )
        weekday_fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(weekday_fig, use_container_width=True)
        if not weekday.empty:
            col_rev = "revenue"
            day_col = x_col
            best = weekday.sort_values(col_rev, ascending=False).iloc[0]
            st.caption(f"יום שיא: {best[day_col]} — {best[col_rev]:,.0f}.")

        st.subheader("התפלגות הכנסה לפי חשבונית")
        inv = get_invoice_distribution()
        hist_fig = px.histogram(
            inv,
            x="invoice_revenue",
            nbins=40,
            title="הכנסה לכל חשבונית",
            labels={"invoice_revenue": "הכנסה לחשבונית", "count": "מספר"},
        )
        hist_fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(hist_fig, use_container_width=True)

        if not inv.empty:
            q50 = float(inv["invoice_revenue"].quantile(0.5))
            q90 = float(inv["invoice_revenue"].quantile(0.9))
            q95 = float(inv["invoice_revenue"].quantile(0.95))
            st.caption(f"חציון={q50:,.2f} | אחוזון 90={q90:,.2f} | אחוזון 95={q95:,.2f}")

    with tab_products:
        st.subheader("מובילים לפי הכנסה")
        top_n = st.slider("כמה מוצרים להציג", min_value=5, max_value=30, value=10, step=5, key="p")
        top_products = products.head(top_n)
        fig = px.bar(
            top_products,
            x="Description",
            y="revenue",
            title=f"{top_n} מובילים",
            labels={"Description": "תיאור מוצר", "revenue": "הכנסה"},
        )
        fig.update_layout(template="plotly_white", xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top_products, use_container_width=True, hide_index=True)
        st.markdown(
            '<div dir="rtl" style="text-align:right;"><b>תובנה:</b> מוצרי דגל נושאים חלק גדול מההכנסה.</div>',
            unsafe_allow_html=True,
        )

    with tab_customers:
        st.subheader("לקוחות מובילים לפי הכנסה")
        top_n_c = st.slider("כמה לקוחות להציג", min_value=5, max_value=30, value=10, step=5, key="c")
        top_customers = customers.head(top_n_c)
        fig = px.bar(
            top_customers,
            x="CustomerID",
            y="revenue",
            title=f"{top_n_c} לקוחות מובילים",
            labels={"CustomerID": "מזהה לקוח", "revenue": "הכנסה"},
        )
        fig.update_layout(template="plotly_white", height=450)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top_customers, use_container_width=True, hide_index=True)
        st.markdown(
            '<div dir="rtl" style="text-align:right;"><b>תובנה:</b> ריכוז הכנסות — לשמור ולפתח חשבונות מפתח.</div>',
            unsafe_allow_html=True,
        )

    with tab_countries:
        st.subheader("מדינות מובילות לפי הכנסה")
        top_n_co = st.slider("כמה מדינות להציג", min_value=5, max_value=30, value=10, step=5, key="co")
        top_countries = countries.head(top_n_co)
        fig = px.bar(
            top_countries,
            x="Country",
            y="revenue",
            title=f"{top_n_co} מדינות מובילות",
            labels={"Country": "מדינה", "revenue": "הכנסה"},
        )
        fig.update_layout(template="plotly_white", xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top_countries, use_container_width=True, hide_index=True)
        st.markdown(
            '<div dir="rtl" style="text-align:right;"><b>תובנה:</b> דומיננטיות בריטניה מול פוטנציאל בינלאומי.</div>',
            unsafe_allow_html=True,
        )

    with tab_deep:
        st.subheader("פילוח RFM (התנהגות לקוחות)")
        st.caption(
            "RFM: עדכנות (ימים מאז רכישה אחרונה), תדירות (מספר חשבוניות), כסף (הכנסה מצטברת)."
        )

        rfm_df = get_rfm_df()
        if rfm_df.empty:
            st.warning("אין נתוני RFM.")
            return

        seg_counts = (
            rfm_df.groupby("rfm_segment")
            .agg(
                customers=("CustomerID", "count"),
                avg_monetary=("monetary", "mean"),
                avg_recency=("recency_days", "mean"),
            )
            .reset_index()
            .sort_values("customers", ascending=False)
            .head(20)
        )

        seg_fig = px.bar(
            seg_counts,
            x="rfm_segment",
            y="customers",
            title="סגמנטי RFM מובילים (לפי מספר לקוחות)",
            labels={"rfm_segment": "סגמנט", "customers": "מספר לקוחות"},
        )
        seg_fig.update_layout(template="plotly_white", height=420, xaxis_tickangle=-45)
        st.plotly_chart(seg_fig, use_container_width=True)

        best = rfm_df.sort_values("monetary", ascending=False).head(1).iloc[0]
        st.caption(
            f"לקוח עם הכנסה מקסימלית: מזהה {int(best['CustomerID'])} | "
            f"הכנסה={best['monetary']:,.2f} | חשבוניות={int(best['frequency'])} | "
            f"עדכנות={best['recency_days']:.1f} ימים."
        )

        with st.expander("המחלקה RetailAnalytics — הסבר ודוגמה", expanded=True):
            st.markdown(
                """
<div dir="rtl" style="direction: rtl; text-align: right; line-height: 1.75;">
  <p><b>תפקיד המחלקה</b></p>
  <p>
    Streamlit כאן הוא רק תצוגה. כל השאילתות והעיבודים המספריים נמצאים ב־
    <code>src/retail_etl/analytics.py</code> בתוך <code>RetailAnalytics</code>.
  </p>
  <p><b>מתודות עיקריות</b></p>
  <ul>
    <li><code>get_kpis()</code> — מדדי ליבה לדשבורד.</li>
    <li><code>get_revenue_by_weekday()</code> — הכנסה לפי יום בשבוע.</li>
    <li><code>get_invoice_revenue_distribution()</code> — סכום לכל חשבונית (להיסטוגרמה).</li>
    <li><code>get_rfm(q=5)</code> — חישוב RFM וציונים.</li>
  </ul>
</div>
""",
                unsafe_allow_html=True,
            )

            st.divider()
            st.markdown("**דוגמת שימוש (כמו בדשבורד):**")
            sample_usage = """from pathlib import Path
from retail_etl.analytics import RetailAnalytics

db_path = Path("data/db/retail.db")
analytics = RetailAnalytics(db_path)

# מדדי ליבה
kpis = analytics.get_kpis()

# פילוח RFM (חמישה מדרגות כברירת מחדל)
rfm_df = analytics.get_rfm(q=5)
"""
            st.code(sample_usage, language="python")

            with st.expander(
                "מקור הפונקציה get_rfm (למצב טכני)",
                expanded=(audience == "טכני"),
            ):
                @st.cache_data(ttl=3600)
                def _cached_get_rfm_source() -> str:
                    return inspect.getsource(RetailAnalytics.get_rfm)

                st.code(_cached_get_rfm_source(), language="python")

    with tab_explain:
        st.markdown(
            """
<div dir="rtl" style="direction: rtl; text-align: right; line-height: 1.75;">
  <h3>סיכום הפרויקט</h3>
  <p>
    Pipeline מקצה לקצה: <b>חילוץ</b> (CSV או קגל), <b>טרנספורמציה</b> (ניקוי ב־Pandas),
    <b>טעינה</b> (SQLite: staging ואז marts), <b>הצגה</b> (Streamlit + Plotly).
  </p>

  <h4>1) ETL</h4>
  <ul>
    <li>קובץ מקור: <code>data/raw/retail_sales.csv</code>.</li>
    <li>ניקוי: תאריכים, טקסטים, מספרים, סינון כמויות/מחירים, הסרת שורות בלי <code>CustomerID</code>.</li>
    <li>שדה מחושב: <code>line_total = Quantity * UnitPrice</code>.</li>
    <li>במסד: <code>stg_sales_clean</code> ואז טבלאות mart מוכנות לדוחות.</li>
  </ul>

  <h4>2) ניטור וקגל</h4>
  <ul>
    <li><code>ingest_kaggle.py</code> — הורדת קובץ ספציפי ל־<code>data/raw</code>.</li>
    <li>טביעת אצבע לקובץ (גודל + SHA256) נשמרת בטבלאות מטא.</li>
    <li><code>monitor.py</code> — השוואה לקובץ המקומי, בדיקת עמודות מול רשימה צפויה, כתיבת התראות.</li>
  </ul>

  <h4>3) אבטחת שאילתות וארגון SQL</h4>
  <ul>
    <li>שמות טבלאות לקריאה/ייצוא עוברים בדיקה מול רשימה מותרת.</li>
    <li>טקסטי SQL ארוכים נמצאים ב־<code>src/retail_etl/sql/*.sql</code> ונטענים ב־<code>sql_loader.py</code>.</li>
  </ul>

  <h4>4) בדיקות והרצה בקונטיינר</h4>
  <ul>
    <li><code>tests/</code> — pytest.</li>
    <li><code>Dockerfile</code> — הרצה אחידה; פורט ברירת מחדל 8501.</li>
  </ul>

  <p>למצגת מפורטת על קבצים ותיקיות — עבור ללשונית <b>מצגת — אדריכלות</b>.</p>
</div>
""",
            unsafe_allow_html=True,
        )

    with tab_table:
        st.subheader("הטבלה המרכזית ב־SQLite")
        st.caption(
            "הטבלה `stg_sales_clean` נבנית אחרי ניקוי; ממנה נגזרות טבלאות ה־mart."
        )

        st.markdown(
            """
<div dir="rtl" style="direction: rtl; text-align: right; line-height: 1.75;">
  <h4>מה יש בטבלה</h4>
  <p>טבלת staging אחרי ניקוי — בסיס ל־
    <code>mart_sales_monthly</code>, <code>mart_product_summary</code>,
    <code>mart_customer_summary</code>, <code>mart_country_summary</code>.
  </p>

  <h4>עמודות</h4>
  <ul>
    <li><code>InvoiceNo</code> — מספר חשבונית</li>
    <li><code>StockCode</code> — קוד מוצר</li>
    <li><code>Description</code> — תיאור (ריווח הוסר)</li>
    <li><code>Quantity</code> — כמות</li>
    <li><code>InvoiceDate</code> — תאריך ושעה כמחרוזת אחידה ל־SQLite</li>
    <li><code>UnitPrice</code> — מחיר ליחידה</li>
    <li><code>CustomerID</code> — מזהה לקוח</li>
    <li><code>Country</code> — מדינה</li>
    <li><code>line_total</code> — <code>Quantity * UnitPrice</code></li>
  </ul>

  <h4>לוגיקת ניקוי (תמצית)</h4>
  <ul>
    <li>תאריך: המרה ל־datetime; ערכים לא תקינים נפסלים.</li>
    <li>מספרים: המרה בטוחה ל־Quantity ו־UnitPrice.</li>
    <li>ללא <code>CustomerID</code> — השורה מוסרת (לפי הגדרת הפרויקט).</li>
    <li><code>Quantity &gt;= 1</code> ו־<code>UnitPrice &gt; 0</code> — מסננים החזרות/שורות לא רלוונטיות.</li>
  </ul>
</div>
""",
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
