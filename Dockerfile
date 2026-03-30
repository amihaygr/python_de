# Production-oriented image: Streamlit UI + ETL package.
FROM python:3.12-slim-bookworm

WORKDIR /app

# zoneinfo (Asia/Jerusalem) needs IANA tz database in slim images
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    RETAIL_ETL_LOG_LEVEL=INFO \
    PYTHONPATH=/app/src

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY src ./src
COPY app.py README.md ./
COPY .streamlit ./.streamlit

# Persist DB and raw data on a volume in real deployments.
RUN mkdir -p data/db data/raw reports/charts data/exports

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health')"

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.headless=true"]
