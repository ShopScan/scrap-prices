ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

RUN apt-get update && apt-get install -y \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libxkbcommon0 \
    libgtk-3-0 \
    libdrm2 \
    libgbm1 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libasound2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir "playwright>=1.44,<2" && \
    playwright install chromium
