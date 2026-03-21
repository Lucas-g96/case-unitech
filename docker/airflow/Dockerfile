# ──────────────────────────────────────────────────────────────────
# Airflow customizado com DuckDB + dependências do projeto
# ──────────────────────────────────────────────────────────────────
FROM apache/airflow:2.10.4-python3.12

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instala dependências Python adicionais
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copia o pacote ETL
COPY src /opt/airflow/src
