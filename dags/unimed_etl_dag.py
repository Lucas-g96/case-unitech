"""
unimed_etl_dag.py
DAG principal do pipeline Unimed Fortaleza 360°.

Fluxo das tasks:
    start
      ├─ extract_all_sources
      │    (lê CSVs via DuckDB em memória compartilhada via XCom — paths)
      │
      ├─ transform_and_load_calendario
      ├─ transform_and_load_operadoras
      │    └─ transform_and_load_produtos   (depende de operadoras)
      │         ├─ transform_and_load_beneficiarios
      │         └─ transform_and_load_financeiro
      │
      └─ quality_checks
           └─ end
"""

from __future__ import annotations

import io
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Garante que o pacote ETL seja encontrado dentro do container
sys.path.insert(0, "/opt/airflow/src")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from loguru import logger

# ─────────────────────────────────────────────────────────────────
# Importação lazy dos módulos ETL
# ─────────────────────────────────────────────────────────────────

def _import_etl():
    from unimed_etl.extract import extract_all
    from unimed_etl.transform import (
        build_dim_calendario,
        transform_operadoras,
        transform_produtos,
        transform_beneficiarios,
        transform_financeiro,
    )
    from unimed_etl.load import (
        load_dim_calendario,
        load_dim_operadora,
        load_dim_produto,
        load_fact_beneficiarios,
        load_fact_financeiro,
    )
    from unimed_etl.quality import run_all_checks
    return {
        "extract_all": extract_all,
        "build_dim_calendario": build_dim_calendario,
        "transform_operadoras": transform_operadoras,
        "transform_produtos": transform_produtos,
        "transform_beneficiarios": transform_beneficiarios,
        "transform_financeiro": transform_financeiro,
        "load_dim_calendario": load_dim_calendario,
        "load_dim_operadora": load_dim_operadora,
        "load_dim_produto": load_dim_produto,
        "load_fact_beneficiarios": load_fact_beneficiarios,
        "load_fact_financeiro": load_fact_financeiro,
        "run_all_checks": run_all_checks,
    }


# ─────────────────────────────────────────────────────────────────
# Task functions
# ─────────────────────────────────────────────────────────────────

def task_extract(**ctx):
    """Extrai todos os CSVs e empurra para XCom via pickle."""
    etl = _import_etl()
    raw = etl["extract_all"]()
    ti = ctx["ti"]
    for key, df in raw.items():
        ti.xcom_push(key=f"raw_{key}", value=df.to_json(orient="split", date_format="iso"))
    logger.info(f"[DAG] Extract concluído: {list(raw.keys())}")


def task_load_calendario(**ctx):
    etl = _import_etl()
    df = etl["build_dim_calendario"]()
    etl["load_dim_calendario"](df)


def task_load_operadoras(**ctx):
    import pandas as pd
    etl = _import_etl()

    ti = ctx["ti"]
    raw_json = ti.xcom_pull(task_ids="extract_all_sources", key="raw_operadoras")

    # CORRIGIDO: io.StringIO evita que pandas interprete o JSON como filepath
    df_raw = pd.read_json(io.StringIO(raw_json), orient="split")

    df_clean = etl["transform_operadoras"](df_raw)
    df_with_sk = etl["load_dim_operadora"](df_clean)

    ti.xcom_push(key="dim_operadora", value=df_with_sk.to_json(orient="split", date_format="iso"))


def task_load_produtos(**ctx):
    import pandas as pd
    etl = _import_etl()

    ti = ctx["ti"]
    raw_json = ti.xcom_pull(task_ids="extract_all_sources", key="raw_produtos")
    op_json  = ti.xcom_pull(task_ids="transform_and_load_operadoras", key="dim_operadora")

    # CORRIGIDO: io.StringIO em todas as leituras de JSON do XCom
    df_raw = pd.read_json(io.StringIO(raw_json), orient="split")
    df_op  = pd.read_json(io.StringIO(op_json),  orient="split")

    df_clean   = etl["transform_produtos"](df_raw)
    df_with_sk = etl["load_dim_produto"](df_clean)

    ti.xcom_push(key="dim_produto",   value=df_with_sk.to_json(orient="split", date_format="iso"))
    ti.xcom_push(key="dim_operadora", value=op_json)


def task_load_beneficiarios(**ctx):
    import pandas as pd
    etl = _import_etl()

    ti = ctx["ti"]
    raw_json  = ti.xcom_pull(task_ids="extract_all_sources",         key="raw_beneficiarios")
    op_json   = ti.xcom_pull(task_ids="transform_and_load_produtos", key="dim_operadora")
    prod_json = ti.xcom_pull(task_ids="transform_and_load_produtos", key="dim_produto")

    # CORRIGIDO: io.StringIO em todas as leituras
    df_raw  = pd.read_json(io.StringIO(raw_json),  orient="split")
    df_op   = pd.read_json(io.StringIO(op_json),   orient="split")
    df_prod = pd.read_json(io.StringIO(prod_json), orient="split")

    df_clean = etl["transform_beneficiarios"](df_raw, df_op, df_prod)
    etl["load_fact_beneficiarios"](df_clean)


def task_load_financeiro(**ctx):
    import pandas as pd
    etl = _import_etl()

    ti = ctx["ti"]
    raw_json = ti.xcom_pull(task_ids="extract_all_sources",          key="raw_financeiro")
    op_json  = ti.xcom_pull(task_ids="transform_and_load_produtos",  key="dim_operadora")

    # CORRIGIDO: io.StringIO em todas as leituras
    df_raw = pd.read_json(io.StringIO(raw_json), orient="split")
    df_op  = pd.read_json(io.StringIO(op_json),  orient="split")

    df_clean = etl["transform_financeiro"](df_raw, df_op)
    etl["load_fact_financeiro"](df_clean)


def task_quality_checks(**ctx):
    etl = _import_etl()
    etl["run_all_checks"]()


# ─────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="unimed_fortaleza_etl_360",
    description="Pipeline ETL completo — Unimed Fortaleza 360°",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 1 * *",
    catchup=False,
    tags=["unimed", "ans", "etl", "star-schema"],
    doc_md="""
## Unimed Fortaleza 360° — ETL Pipeline

**Fontes:**
- `3T2025.csv` → fact_financeiro
- `Relatorio_cadop.csv` → dim_operadora
- `pda-008-*.csv` → dim_produto
- `pda-024-icb-CE-*.csv` → fact_beneficiarios

**Stack:** DuckDB (staging) → pandas (transform) → PostgreSQL (star schema)

**Destino Power BI:** conectar no PostgreSQL com DirectQuery ou Import.
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    extract = PythonOperator(
        task_id="extract_all_sources",
        python_callable=task_extract,
    )

    load_calendario = PythonOperator(
        task_id="transform_and_load_calendario",
        python_callable=task_load_calendario,
    )

    load_operadoras = PythonOperator(
        task_id="transform_and_load_operadoras",
        python_callable=task_load_operadoras,
    )

    load_produtos = PythonOperator(
        task_id="transform_and_load_produtos",
        python_callable=task_load_produtos,
    )

    load_beneficiarios = PythonOperator(
        task_id="transform_and_load_beneficiarios",
        python_callable=task_load_beneficiarios,
    )

    load_financeiro = PythonOperator(
        task_id="transform_and_load_financeiro",
        python_callable=task_load_financeiro,
    )

    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=task_quality_checks,
    )

    # ── Dependências ──────────────────────────────────────────────
    (
        start
        >> extract
        >> [load_calendario, load_operadoras]
    )
    load_operadoras >> load_produtos
    load_produtos >> [load_beneficiarios, load_financeiro]
    [load_beneficiarios, load_financeiro, load_calendario] >> quality >> end