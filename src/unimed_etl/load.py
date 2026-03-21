"""
load.py
Carrega os DataFrames transformados no PostgreSQL (Star Schema).
Usa SQLAlchemy + psycopg2. Suporte a upsert (INSERT … ON CONFLICT DO UPDATE)
para idempotência (DAG pode rodar novamente sem duplicar dados).
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from unimed_etl.config import DW, TABLES, SCHEMA


def _get_engine() -> Engine:
    return create_engine(DW.url, pool_pre_ping=True)


# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────

def _upsert_df(
    engine: Engine,
    df: pd.DataFrame,
    table: str,
    conflict_cols: list[str],
    update_cols: list[str] | None = None,
    chunk_size: int = 5_000,
) -> int:
    """
    Faz upsert via temp table + INSERT … ON CONFLICT.
    Retorna quantidade de linhas afetadas.
    """
    if df.empty:
        logger.warning(f"[LOAD] DataFrame vazio para '{table}' — pulando.")
        return 0

    schema, tbl = table.split(".")
    tmp = f"tmp_{tbl}"
    all_cols = df.columns.tolist()
    upd_cols = update_cols or [c for c in all_cols if c not in conflict_cols]

    total = 0
    with engine.begin() as conn:
        # Cria temp table como cópia da estrutura alvo
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp}"))
        conn.execute(text(f"CREATE TEMP TABLE {tmp} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"))

        # Carrega chunks na temp table via INSERT nativo (compatível SQLAlchemy 2.0)
        # Evita to_sql que requer .cursor() — incompatível com SQLAlchemy 2.0 Connection
        cols_str = ", ".join(all_cols)
        params_str = ", ".join(f":{c}" for c in all_cols)
        insert_sql = text(f"INSERT INTO {tmp} ({cols_str}) VALUES ({params_str})")

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            records = chunk.where(pd.notna(chunk), other=None).to_dict(orient="records")
            conn.execute(insert_sql, records)
            total += len(chunk)

        # Upsert da temp → tabela definitiva
        conflict_str = ", ".join(conflict_cols)
        if upd_cols:
            set_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in upd_cols)
            upsert_sql = f"""
                INSERT INTO {table} ({', '.join(all_cols)})
                SELECT {', '.join(all_cols)} FROM {tmp}
                ON CONFLICT ({conflict_str}) DO UPDATE SET {set_str}
            """
        else:
            upsert_sql = f"""
                INSERT INTO {table} ({', '.join(all_cols)})
                SELECT {', '.join(all_cols)} FROM {tmp}
                ON CONFLICT ({conflict_str}) DO NOTHING
            """
        conn.execute(text(upsert_sql))

    logger.info(f"[LOAD] '{table}': {total:,} linhas processadas (upsert).")
    return total


# ──────────────────────────────────────────────────────────────────
# Load por dimensão / fato
# ──────────────────────────────────────────────────────────────────

def load_dim_calendario(df: pd.DataFrame) -> None:
    logger.info("[LOAD] Carregando dim_calendario…")
    engine = _get_engine()
    _upsert_df(engine, df, TABLES["dim_calendario"], conflict_cols=["sk_data"])
    logger.success("[LOAD] dim_calendario ✓")


def load_dim_operadora(df: pd.DataFrame) -> pd.DataFrame:
    """
    Carrega dim_operadora e retorna o DataFrame enriquecido com sk_operadora
    (surrogate key gerada pelo PostgreSQL) — necessário para fazer FK nas facts.
    """
    logger.info("[LOAD] Carregando dim_operadora…")
    engine = _get_engine()

    non_key_cols = [c for c in df.columns if c != "registro_ans"]
    _upsert_df(engine, df, TABLES["dim_operadora"], conflict_cols=["registro_ans"], update_cols=non_key_cols)

    # Recupera SK geradas via query nativa (pd.read_sql incompatível com SQLAlchemy 2.0)
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT sk_operadora, registro_ans FROM {TABLES['dim_operadora']}"))
        df_sk = pd.DataFrame(result.fetchall(), columns=result.keys())

    df_out = df.merge(df_sk, on="registro_ans", how="left")
    logger.success("[LOAD] dim_operadora ✓")
    return df_out


def load_dim_produto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Carrega dim_produto e retorna DataFrame com sk_produto.
    """
    logger.info("[LOAD] Carregando dim_produto…")
    engine = _get_engine()

    non_key_cols = [c for c in df.columns if c not in ("registro_ans", "cd_produto")]
    _upsert_df(
        engine, df, TABLES["dim_produto"],
        conflict_cols=["registro_ans", "cd_produto"],
        update_cols=non_key_cols,
    )

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT sk_produto, registro_ans, cd_produto FROM {TABLES['dim_produto']}"))
        df_sk = pd.DataFrame(result.fetchall(), columns=result.keys())

    df_out = df.merge(df_sk, on=["registro_ans", "cd_produto"], how="left")
    logger.success("[LOAD] dim_produto ✓")
    return df_out


def load_fact_beneficiarios(df: pd.DataFrame) -> None:
    logger.info("[LOAD] Carregando fact_beneficiarios…")
    engine = _get_engine()

    # Apaga registros da competência que está sendo recarregada
    with engine.begin() as conn:
        datas = df["sk_data"].dropna().unique().tolist()
        if datas:
            placeholders = ", ".join(f"'{d}'" for d in datas)
            conn.execute(
                text(f"DELETE FROM {TABLES['fact_beneficiarios']} WHERE sk_data IN ({placeholders})")
            )

    chunk_size = 10_000
    all_cols_b = df.columns.tolist()
    cols_str_b = ", ".join(all_cols_b)
    params_str_b = ", ".join(f":{c}" for c in all_cols_b)
    insert_sql_b = text(f"INSERT INTO {SCHEMA}.fact_beneficiarios ({cols_str_b}) VALUES ({params_str_b})")

    with engine.begin() as conn:
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            records = chunk.where(pd.notna(chunk), other=None).to_dict(orient="records")
            conn.execute(insert_sql_b, records)

    logger.success(f"[LOAD] fact_beneficiarios ✓ — {len(df):,} linhas.")


def load_fact_financeiro(df: pd.DataFrame) -> None:
    logger.info("[LOAD] Carregando fact_financeiro…")
    engine = _get_engine()

    # Remove registros do mesmo trimestre antes de reinserir
    trimestres = df["trimestre_ref"].dropna().unique().tolist()
    if trimestres:
        with engine.begin() as conn:
            placeholders = ", ".join(f"'{t}'" for t in trimestres)
            conn.execute(
                text(f"DELETE FROM {TABLES['fact_financeiro']} WHERE trimestre_ref IN ({placeholders})")
            )

    all_cols_f = df.columns.tolist()
    cols_str_f = ", ".join(all_cols_f)
    params_str_f = ", ".join(f":{c}" for c in all_cols_f)
    insert_sql_f = text(f"INSERT INTO {SCHEMA}.fact_financeiro ({cols_str_f}) VALUES ({params_str_f})")

    with engine.begin() as conn:
        for i in range(0, len(df), chunk_size := 10_000):
            chunk = df.iloc[i : i + chunk_size]
            records = chunk.where(pd.notna(chunk), other=None).to_dict(orient="records")
            conn.execute(insert_sql_f, records)

    logger.success(f"[LOAD] fact_financeiro ✓ — {len(df):,} linhas.")