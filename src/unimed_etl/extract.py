"""
extract.py
Lê os CSVs da ANS usando DuckDB (auto-detect, encoding, tipagem).
Retorna DataFrames Pandas prontos para a camada de transformação.
"""

from __future__ import annotations

import duckdb
import pandas as pd
from loguru import logger
from pathlib import Path

from unimed_etl.config import DUCKDB_PATH, FILES


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Retorna conexão DuckDB persistente em arquivo."""
    conn = duckdb.connect(DUCKDB_PATH)
    return conn


def _read_csv_duckdb(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    table_name: str,
    delimiter: str = ";",
    encoding: str = "latin-1",
    sample_size: int = 50_000,
) -> pd.DataFrame:
    """
    Lê um CSV via DuckDB com:
      • auto-detecção de tipos
      • tratamento de encoding (latin-1 padrão ANS)
      • registro em tabela staging no DuckDB
    """
    logger.info(f"[EXTRACT] Lendo {path.name} → staging '{table_name}'")

    # DuckDB lê direto do disco — zero cópia para arquivos grandes
    conn.execute(f"DROP TABLE IF EXISTS staging_{table_name}")
    conn.execute(f"""
        CREATE TABLE staging_{table_name} AS
        SELECT *
        FROM read_csv_auto(
            '{path}',
            delim         = '{delimiter}',
            header        = true,
            
            sample_size   = {sample_size},
            ignore_errors = true
        )
    """)

    row_count = conn.execute(f"SELECT COUNT(*) FROM staging_{table_name}").fetchone()[0]
    logger.info(f"[EXTRACT] {table_name}: {row_count:,} linhas carregadas")

    df = conn.execute(f"SELECT * FROM staging_{table_name}").df()
    return df


def extract_operadoras() -> pd.DataFrame:
    """Relatorio_cadop.csv — cadastro de operadoras ativas."""
    conn = _get_conn()
    df = _read_csv_duckdb(conn, FILES["operadoras"], "operadoras")
    conn.close()
    return df


def extract_produtos() -> pd.DataFrame:
    """pda-008 — características dos produtos/planos."""
    conn = _get_conn()
    df = _read_csv_duckdb(conn, FILES["produtos"], "produtos")
    conn.close()
    return df


def extract_beneficiarios() -> pd.DataFrame:
    """pda-024-icb-CE — informações de beneficiários (Ceará)."""
    conn = _get_conn()
    df = _read_csv_duckdb(conn, FILES["beneficiarios"], "beneficiarios")
    conn.close()
    return df


def extract_financeiro() -> pd.DataFrame:
    """3T2025.csv — demonstrações contábeis DIOPS (balancete 3T 2025)."""
    conn = _get_conn()
    df = _read_csv_duckdb(conn, FILES["financeiro"], "financeiro")
    conn.close()
    return df


def extract_all() -> dict[str, pd.DataFrame]:
    """Extrai todas as fontes em uma única conexão DuckDB (mais eficiente)."""
    conn = _get_conn()
    results = {}

    for key, path in FILES.items():
        try:
            results[key] = _read_csv_duckdb(conn, path, key)
        except Exception as exc:
            logger.error(f"[EXTRACT] Falha ao ler '{key}': {exc}")
            raise

    conn.close()
    logger.success("[EXTRACT] Todas as fontes extraídas com sucesso.")
    return results