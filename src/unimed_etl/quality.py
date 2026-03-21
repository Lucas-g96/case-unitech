"""
quality.py
Checks básicos de qualidade de dados pós-carga.
Levanta exceções se thresholds críticos forem violados.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

from unimed_etl.config import DW, TABLES, REGISTRO_ANS_UNIMED_FORTALEZA


def _engine():
    return create_engine(DW.url, pool_pre_ping=True)


def check_row_counts(min_rows: dict[str, int] | None = None) -> dict[str, int]:
    """Verifica contagem mínima de linhas por tabela."""
    defaults = {
        TABLES["dim_operadora"]:     100,
        TABLES["dim_produto"]:       10,
        TABLES["fact_beneficiarios"]:1,
        TABLES["fact_financeiro"]:   1,
    }
    thresholds = min_rows or defaults

    counts: dict[str, int] = {}
    with _engine().connect() as conn:
        for table, min_count in thresholds.items():
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            counts[table] = result
            if result < min_count:
                raise ValueError(
                    f"[QUALITY] '{table}' tem {result} linhas — mínimo esperado: {min_count}"
                )
            logger.info(f"[QUALITY] {table}: {result:,} linhas ✓")

    return counts


def check_unimed_fortaleza_presente() -> bool:
    """Verifica se Unimed Fortaleza está nas dimensões."""
    with _engine().connect() as conn:
        result = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {TABLES['dim_operadora']} "
                f"WHERE registro_ans = :reg"
            ),
            {"reg": REGISTRO_ANS_UNIMED_FORTALEZA},
        ).scalar()

    if result == 0:
        raise ValueError(
            f"[QUALITY] Unimed Fortaleza (ANS {REGISTRO_ANS_UNIMED_FORTALEZA}) "
            "não encontrada em dim_operadora!"
        )

    logger.success(f"[QUALITY] Unimed Fortaleza (ANS {REGISTRO_ANS_UNIMED_FORTALEZA}) presente ✓")
    return True


def check_nulls_fks() -> None:
    """Verifica proporção de FKs nulas nas facts (máx. 5%)."""
    checks = [
        (TABLES["fact_beneficiarios"], "sk_operadora"),
        (TABLES["fact_financeiro"],    "sk_operadora"),
    ]
    with _engine().connect() as conn:
        for table, col in checks:
            total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 1
            nulls = conn.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
            ).scalar()
            pct = nulls / total * 100
            if pct > 5:
                raise ValueError(
                    f"[QUALITY] {table}.{col}: {pct:.1f}% nulos — acima do limite de 5%!"
                )
            logger.info(f"[QUALITY] {table}.{col}: {pct:.2f}% nulos ✓")


def check_sinistralidade_plausivel() -> None:
    """
    Sinistralidade geral do mercado deve estar entre 50% e 150%
    (valores extremos indicam dados corrompidos).
    """
    sql = """
        SELECT
            SUM(CASE WHEN grupo_conta = 'DESPESA_ASSISTENCIAL' THEN vl_saldo_final ELSE 0 END) /
            NULLIF(SUM(CASE WHEN grupo_conta = 'RECEITA_EVENTOS' THEN vl_saldo_final ELSE 0 END), 0) * 100
            AS sinistralidade
        FROM ans.fact_financeiro
    """
    with _engine().connect() as conn:
        sinistralidade = conn.execute(text(sql)).scalar()

    if sinistralidade is None:
        logger.warning("[QUALITY] Não foi possível calcular sinistralidade — dados insuficientes.")
        return

    logger.info(f"[QUALITY] Sinistralidade geral: {sinistralidade:.1f}%")
    if not (20 <= sinistralidade <= 200):
        raise ValueError(
            f"[QUALITY] Sinistralidade {sinistralidade:.1f}% fora do range plausível (20%-200%)"
        )
    logger.success("[QUALITY] Sinistralidade dentro do range esperado ✓")


def run_all_checks() -> None:
    """Executa todos os checks em sequência."""
    logger.info("═" * 60)
    logger.info("[QUALITY] Iniciando validações de qualidade de dados…")
    check_row_counts()
    check_unimed_fortaleza_presente()
    check_nulls_fks()
    check_sinistralidade_plausivel()
    logger.success("[QUALITY] Todas as validações passaram ✓")
    logger.info("═" * 60)
