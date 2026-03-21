"""
transform.py
Limpeza, tipagem e construção do Star Schema com os nomes reais das colunas ANS.

Fontes confirmadas:
  - 3T2025.csv              → REG_ANS, DATA, CD_CONTA_CONTABIL, DESCRICAO, VL_SALDO_INICIAL, VL_SALDO_FINAL
  - Relatorio_cadop.csv     → REGISTRO_OPERADORA, Nome_Fantasia, Razao_Social, Modalidade, UF, Cidade, CNPJ, Data_Registro_ANS
  - pda-008-*.csv           → REGISTRO_OPERADORA, CD_PLANO, NM_PLANO, CONTRATACAO, ABRANGENCIA_COBERTURA, FATOR_MODERADOR, SITUACAO_PLANO
  - pda-024-icb-CE-*.csv   → CD_OPERADORA, ID_CMPT_MOVEL, SG_UF, NM_MUNICIPIO, TP_SEXO, DE_FAIXA_ETARIA,
                              DE_CONTRATACAO_PLANO, CD_PLANO, QT_BENEFICIARIO_ATIVO
"""

from __future__ import annotations

import duckdb
import pandas as pd
from loguru import logger
from datetime import date

from unimed_etl.config import DUCKDB_PATH


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DUCKDB_PATH)


def _clean_monetary(series: pd.Series) -> pd.Series:
    """Converte '3.094.590,67' -> 3094590.67 (padrao brasileiro ANS)."""
    return (
        series
        .astype(str)
        .str.strip()
        .str.replace(r"\s", "", regex=True)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )


def _classificar_grupo_conta(descricao: str) -> str:
    if not isinstance(descricao, str):
        return "OUTROS"
    d = descricao.upper()

    if any(k in d for k in [
        "CONTRAPRESTACAO", "CONTRAPRESTAÇÃO", "MENSALIDADE",
        "RECEITA DE PLANO", "RECEITAS DE PLANO", "RECEITAS ASSISTENCIAIS",
        "PREMIO", "PRÊMIO"
    ]):
        return "RECEITA_EVENTOS"

    if any(k in d for k in [
        "EVENTOS INDENIZADOS", "SINISTROS", "ASSISTENCIA A SAUDE",
        "ASSISTÊNCIA À SAÚDE", "DESPESAS ASSISTENCIAIS",
        "INTERNACAO", "INTERNAÇÃO", "CONSULTA", "EXAME", "PROCEDIMENTO"
    ]):
        return "DESPESA_ASSISTENCIAL"

    if any(k in d for k in ["RECEITAS FINANCEIRAS", "APLICACOES FINANCEIRAS", "RENDA FIXA"]):
        return "RECEITA_FINANCEIRA"

    if any(k in d for k in ["DESPESAS ADMINISTRATIVAS", "ADMINISTRATIVAS", "PESSOAL"]):
        return "DESPESA_ADM"

    return "OUTROS"


# ── dim_calendario ────────────────────────────────────────────────

def build_dim_calendario(
    start: date = date(2020, 1, 1),
    end: date = date(2026, 12, 31),
) -> pd.DataFrame:
    logger.info("[TRANSFORM] Construindo dim_calendario...")
    conn = _conn()
    conn.execute(f"""
        CREATE OR REPLACE TABLE dim_calendario AS
        SELECT
            dt::DATE AS sk_data,
            YEAR(dt) AS ano,
            CASE WHEN MONTH(dt) <= 6 THEN 1 ELSE 2 END AS semestre,
            QUARTER(dt) AS trimestre,
            MONTH(dt) AS mes,
            CASE MONTH(dt)
                WHEN 1  THEN 'Janeiro'   WHEN 2  THEN 'Fevereiro'
                WHEN 3  THEN 'Marco'     WHEN 4  THEN 'Abril'
                WHEN 5  THEN 'Maio'      WHEN 6  THEN 'Junho'
                WHEN 7  THEN 'Julho'     WHEN 8  THEN 'Agosto'
                WHEN 9  THEN 'Setembro'  WHEN 10 THEN 'Outubro'
                WHEN 11 THEN 'Novembro'  ELSE    'Dezembro'
            END AS nome_mes,
            WEEKOFYEAR(dt) AS semana_ano,
            DAY(dt) AS dia,
            CASE DAYOFWEEK(dt)
                WHEN 0 THEN 'Domingo'      WHEN 1 THEN 'Segunda-feira'
                WHEN 2 THEN 'Terca-feira'  WHEN 3 THEN 'Quarta-feira'
                WHEN 4 THEN 'Quinta-feira' WHEN 5 THEN 'Sexta-feira'
                ELSE 'Sabado'
            END AS dia_semana,
            DAYOFWEEK(dt) IN (0, 6) AS is_fim_semana
        FROM generate_series(DATE '{start}', DATE '{end}', INTERVAL '1 day') t(dt)
    """)
    df = conn.execute("SELECT * FROM dim_calendario ORDER BY sk_data").df()
    conn.close()
    logger.success(f"[TRANSFORM] dim_calendario: {len(df):,} datas.")
    return df


# ── dim_operadora  <- Relatorio_cadop.csv ─────────────────────────

def transform_operadoras(df_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Transformando dim_operadora...")
    df = df_raw.copy()

    df = df.rename(columns={
        "REGISTRO_OPERADORA": "registro_ans",
        "Razao_Social":       "razao_social",
        "Nome_Fantasia":      "nome_fantasia",
        "Modalidade":         "modalidade",
        "Cidade":             "municipio",
        "UF":                 "uf",
        "CNPJ":               "cnpj",
        "Data_Registro_ANS":  "data_registro",
    })

    df["registro_ans"]  = df["registro_ans"].astype(str).str.strip().str.zfill(6)
    df["uf"]            = df["uf"].astype(str).str.strip().str.upper().str[:2]
    df["modalidade"]    = df["modalidade"].astype(str).str.strip()
    df["nome_fantasia"] = df["nome_fantasia"].astype(str).str.strip().replace("nan", None)
    df["razao_social"]  = df["razao_social"].astype(str).str.strip()
    df["data_registro"] = pd.to_datetime(df["data_registro"], errors="coerce").dt.date

    df = df.drop_duplicates(subset=["registro_ans"], keep="last")

    keep = ["registro_ans", "nome_fantasia", "razao_social",
            "modalidade", "uf", "municipio", "cnpj", "data_registro"]
    df = df[[c for c in keep if c in df.columns]].reset_index(drop=True)

    logger.success(f"[TRANSFORM] dim_operadora: {len(df):,} operadoras.")
    return df


# ── dim_produto  <- pda-008-*.csv ─────────────────────────────────

def transform_produtos(df_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Transformando dim_produto...")
    df = df_raw.copy()

    df = df.rename(columns={
        "REGISTRO_OPERADORA":   "registro_ans",
        "CD_PLANO":             "cd_produto",
        "NM_PLANO":             "nome_plano",
        "SGMT_ASSISTENCIAL":    "segmentacao",
        "GR_CONTRATACAO":       "tipo_contratacao",
        "ABRANGENCIA_COBERTURA":"abrangencia",
        "FATOR_MODERADOR":      "coparticipacao",
        "DT_REGISTRO_PLANO":    "dt_registro_produto",
        "SITUACAO_PLANO":       "situacao_produto",
    })

    df["registro_ans"] = df["registro_ans"].astype(str).str.strip().str.zfill(6)
    df["cd_produto"]   = df["cd_produto"].astype(str).str.strip()

    if "coparticipacao" in df.columns:
        df["coparticipacao"] = df["coparticipacao"].astype(str).str.strip().apply(
            lambda x: "S" if "copart" in x.lower() else "N"
        )

    if "dt_registro_produto" in df.columns:
        df["dt_registro_produto"] = pd.to_datetime(
            df["dt_registro_produto"], errors="coerce"
        ).dt.date

    df = df.drop_duplicates(subset=["registro_ans", "cd_produto"], keep="last")

    keep = ["registro_ans", "cd_produto", "nome_plano", "segmentacao",
            "tipo_contratacao", "abrangencia", "coparticipacao",
            "dt_registro_produto", "situacao_produto"]
    df = df[[c for c in keep if c in df.columns]].reset_index(drop=True)

    logger.success(f"[TRANSFORM] dim_produto: {len(df):,} produtos.")
    return df


# ── fact_beneficiarios  <- pda-024-icb-CE-*.csv ───────────────────

def transform_beneficiarios(
    df_raw: pd.DataFrame,
    df_operadora: pd.DataFrame,
    df_produto: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("[TRANSFORM] Transformando fact_beneficiarios...")
    df = df_raw.copy()

    df = df.rename(columns={
        "ID_CMPT_MOVEL":             "competencia",
        "CD_OPERADORA":              "registro_ans",
        "SG_UF":                     "uf",
        "NM_MUNICIPIO":              "municipio",
        "TP_SEXO":                   "sexo",
        "DE_FAIXA_ETARIA":           "faixa_etaria",
        "DE_CONTRATACAO_PLANO":      "tipo_contratacao",
        "CD_PLANO":                  "cd_produto",
        "QT_BENEFICIARIO_ATIVO":     "qtd_beneficiarios",
        "QT_BENEFICIARIO_ADERIDO":   "qt_aderido",
        "QT_BENEFICIARIO_CANCELADO": "qt_cancelado",
    })

    df["registro_ans"] = df["registro_ans"].astype(str).str.strip().str.zfill(6)

    # "2026-01" -> 2026-01-01
    df["sk_data"] = pd.to_datetime(
        df["competencia"].astype(str).str.strip(),
        format="%Y-%m", errors="coerce"
    ).dt.date

    df["qtd_beneficiarios"] = pd.to_numeric(
        df["qtd_beneficiarios"], errors="coerce"
    ).fillna(0).astype(int)

    df["uf"]        = df["uf"].astype(str).str.strip().str.upper().str[:2]
    df["cd_produto"] = df["cd_produto"].astype(str).str.strip()

    # FKs das dimensões — garante str em ambos os lados (XCom deserializa como int64)
    op_keys = df_operadora[["registro_ans", "sk_operadora"]].drop_duplicates().copy()
    op_keys["registro_ans"] = op_keys["registro_ans"].astype(str).str.strip().str.zfill(6)
    df = df.merge(op_keys, on="registro_ans", how="left")

    prod_keys = df_produto[["registro_ans", "cd_produto", "sk_produto"]].drop_duplicates().copy()
    prod_keys["registro_ans"] = prod_keys["registro_ans"].astype(str).str.strip().str.zfill(6)
    prod_keys["cd_produto"]   = prod_keys["cd_produto"].astype(str).str.strip()
    df = df.merge(prod_keys, on=["registro_ans", "cd_produto"], how="left")

    keep = ["sk_data", "sk_operadora", "sk_produto",
            "uf", "municipio", "faixa_etaria", "sexo",
            "tipo_contratacao", "qtd_beneficiarios"]
    df = df[[c for c in keep if c in df.columns]]
    df = df.dropna(subset=["sk_operadora", "sk_data"]).reset_index(drop=True)

    # Converte SKs float64→int — merge com NULLs promove float64,
    # que o PostgreSQL rejeita em colunas INTEGER
    df["sk_operadora"] = df["sk_operadora"].astype(int)
    if "sk_produto" in df.columns:
        # Int64 nullable: mantém NaN como pd.NA (Python None no INSERT → NULL no PG)
        df["sk_produto"] = pd.array(
            pd.to_numeric(df["sk_produto"], errors="coerce"), dtype="Int64"
        )

    logger.success(f"[TRANSFORM] fact_beneficiarios: {len(df):,} registros.")
    return df


# ── fact_financeiro  <- 3T2025.csv ────────────────────────────────

def transform_financeiro(
    df_raw: pd.DataFrame,
    df_operadora: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("[TRANSFORM] Transformando fact_financeiro...")
    df = df_raw.copy()

    df = df.rename(columns={
        "REG_ANS":           "registro_ans",
        "DATA":              "sk_data",
        "CD_CONTA_CONTABIL": "cd_conta_contabil",
        "DESCRICAO":         "descricao_conta",
        "VL_SALDO_INICIAL":  "vl_saldo_inicial",
        "VL_SALDO_FINAL":    "vl_saldo_final",
    })

    df["registro_ans"]     = df["registro_ans"].astype(str).str.strip().str.zfill(6)
    df["sk_data"]          = pd.to_datetime(df["sk_data"], errors="coerce").dt.date
    df["vl_saldo_inicial"] = _clean_monetary(df["vl_saldo_inicial"])
    df["vl_saldo_final"]   = _clean_monetary(df["vl_saldo_final"])
    df["trimestre_ref"]    = "3T2025"
    df["grupo_conta"]      = df["descricao_conta"].apply(_classificar_grupo_conta)

    # FK operadora — garante str em ambos os lados (XCom deserializa como int64)
    op_keys = df_operadora[["registro_ans", "sk_operadora"]].drop_duplicates().copy()
    op_keys["registro_ans"] = op_keys["registro_ans"].astype(str).str.strip().str.zfill(6)
    df = df.merge(op_keys, on="registro_ans", how="left")

    keep = ["sk_data", "sk_operadora", "cd_conta_contabil",
            "descricao_conta", "grupo_conta",
            "vl_saldo_inicial", "vl_saldo_final", "trimestre_ref"]
    df = df[[c for c in keep if c in df.columns]]
    df = df.dropna(subset=["sk_operadora", "sk_data"]).reset_index(drop=True)
    df["sk_operadora"] = df["sk_operadora"].astype(int)

    logger.success(f"[TRANSFORM] fact_financeiro: {len(df):,} registros.")
    return df