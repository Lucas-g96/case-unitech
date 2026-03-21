"""
tests/test_transform.py
Testes unitários das funções de transformação.
Usa DataFrames sintéticos — não depende dos CSVs reais.
"""

import pytest
import pandas as pd
from datetime import date

from unimed_etl.transform import (
    _normalize_cols,
    _clean_monetary,
    build_dim_calendario,
    transform_operadoras,
    transform_produtos,
)


# ── Helpers ───────────────────────────────────────────────────────

def test_normalize_cols():
    df = pd.DataFrame(columns=["Nome Fantasia", "UF/Estado", "Data.Registro"])
    df = _normalize_cols(df)
    assert list(df.columns) == ["nome_fantasia", "uf_estado", "data_registro"]


def test_clean_monetary():
    s = pd.Series(["R$ 1.234,56", "R$ -500,00", "0", None, "abc"])
    result = _clean_monetary(s)
    assert result[0] == pytest.approx(1234.56)
    assert result[1] == pytest.approx(-500.0)
    assert result[2] == pytest.approx(0.0)
    assert result[3] == pytest.approx(0.0)  # None → 0
    assert result[4] == pytest.approx(0.0)  # 'abc' → 0


# ── Calendário ────────────────────────────────────────────────────

def test_build_dim_calendario_shape():
    df = build_dim_calendario(start=date(2025, 1, 1), end=date(2025, 12, 31))
    assert len(df) == 365
    assert "sk_data" in df.columns
    assert "ano" in df.columns
    assert "trimestre" in df.columns


def test_build_dim_calendario_weekend():
    df = build_dim_calendario(start=date(2025, 1, 1), end=date(2025, 1, 7))
    # 2025-01-04 é sábado, 2025-01-05 é domingo
    weekends = df[df["is_fim_semana"] == True]
    assert len(weekends) == 2


# ── Operadoras ────────────────────────────────────────────────────

SAMPLE_OPERADORAS = pd.DataFrame({
    "Registro_ANS":      ["359017", "000001", "359017"],  # duplicata intencional
    "Nome_Fantasia":     ["Unimed Fortaleza", "Outra Op", "Unimed Fortaleza Dup"],
    "Razao_Social":      ["Unimed Fortaleza Coop", "Outra S/A", "Unimed Dup Coop"],
    "Modalidade_Operadora": ["Cooperativa Médica", "Medicina de Grupo", "Cooperativa Médica"],
    "UF":                ["CE", "SP", "CE"],
    "Municipio":         ["Fortaleza", "São Paulo", "Fortaleza"],
    "CNPJ":              ["00.000.000/0001-00", None, "00.000.000/0001-00"],
    "Situacao":          ["Ativa", "Ativa", "Ativa"],
})


def test_transform_operadoras_removes_duplicates():
    df = transform_operadoras(SAMPLE_OPERADORAS)
    assert df["registro_ans"].nunique() == len(df)


def test_transform_operadoras_zero_pads_registro():
    df = transform_operadoras(SAMPLE_OPERADORAS)
    # registro_ans deve ter ao menos 6 caracteres (zero-padded)
    assert all(len(r) >= 6 for r in df["registro_ans"])


def test_transform_operadoras_uf_uppercase():
    df = transform_operadoras(SAMPLE_OPERADORAS)
    assert all(u == u.upper() for u in df["uf"].dropna())


# ── Produtos ─────────────────────────────────────────────────────

SAMPLE_PRODUTOS = pd.DataFrame({
    "Registro_ANS": ["359017", "359017", "000001"],
    "CD_Produto":   ["001", "002", "001"],
    "Nome_Plano":   ["Plano A", "Plano B", "Plano X"],
    "Segmentacao":  ["Ambulatorial", "Hospitalar", "Ambulatorial"],
    "Tipo_Contratacao": ["IND", "COL_EMP", "ADESAO"],
    "Abrangencia":  ["Municipal", "Estadual", "Nacional"],
    "Coparticipacao": ["S", "N", "S"],
})


def test_transform_produtos_tipo_contratacao_normalizado():
    df = transform_produtos(SAMPLE_PRODUTOS)
    assert "Individual" in df["tipo_contratacao"].values
    assert "Coletivo Empresarial" in df["tipo_contratacao"].values


def test_transform_produtos_no_duplicates():
    df = transform_produtos(SAMPLE_PRODUTOS)
    assert df.duplicated(subset=["registro_ans", "cd_produto"]).sum() == 0
