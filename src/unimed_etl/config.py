"""
config.py
Centraliza todas as configurações do projeto via variáveis de ambiente.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Caminhos base ─────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_PATH = Path(os.getenv("RAW_DATA_PATH", str(PROJECT_ROOT / "data" / "raw")))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/tmp/unimed_staging.duckdb")


# ── Data Warehouse ────────────────────────────────────────────────
@dataclass(frozen=True)
class DWConfig:
    # CORRIGIDO: dentro do Docker o host é o nome do serviço no compose,
    # não "localhost". Sobrescreva via variável de ambiente DW_HOST se necessário.
    host: str = field(default_factory=lambda: os.getenv("DW_HOST", "unimed_dw"))
    port: int = field(default_factory=lambda: int(os.getenv("DW_PORT", "5432")))
    db: str = field(default_factory=lambda: os.getenv("DW_DB", "unimed_dw"))
    user: str = field(default_factory=lambda: os.getenv("DW_USER", "dw_user"))
    password: str = field(default_factory=lambda: os.getenv("DW_PASSWORD", "dw_password"))

    @property
    def url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    @property
    def url_raw(self) -> str:
        """URL sem driver — usada pelo DuckDB postgres extension."""
        return f"host={self.host} port={self.port} dbname={self.db} user={self.user} password={self.password}"


DW = DWConfig()

# ── Unimed Fortaleza ──────────────────────────────────────────────
REGISTRO_ANS_UNIMED_FORTALEZA = os.getenv("REGISTRO_ANS_UNIMED_FORTALEZA", "317144")

# ── Arquivos de origem ────────────────────────────────────────────
FILES = {
    "financeiro": RAW_DATA_PATH / "3T2025.csv",
    "operadoras": RAW_DATA_PATH / "Relatorio_cadop.csv",
    "produtos": RAW_DATA_PATH / "pda-008-caracteristicas_produtos_saude_suplementar.csv",
    "beneficiarios": RAW_DATA_PATH / "pda-024-icb-CE-2026_01.csv",
}

# ── Schema / tabelas alvo ─────────────────────────────────────────
SCHEMA = "ans"

TABLES = {
    "dim_calendario": f"{SCHEMA}.dim_calendario",
    "dim_operadora": f"{SCHEMA}.dim_operadora",
    "dim_produto": f"{SCHEMA}.dim_produto",
    "fact_beneficiarios": f"{SCHEMA}.fact_beneficiarios",
    "fact_financeiro": f"{SCHEMA}.fact_financeiro",
}

# ── Mapeamento de grupos de contas contábeis (DIOPS) ─────────────
GRUPOS_CONTA = {
    # Receitas assistenciais (base da sinistralidade)
    "RECEITA_EVENTOS": [
        "CONTRAPRESTACOES_PECUNIARIAS",
        "RECEITA_EVENTOS_INDENIZADOS",
    ],
    # Despesas assistenciais (numerador da sinistralidade)
    "DESPESA_ASSISTENCIAL": [
        "EVENTOS_INDENIZADOS",
        "CONSTITUICAO_PROVISAO",
        "REVERSAO_PROVISAO",
    ],
    # Resultado operacional
    "DESPESA_ADM": ["DESPESAS_ADMINISTRATIVAS"],
    "RESULTADO": ["RESULTADO_PERIODO"],
}