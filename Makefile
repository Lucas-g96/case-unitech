# ══════════════════════════════════════════════════════════════════
# Makefile — Unimed Fortaleza 360°
# ══════════════════════════════════════════════════════════════════

.PHONY: help setup up down logs etl test lint format clean

# Default target
help:
	@echo ""
	@echo "  Unimed Fortaleza 360° — Comandos disponíveis"
	@echo "  ─────────────────────────────────────────────"
	@echo "  make setup       Cria .env e instala dependências (UV)"
	@echo "  make up          Sobe toda a stack Docker"
	@echo "  make down        Para e remove containers"
	@echo "  make logs        Exibe logs do Airflow scheduler"
	@echo "  make etl         Executa o pipeline ETL manualmente"
	@echo "  make test        Roda testes (pytest)"
	@echo "  make lint        Verifica qualidade do código (ruff)"
	@echo "  make format      Formata o código (ruff format)"
	@echo "  make clean       Remove arquivos temporários"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────
setup:
	@[ -f .env ] || cp .env.example .env
	@echo "✓ .env criado"
	uv sync --all-extras
	@echo "✓ Dependências instaladas com UV"

# ── Docker ────────────────────────────────────────────────────────
up:
	@mkdir -p data/raw logs
	docker compose up -d --build
	@echo ""
	@echo "  ✓ Stack iniciada!"
	@echo "  → Airflow UI: http://localhost:8080  (admin/admin)"
	@echo "  → PostgreSQL DW: localhost:5432"
	@echo ""

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

# ── ETL manual (sem Airflow) ──────────────────────────────────────
etl:
	uv run python -c "
from unimed_etl.extract import extract_all
from unimed_etl.transform import build_dim_calendario, transform_operadoras, transform_produtos, transform_beneficiarios, transform_financeiro
from unimed_etl.load import load_dim_calendario, load_dim_operadora, load_dim_produto, load_fact_beneficiarios, load_fact_financeiro
from unimed_etl.quality import run_all_checks

print('── Extraindo fontes ──')
raw = extract_all()

print('── Carregando Calendário ──')
load_dim_calendario(build_dim_calendario())

print('── Carregando Operadoras ──')
df_op = load_dim_operadora(transform_operadoras(raw['operadoras']))

print('── Carregando Produtos ──')
df_prod = load_dim_produto(transform_produtos(raw['produtos']))

print('── Carregando Beneficiários ──')
load_fact_beneficiarios(transform_beneficiarios(raw['beneficiarios'], df_op, df_prod))

print('── Carregando Financeiro ──')
load_fact_financeiro(transform_financeiro(raw['financeiro'], df_op))

print('── Quality Checks ──')
run_all_checks()
print('Pipeline concluído ✓')
"

# ── Testes ────────────────────────────────────────────────────────
test:
	uv run pytest

test-cov:
	uv run pytest --cov=src/unimed_etl --cov-report=html
	@echo "→ Abra htmlcov/index.html para ver a cobertura"

# ── Qualidade de código ───────────────────────────────────────────
lint:
	uv run ruff check src/ dags/ tests/

format:
	uv run ruff format src/ dags/ tests/

# ── Limpeza ──────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -f /tmp/unimed_staging.duckdb
	@echo "✓ Arquivos temporários removidos"
