#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════
# git_setup.sh — Inicializa o repositório Git e faz o primeiro commit
# Uso: bash git_setup.sh <url-do-seu-repo-remoto>
# Ex:  bash git_setup.sh git@github.com:seu-user/unimed-fortaleza-360.git
# ══════════════════════════════════════════════════════════════════

set -euo pipefail

REMOTE_URL="${1:-}"

echo "──────────────────────────────────────────"
echo "  Git setup — Unimed Fortaleza 360°"
echo "──────────────────────────────────────────"

# 1. Init
git init
git branch -M main

# 2. Configura o .env (não versionar o real)
cp .env.example .env 2>/dev/null || true

# 3. Primeiro commit
git add .
git commit -m "feat: estrutura inicial do pipeline ETL Unimed Fortaleza 360°

- Stack: UV + DuckDB + PostgreSQL + Airflow (Docker)
- Star Schema: dim_calendario, dim_operadora, dim_produto
- Facts: fact_beneficiarios, fact_financeiro
- DAG Airflow: unimed_fortaleza_etl_360 (schedule mensal)
- Views analíticas: vw_sinistralidade, vw_market_share
- Quality checks pós-carga
- Testes unitários (pytest)"

# 4. Adiciona remote e envia (se URL fornecida)
if [ -n "$REMOTE_URL" ]; then
    git remote add origin "$REMOTE_URL"
    git push -u origin main
    echo "✓ Código enviado para $REMOTE_URL"
else
    echo "ℹ️  Nenhum remote informado. Para enviar ao GitHub:"
    echo "   git remote add origin <url>"
    echo "   git push -u origin main"
fi

echo ""
echo "✓ Repositório pronto!"
