# Painel 360° — Unimed Fortaleza

[![Power BI](https://img.shields.io/badge/Power%20BI-Acessar%20Dashboard-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://app.powerbi.com/view?r=eyJrIjoiMzMzZWM0YTktNjE2Mi00OWU2LWI4ODMtZjcyMjk5OTk0YTIxIiwidCI6IjRmMGYwZjk5LTE5NGEtNDgwNi1iZGEwLTcxYTRjNDAyNmZjYiJ9&pageName=f508232f05bfccafbeac)

Pipeline ETL completo e dashboard analítico para o mercado de saúde suplementar do Ceará, com foco na Unimed Fortaleza (ANS 317144).

---

## Visão Geral

Este projeto constrói um pipeline de dados end-to-end usando dados abertos da ANS (Agência Nacional de Saúde Suplementar), carregando-os em um Data Warehouse com modelagem Star Schema e expondo as análises em um dashboard Power BI com duas páginas: Panorama de Mercado e Saúde Financeira.

---

## Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Linguagem | Python 3.12 |
| Gerenciador de pacotes | UV |
| Staging / Extração | DuckDB |
| Transformação | Pandas |
| Orquestração | Apache Airflow (Docker) |
| Data Warehouse | PostgreSQL 15 |
| Visualização | Microsoft Power BI |
| Containerização | Docker + Docker Compose |

---

## Arquitetura do Pipeline

```
CSVs ANS (raw)
     │
     ▼
  DuckDB (staging)
  └── Leitura com auto-detecção de encoding
     │
     ▼
  Pandas (transform)
  ├── Tipagem e limpeza
  ├── Padronização de campos
  └── Mesclagem (merge) entre fontes
     │
     ▼
  PostgreSQL (star schema)
  └── Schema: ans
     │
     ▼
  Power BI (dashboard)
  ├── Página 1: Panorama de Mercado
  └── Página 2: Saúde Financeira
```

---

## Fontes de Dados (ANS)

| Arquivo | Descrição | Tabela destino |
|---|---|---|
| `3T2025.csv` | Demonstrações Contábeis DIOPS | `fact_financeiro` |
| `Relatorio_cadop.csv` | Registro de Operadoras Ativas | `dim_operadora` |
| `pda-008-caracteristicas_produtos_saude_suplementar.csv` | Características dos Planos | `dim_produto` |
| `pda-024-icb-CE-2026_01.csv` | Beneficiários — Ceará | `fact_beneficiarios` |

---

## Modelagem de Dados (Star Schema)

```
              dim_calendario          dim_operadora        dim_produto
              (sk_data PK)           (sk_operadora PK)    (sk_produto PK)
                   │  │                    │  │                  │
                   │  └────────────────────┘  │                  │
                   │         │                │                  │
            fact_financeiro  │      fact_beneficiarios ──────────┘
            (sk_data FK)     │      (sk_data FK)
            (sk_operadora FK)└───── (sk_operadora FK)
                                    (sk_produto FK)
```

### Tabelas

**dim_calendario**
- `sk_data` (PK), `ano`, `semestre`, `trimestre`, `mes`, `nome_mes`, `semana_ano`, `dia`, `dia_semana`, `is_fim_semana`

**dim_operadora**
- `sk_operadora` (PK), `registro_ans`, `nome_fantasia`, `razao_social`, `modalidade`, `uf`, `municipio`, `cnpj`, `data_registro`

**dim_produto**
- `sk_produto` (PK), `registro_ans`, `cd_produto`, `nome_plano`, `segmentacao`, `tipo_contratacao`, `abrangencia`, `coparticipacao`, `dt_registro_produto`, `situacao_produto`

**fact_beneficiarios**
- `sk_data` (FK), `sk_operadora` (FK), `sk_produto` (FK), `uf`, `municipio`, `faixa_etaria`, `sexo`, `tipo_contratacao`, `qtd_beneficiarios`

**fact_financeiro**
- `sk_data` (FK), `sk_operadora` (FK), `cd_conta_contabil`, `descricao_conta`, `grupo_conta`, `vl_saldo_inicial`, `vl_saldo_final`, `trimestre_ref`

### Views Analíticas

- `vw_sinistralidade` — receita, despesa e sinistralidade % por operadora e trimestre
- `vw_market_share` — participação de mercado por operadora, modalidade, UF e mês

---

## Estrutura do Projeto

```
unimed-fortaleza-360/
├── .env.example
├── .gitignore
├── .python-version          # Python 3.12
├── pyproject.toml
├── requirements.txt
├── Makefile
├── docker-compose.yml
├── docker/
│   ├── airflow/
│   │   └── Dockerfile
│   └── postgres/
│       └── init.sql         # Criação do star schema
├── dags/
│   └── unimed_etl_dag.py    # DAG principal do Airflow
├── src/
│   └── unimed_etl/
│       ├── config.py        # Configurações centralizadas
│       ├── extract.py       # Leitura dos CSVs via DuckDB
│       ├── transform.py     # Limpeza e transformação com Pandas
│       ├── load.py          # Carga no PostgreSQL via SQLAlchemy
│       └── quality.py       # Quality checks pós-carga
├── data/
│   └── raw/                 # CSVs da ANS (não versionados)
├── tests/
│   └── test_transform.py
└── docs/
    └── powerbi_guide.md
```

---

## DAG do Airflow

```
start
  └── extract_all_sources
        ├── transform_and_load_calendario
        └── transform_and_load_operadoras
              └── transform_and_load_produtos
                    ├── transform_and_load_beneficiarios
                    └── transform_and_load_financeiro
                          └── quality_checks
                                └── end
```

**Schedule:** Todo dia 1 do mês às 06h (`0 6 1 * *`)

**Idempotência:** Todas as cargas usam upsert (`INSERT ... ON CONFLICT DO UPDATE`) nas dimensões e DELETE + INSERT nas facts por competência/trimestre.

---

## Como Executar

### Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.12 (para desenvolvimento local)
- UV (`pip install uv`)

### 1. Clonar e configurar

```bash
cd unimed-fortaleza-360
cp .env.example .env
```

### 2. Colocar os CSVs da ANS

```bash
mkdir -p data/raw
# Copiar os 4 arquivos para data/raw/
```

### 3. Subir os containers

```bash
docker compose up -d
```

Aguarda todos os serviços ficarem healthy:

```bash
docker compose ps
```

### 4. Acessar o Airflow

- URL: http://localhost:8080
- Usuário: `admin`
- Senha: `admin`

Ativa a DAG `unimed_fortaleza_etl_360` e dispara manualmente.

### 5. Verificar os dados no PostgreSQL

```bash
docker exec -it unimed_dw psql -U dw_user -d unimed_dw
```

```sql
SELECT tabela, COUNT(*)
FROM (
  SELECT 'dim_operadora' AS tabela, COUNT(*) FROM ans.dim_operadora
  UNION ALL SELECT 'dim_produto',            COUNT(*) FROM ans.dim_produto
  UNION ALL SELECT 'fact_beneficiarios',     COUNT(*) FROM ans.fact_beneficiarios
  UNION ALL SELECT 'fact_financeiro',        COUNT(*) FROM ans.fact_financeiro
) t
GROUP BY tabela;
```

---

## Conexão Power BI

| Parâmetro | Valor |
|---|---|
| Servidor | `localhost` |
| Porta | `5432` |
| Banco | `unimed_dw` |
| Usuário | `dw_user` |
| Senha | conforme `.env` (`dw_password` por padrão) |
| Schema | `ans` |

### Tabelas a importar

```
ans.dim_calendario
ans.dim_operadora
ans.dim_produto
ans.fact_beneficiarios
ans.fact_financeiro
ans.vw_sinistralidade
ans.vw_market_share
```

### Relacionamentos no modelo

```
fact_beneficiarios.sk_data      → dim_calendario.sk_data      (N:1)
fact_beneficiarios.sk_operadora → dim_operadora.sk_operadora  (N:1)
fact_beneficiarios.sk_produto   → dim_produto.sk_produto      (N:1)
fact_financeiro.sk_data         → dim_calendario.sk_data      (N:1)
fact_financeiro.sk_operadora    → dim_operadora.sk_operadora  (N:1)
```

---

## Medidas DAX

### Página 1 — Panorama de Mercado

```dax
Total Beneficiários CE =
SUM(fato_beneficiarios[qtd_beneficiarios])

Beneficiários Unimed Fortaleza =
CALCULATE(
    SUM(fato_beneficiarios[qtd_beneficiarios]),
    dim_operadora[registro_ans] = "317144"
)

Market Share Unimed Fortaleza % =
DIVIDE([Beneficiários Unimed Fortaleza], [Total Beneficiários CE]) * 100

Operadoras Ativas CE =
DISTINCTCOUNT(fato_beneficiarios[sk_operadora])
```

### Página 2 — Saúde Financeira

```dax
Receita Assistencial =
CALCULATE(
    SUM(fato_financeiro[vl_saldo_final]),
    fato_financeiro[grupo_conta] = "RECEITA_EVENTOS"
)

Despesa Assistencial =
CALCULATE(
    SUM(fato_financeiro[vl_saldo_final]),
    fato_financeiro[grupo_conta] = "DESPESA_ASSISTENCIAL"
)

Sinistralidade % =
DIVIDE([Despesa Assistencial], [Receita Assistencial]) * 100

Resultado Assistencial =
[Receita Assistencial] - [Despesa Assistencial]

Receita Unimed Fortaleza =
CALCULATE([Receita Assistencial], dim_operadora[registro_ans] = "317144")

Despesa Unimed Fortaleza =
CALCULATE([Despesa Assistencial], dim_operadora[registro_ans] = "317144")

Sinistralidade Unimed Fortaleza % =
CALCULATE([Sinistralidade %], dim_operadora[registro_ans] = "317144")

Resultado Unimed Fortaleza =
[Receita Unimed Fortaleza] - [Despesa Unimed Fortaleza]
```

---

## Dashboard Power BI

### Página 1 — Panorama de Mercado

| Visual | Tipo | Campos |
|---|---|---|
| KPIs | 4 cartões | Total beneficiários, Beneficiários Unimed Fortaleza, Market Share %, Operadoras Ativas |
| Beneficiários por Modalidade | Gráfico de barras | modalidade × qtd_beneficiarios |
| Beneficiários por Operadora | Barras horizontais | nome_fantasia × qtd_beneficiarios (Top 10) |
| Concentração por Estado | Mapa | uf × qtd_beneficiarios |
| Filtros | Slicers | Ano, Estado, Tipo de Contratação |

### Página 2 — Saúde Financeira

| Visual | Tipo | Campos |
|---|---|---|
| KPIs | 4 cartões | Receita UF, Despesa UF, Resultado UF, Sinistralidade UF % (vermelho/verde) |
| Receita vs Despesa | Colunas agrupadas + linha | nome_fantasia × Receita/Despesa + Sinistralidade % (Top 10) |
| Top 10 Lucrativas | Barras horizontais | nome_fantasia × Resultado Assistencial |
| Top 10 Prejuízo | Barras horizontais | nome_fantasia × Resultado Assistencial (Inferior 10) |
| Filtros | Slicers | Ano, Trimestre, Modalidade |

---

## Volumes de Dados

| Tabela | Registros |
|---|---|
| dim_calendario | 2.557 datas |
| dim_operadora | 1.109 operadoras |
| dim_produto | 163.095 planos |
| fact_beneficiarios | 361.918 registros |
| fact_financeiro | 709.839 lançamentos |

**Unimed Fortaleza (ANS 317144):**
- 352.715 beneficiários ativos
- R$ 65,8 bilhões em movimentação financeira
- Sinistralidade: 121,06% (3T2025)

---

## Decisões Técnicas

Durante o desenvolvimento foram enfrentados e resolvidos alguns desafios relevantes:

- **Encoding dos CSVs da ANS** — arquivos em latin-1 não suportados diretamente pelo DuckDB; resolvido com auto-detecção de encoding via `read_csv_auto`.
- **Incompatibilidade SQLAlchemy 2.0 + pandas** — `to_sql` e `read_sql` do pandas não aceitam `Connection` do SQLAlchemy 2.0; toda a carga foi reescrita com `conn.execute(text(...), records)` nativo.
- **XCom + pandas** — o Airflow serializa dados entre tasks como JSON; pandas interpretava JSONs grandes como filepath; corrigido com `pd.read_json(io.StringIO(raw_json), orient="split")` em todas as tasks.
- **Tipo mismatch no merge** — após desserialização do XCom, `registro_ans` voltava como `int64` quebrando o JOIN com a dimensão; corrigido forçando `.astype(str).str.zfill(6)` em ambos os lados.
- **Idempotência** — dimensões usam upsert (`ON CONFLICT DO UPDATE`) e facts usam DELETE por competência/trimestre antes do INSERT, garantindo que a DAG possa rodar múltiplas vezes sem duplicar dados.

---

## Quality Checks

Executados automaticamente ao final de cada carga:

- Contagem mínima de linhas por tabela
- Presença da Unimed Fortaleza (ANS 317144) na dim_operadora
- Proporção de FKs nulas nas facts (máximo 5%)
- Sinistralidade geral do mercado dentro do range plausível (20%–200%)

---

## Links

- 📊 [Dashboard Power BI](https://app.powerbi.com/view?r=eyJrIjoiMzMzZWM0YTktNjE2Mi00OWU2LWI4ODMtZjcyMjk5OTk0YTIxIiwidCI6IjRmMGYwZjk5LTE5NGEtNDgwNi1iZGEwLTcxYTRjNDAyNmZjYiJ9&pageName=f508232f05bfccafbeac)

---

## Variáveis de Ambiente

```env
DW_HOST=unimed_dw
DW_PORT=5432
DW_DB=unimed_dw
DW_USER=dw_user
DW_PASSWORD=dw_password
RAW_DATA_PATH=./data/raw
DUCKDB_PATH=/tmp/unimed_staging.duckdb
REGISTRO_ANS_UNIMED_FORTALEZA=317144
```