-- ══════════════════════════════════════════════════════════════════
-- init.sql — Criação do schema e star schema no Data Warehouse
-- Executado automaticamente pelo postgres na primeira inicialização
-- ══════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS ans;

-- ──────────────────────────────────────────────────────────────────
-- DIM — Calendário
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ans.dim_calendario (
    sk_data         DATE        PRIMARY KEY,
    ano             SMALLINT    NOT NULL,
    semestre        SMALLINT    NOT NULL,
    trimestre       SMALLINT    NOT NULL,
    mes             SMALLINT    NOT NULL,
    nome_mes        VARCHAR(20) NOT NULL,
    semana_ano      SMALLINT    NOT NULL,
    dia             SMALLINT    NOT NULL,
    dia_semana      VARCHAR(15) NOT NULL,
    is_fim_semana   BOOLEAN     NOT NULL DEFAULT false
);

-- ──────────────────────────────────────────────────────────────────
-- DIM — Operadora
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ans.dim_operadora (
    sk_operadora    SERIAL      PRIMARY KEY,
    registro_ans    VARCHAR(10) NOT NULL UNIQUE,
    nome_fantasia   VARCHAR(200),
    razao_social    VARCHAR(200),
    modalidade      VARCHAR(100),   -- Cooperativa Médica, Medicina de Grupo, Seguradora…
    uf              CHAR(2),
    municipio       VARCHAR(120),
    cnpj            VARCHAR(18),
    situacao        VARCHAR(80),
    data_registro   DATE,
    dt_carga        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ──────────────────────────────────────────────────────────────────
-- DIM — Produto (Plano de Saúde)
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ans.dim_produto (
    sk_produto          SERIAL      PRIMARY KEY,
    registro_ans        VARCHAR(10) NOT NULL,
    cd_produto          VARCHAR(30) NOT NULL,
    nome_plano          VARCHAR(300),
    segmentacao         VARCHAR(100),   -- Ambulatorial, Hospitalar, Ref, Odonto…
    tipo_contratacao    VARCHAR(80),    -- Individual, Coletivo Empresarial, Col. Adesão
    abrangencia         VARCHAR(80),    -- Municipal, Estadual, Grupo, Nacional
    cobertura_acidente  VARCHAR(20),
    coparticipacao      VARCHAR(5),     -- S / N
    dt_registro_produto DATE,
    situacao_produto    VARCHAR(60),
    dt_carga            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (registro_ans, cd_produto)
);

-- ──────────────────────────────────────────────────────────────────
-- FACT — Beneficiários
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ans.fact_beneficiarios (
    sk_beneficiarios    BIGSERIAL   PRIMARY KEY,
    sk_data             DATE        NOT NULL REFERENCES ans.dim_calendario(sk_data),
    sk_operadora        INTEGER     NOT NULL REFERENCES ans.dim_operadora(sk_operadora),
    sk_produto          INTEGER              REFERENCES ans.dim_produto(sk_produto),
    uf                  CHAR(2),
    municipio           VARCHAR(120),
    faixa_etaria        VARCHAR(30),
    sexo                CHAR(1),
    tipo_contratacao    VARCHAR(80),
    qtd_beneficiarios   INTEGER     NOT NULL DEFAULT 0,
    dt_carga            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ──────────────────────────────────────────────────────────────────
-- FACT — Financeiro (DRE/Balancete DIOPS)
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ans.fact_financeiro (
    sk_financeiro       BIGSERIAL   PRIMARY KEY,
    sk_data             DATE        NOT NULL REFERENCES ans.dim_calendario(sk_data),
    sk_operadora        INTEGER     NOT NULL REFERENCES ans.dim_operadora(sk_operadora),
    cd_conta_contabil   VARCHAR(30),
    descricao_conta     VARCHAR(300),
    grupo_conta         VARCHAR(100),   -- RECEITA, DESPESA_ASSISTENCIAL, DESPESA_ADM…
    vl_saldo_inicial    NUMERIC(18,2)   NOT NULL DEFAULT 0,
    vl_saldo_final      NUMERIC(18,2)   NOT NULL DEFAULT 0,
    trimestre_ref       CHAR(6),        -- ex: 3T2025
    dt_carga            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ──────────────────────────────────────────────────────────────────
-- Índices de performance
-- ──────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ben_data       ON ans.fact_beneficiarios(sk_data);
CREATE INDEX IF NOT EXISTS idx_ben_operadora  ON ans.fact_beneficiarios(sk_operadora);
CREATE INDEX IF NOT EXISTS idx_ben_produto    ON ans.fact_beneficiarios(sk_produto);
CREATE INDEX IF NOT EXISTS idx_ben_uf         ON ans.fact_beneficiarios(uf);

CREATE INDEX IF NOT EXISTS idx_fin_data       ON ans.fact_financeiro(sk_data);
CREATE INDEX IF NOT EXISTS idx_fin_operadora  ON ans.fact_financeiro(sk_operadora);
CREATE INDEX IF NOT EXISTS idx_fin_grupo      ON ans.fact_financeiro(grupo_conta);

-- ──────────────────────────────────────────────────────────────────
-- Views analíticas prontas para Power BI
-- ──────────────────────────────────────────────────────────────────

-- Sinistralidade por operadora / período
CREATE OR REPLACE VIEW ans.vw_sinistralidade AS
SELECT
    o.registro_ans,
    o.nome_fantasia,
    o.modalidade,
    o.uf                                                AS uf_operadora,
    c.ano,
    c.trimestre,
    f.trimestre_ref,
    SUM(CASE WHEN f.grupo_conta = 'RECEITA_EVENTOS'      THEN f.vl_saldo_final ELSE 0 END) AS receita_eventos,
    SUM(CASE WHEN f.grupo_conta = 'DESPESA_ASSISTENCIAL' THEN f.vl_saldo_final ELSE 0 END) AS despesa_assistencial,
    CASE
        WHEN SUM(CASE WHEN f.grupo_conta = 'RECEITA_EVENTOS' THEN f.vl_saldo_final ELSE 0 END) = 0 THEN NULL
        ELSE ROUND(
            SUM(CASE WHEN f.grupo_conta = 'DESPESA_ASSISTENCIAL' THEN f.vl_saldo_final ELSE 0 END) /
            NULLIF(SUM(CASE WHEN f.grupo_conta = 'RECEITA_EVENTOS' THEN f.vl_saldo_final ELSE 0 END), 0) * 100,
        2)
    END                                                 AS sinistralidade_pct
FROM ans.fact_financeiro f
JOIN ans.dim_operadora  o ON o.sk_operadora = f.sk_operadora
JOIN ans.dim_calendario c ON c.sk_data      = f.sk_data
GROUP BY 1,2,3,4,5,6,7;

-- Market Share de beneficiários por modalidade
CREATE OR REPLACE VIEW ans.vw_market_share AS
SELECT
    c.ano,
    c.mes,
    b.uf,
    o.modalidade,
    o.nome_fantasia,
    o.registro_ans,
    SUM(b.qtd_beneficiarios)                            AS total_vidas,
    SUM(SUM(b.qtd_beneficiarios)) OVER (
        PARTITION BY c.ano, c.mes, b.uf
    )                                                   AS total_vidas_mercado,
    ROUND(
        SUM(b.qtd_beneficiarios) * 100.0 /
        NULLIF(SUM(SUM(b.qtd_beneficiarios)) OVER (PARTITION BY c.ano, c.mes, b.uf), 0),
    2)                                                  AS market_share_pct
FROM ans.fact_beneficiarios b
JOIN ans.dim_operadora  o ON o.sk_operadora = b.sk_operadora
JOIN ans.dim_calendario c ON c.sk_data      = b.sk_data
GROUP BY 1,2,3,4,5,6;
