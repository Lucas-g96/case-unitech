# Power BI — Guia de Conexão e Modelagem

## 1. Conexão com o PostgreSQL

### Power BI Desktop
1. **Obter Dados** → **Banco de Dados** → **PostgreSQL**
2. Preencha:
   - Servidor: `localhost:5432` (ou IP do servidor em produção)
   - Banco de dados: `unimed_dw`
3. Modo de conectividade: **Importar** (recomendado para performance) ou **DirectQuery**
4. Credenciais: usuário `dw_user` / senha conforme `.env`
5. Selecione as tabelas do schema `ans`:
   - `dim_calendario`
   - `dim_operadora`
   - `dim_produto`
   - `fact_beneficiarios`
   - `fact_financeiro`
   - `vw_sinistralidade` *(view)*
   - `vw_market_share` *(view)*

---

## 2. Modelagem no Power BI

As relações abaixo devem ser configuradas em **Exibição de Modelo**:

| Tabela Fato | Campo FK | Tabela Dim | Campo PK | Cardinalidade |
|---|---|---|---|---|
| fact_beneficiarios | sk_data | dim_calendario | sk_data | N:1 |
| fact_beneficiarios | sk_operadora | dim_operadora | sk_operadora | N:1 |
| fact_beneficiarios | sk_produto | dim_produto | sk_produto | N:1 |
| fact_financeiro | sk_data | dim_calendario | sk_data | N:1 |
| fact_financeiro | sk_operadora | dim_operadora | sk_operadora | N:1 |

> As views `vw_sinistralidade` e `vw_market_share` já têm os joins prontos — use-as diretamente nos visuais mais pesados.

---

## 3. Medidas DAX essenciais

Cole estas medidas em uma tabela `_Medidas` dedicada:

```dax
// ── Beneficiários ─────────────────────────────────────────────

Total Vidas = 
SUM(fact_beneficiarios[qtd_beneficiarios])

Total Vidas Unimed Fortaleza = 
CALCULATE(
    [Total Vidas],
    dim_operadora[registro_ans] = "359017"
)

Market Share Unimed (%) = 
DIVIDE(
    [Total Vidas Unimed Fortaleza],
    [Total Vidas],
    0
) * 100


// ── Financeiro ────────────────────────────────────────────────

Receita Eventos = 
CALCULATE(
    SUM(fact_financeiro[vl_saldo_final]),
    fact_financeiro[grupo_conta] = "RECEITA_EVENTOS"
)

Despesa Assistencial = 
CALCULATE(
    SUM(fact_financeiro[vl_saldo_final]),
    fact_financeiro[grupo_conta] = "DESPESA_ASSISTENCIAL"
)

Sinistralidade (%) = 
DIVIDE(
    [Despesa Assistencial],
    [Receita Eventos],
    BLANK()
) * 100

Sinistralidade Status = 
IF(
    [Sinistralidade (%)] > 90,
    "CRÍTICO",
    IF([Sinistralidade (%)] > 75, "ATENÇÃO", "OK")
)

Cor Sinistralidade = 
SWITCH(
    [Sinistralidade Status],
    "CRÍTICO",  "#E74C3C",   -- vermelho
    "ATENÇÃO",  "#F39C12",   -- amarelo
    "OK",       "#27AE60"    -- verde
)
```

---

## 4. Página 1 — Panorama de Mercado

| Visual | Tipo | Campos |
|--------|------|--------|
| Mapa de calor por UF | Mapa preenchido | `dim_operadora[uf]` + `[Total Vidas]` |
| Market Share por Modalidade | Gráfico de rosca | `dim_operadora[modalidade]` + `[Total Vidas]` |
| Unimed vs demais Unimeds | Gráfico de barras agrupadas | `dim_operadora[nome_fantasia]` filtrado por modalidade = "Cooperativa Médica" |
| Evolução mensal de vidas | Gráfico de linhas | `dim_calendario[ano]`, `dim_calendario[mes]` + `[Total Vidas]` |
| Filtros (slicers) | Slicer | `dim_calendario[ano]`, `dim_operadora[uf]`, `dim_produto[tipo_contratacao]` |

---

## 5. Página 2 — Saúde Financeira

| Visual | Tipo | Campos |
|--------|------|--------|
| Receita vs Despesa Assistencial | Gráfico de barras agrupadas | `[Receita Eventos]` + `[Despesa Assistencial]` por operadora |
| Top 10 mais lucrativas | Tabela ranqueada | `dim_operadora[nome_fantasia]` + `[Receita Eventos]` - `[Despesa Assistencial]` |
| Operadoras que mais perderam vidas | Gráfico de barras | variação de `[Total Vidas]` entre períodos |
| Card sinistralidade Unimed | Cartão com cor condicional | `[Sinistralidade (%)]` + `[Cor Sinistralidade]` |
| Gauge sinistralidade | Medidor | `[Sinistralidade (%)]` com mínimo 0, máximo 150, alvo 90 |

### Card com cor dinâmica (vermelho/verde)
1. Insira um **Cartão** com `[Sinistralidade (%)]`
2. Em **Formatação condicional da cor da fonte**, selecione **Valor do campo**
3. Aponte para `[Cor Sinistralidade]`

---

## 6. Publicar no Power BI Online

```
Power BI Desktop
  → Arquivo → Publicar → Power BI Online
  → Selecione o Workspace desejado
  → Confirme
```

Após publicar:
- Configure **Atualização agendada** (Settings → Scheduled refresh)
- Use **Gateway de dados local** se o PostgreSQL estiver on-premise
- Ou configure o **PostgreSQL como datasource** no Power BI Service com credenciais

---

## 7. Dica de performance

Para datasets grandes, prefira **Importar** ao invés de DirectQuery e use as **views** (`vw_sinistralidade`, `vw_market_share`) como fonte — elas já fazem os agregados pesados no PostgreSQL antes de chegar ao Power BI.
