# Jurimetria — Pipeline da Taxa de Congestionamento Mensal

Pipeline que lê extratos brutos de processos judiciais (CSV), limpa e trata os
dados, calcula a **Taxa de Congestionamento Mensal** por comarca/serventia, e
publica o resultado como um JSON pronto para ser consumido por um pipeline
downstream (EDA, previsão de 3/6/12 meses e dashboard).

## Sumário

- [Como rodar](#como-rodar)
- [O que o pipeline faz](#o-que-o-pipeline-faz)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Configuração](#configuração-configsettingsyaml)
- [Argumentos de linha de comando](#argumentos-de-linha-de-comando)
- [A métrica](#a-métrica)
- [Incrementalidade e imutabilidade histórica](#incrementalidade-e-imutabilidade-histórica)
- [Arquivamento automático dos CSVs](#arquivamento-automático-dos-csvs)
- [Qualidade de dados](#qualidade-de-dados)
- [O payload JSON](#o-payload-json)
- [Solução de problemas](#solução-de-problemas)

---

## Como rodar

```bash
pip install -r requirements.txt
```

Coloque o(s) CSV(s) brutos em `data/01_raw/` (o nome deve casar com
`raw_glob_pattern` do `settings.yaml` — hoje `analise_txcg_*.csv`), depois:

```bash
python -m src.main
```

Isso é tudo. O pipeline detecta sozinho quais competências (meses) precisa
calcular — não é preciso informar datas manualmente no dia a dia. Ele:

1. lê os CSVs novos de `data/01_raw/`;
2. atualiza a tabela `trusted` (upsert por processo);
3. descobre automaticamente o intervalo de competências que falta processar;
4. calcula a taxa de congestionamento para esse intervalo;
5. gera e valida o payload JSON;
6. arquiva os CSVs já processados, para a próxima execução não relê-los.

## O que o pipeline faz

```
data/01_raw/*.csv                     (bruto — CSVs chegam aqui)
      │  ingest_and_upsert_trusted()
      ▼
data/02_trusted/core_processos.parquet   (1 linha por processo, upsert aplicado)
      │  calcular_competencias()
      ▼
data/03_refined/serie_tx_cong.parquet    (série histórica completa, por competência)
      │  build_payload()
      ▼
data/03_refined/payload_tx_mensal.json   (entregável para o pipeline downstream)
```

## Estrutura do projeto

```
jurimetria-pipeline/
├── config/
│   └── settings.yaml            # regras de negócio (fora do código)
├── contracts/
│   └── payload.schema.json      # contrato do JSON, compartilhado com o downstream
├── data/                        # zonas de dados (git-ignoradas)
│   ├── 01_raw/                  #   CSVs brutos chegam aqui
│   │   └── processados/         #   CSVs já incorporados à trusted (arquivo automático)
│   ├── 02_trusted/              #   core_processos.parquet
│   └── 03_refined/              #   serie_tx_cong.parquet + payload_tx_mensal.json
├── src/
│   ├── main.py                  # ponto de entrada / CLI
│   ├── pipeline.py              # limpeza, upsert, cálculo, payload
│   ├── config.py                # carga do settings.yaml
│   └── payload_validator.py     # valida o JSON contra o contrato antes de publicar
└── requirements.txt
```

## Configuração (`config/settings.yaml`)

Todas as regras de negócio ficam fora do código. As mais relevantes no dia a dia:

| Chave | Para quê |
|---|---|
| `raw_glob_pattern` | Quais arquivos em `data/01_raw/` são lidos (ex.: `"analise_txcg_*.csv"`) |
| `keys` | Colunas de agrupamento da taxa (padrão: `comarca`, `serventia`) |
| `id_columns` | IDs estáveis (ex.: `comarca_id`), se existirem no bruto — recomendado para robustez a acento/renomeação |
| `rolling_window_months` | Janela do denominador (padrão CNJ: 12) |
| `desde_fixo` | Piso do backfill inicial (ex.: `"2020-01"`) — ver [Incrementalidade](#incrementalidade-e-imutabilidade-histórica) |
| `ano_minimo_plausivel` / `ano_maximo_plausivel` | Faixa de sanidade para datas — protege a auto-detecção contra datas corrompidas no extrato |
| `auto_archive` / `processed_subdir` | Liga/desliga e destino do arquivamento automático de CSVs |
| `validate_payload` | Se o payload é validado contra `contracts/payload.schema.json` antes de publicar |

Qualquer chave omitida usa o default definido em `src/config.py`.

## Argumentos de linha de comando

```bash
python -m src.main                              # AUTO (recomendado — ver acima)
python -m src.main --desde 2024-01               # fixa o início; fim continua automático
python -m src.main --desde 2020-01 --ate 2025-12 # intervalo fixo dos dois lados
python -m src.main --competencias 2026-01 2026-03  # lista esparsa, para recompute pontual
python -m src.main --no-archive                 # não move os CSVs processados
python -m src.main --raw-dir outra/pasta         # sobrescreve a zona RAW
python -m src.main --config outro/settings.yaml  # usa outro arquivo de config
```

## A métrica

```
estoque_pendente[mês]        = processos distribuídos até o fim do mês
                                E (sem baixa OU baixa em mês futuro)

baixados_últimos_12m[mês]    = processos baixados na janela [mês-11, mês]

taxa_congestionamento_pct    = estoque_pendente / (estoque_pendente + baixados_últimos_12m) × 100
```

Cada competência é calculada de forma **absoluta** contra a trusted inteira —
não é um acumulado (`cumsum`) encadeado mês a mês. Isso tem duas
consequências importantes:

- **Correção independe de quais meses são publicados.** Calcular só a partir
  de `2020-01` dá exatamente o mesmo resultado que calcular desde o início da
  base e descartar os meses anteriores — o estoque de 2020-01 já é computado
  olhando toda a distribuição histórica anterior, esteja ela "publicada" ou não.
- **Recalcular um mês é idempotente.** Rodar a mesma competência duas vezes
  produz o mesmo resultado, byte a byte.

## Incrementalidade e imutabilidade histórica

A cada execução, o pipeline detecta sozinho o intervalo que falta processar:

- **`desde`** = competência seguinte à última já calculada (ou, na
  primeiríssima execução, o mês da distribuição mais antiga plausível nos
  dados — nunca antes de `desde_fixo`, se configurado).
- **`até`** = o mês mais recente com atividade real (distribuição **ou**
  baixa) nos dados, nunca ultrapassando o mês corrente.

Só as competências dentro desse intervalo são recalculadas; tudo que já foi
calculado antes permanece intacto em `serie_tx_cong.parquet` — uma baixa nova
em 2026 nunca altera o retrato de 2024, por exemplo, porque o estoque de um
mês passado nunca conta baixas de meses futuros a ele.

## Arquivamento automático dos CSVs

Ao final de uma execução bem-sucedida, os CSVs de `data/01_raw/` que acabaram
de ser incorporados à trusted são movidos para
`data/01_raw/processados/<timestamp>/`. Isso é seguro porque a trusted já
persiste tudo cumulativamente — os CSVs arquivados não são mais necessários
para nenhum cálculo futuro, incluindo a janela de 12 meses. Uma falha no meio
do pipeline deixa os CSVs no lugar, prontos para nova tentativa.

## Qualidade de dados

O pipeline nunca apaga ou "corrige" dados brutos silenciosamente. Datas fora
da faixa plausível (`ano_minimo_plausivel`/`ano_maximo_plausivel`) são
ignoradas **apenas** na heurística de auto-detecção do intervalo, com um
aviso no log — a linha original permanece intacta na trusted.

Para investigar diretamente:

```python
import pandas as pd
df = pd.read_parquet("data/02_trusted/core_processos.parquet")

# Datas implausíveis
for col in ["data_distribuicao", "data_baixa"]:
    s = df[df[col].notna() & ((df[col].dt.year < 1990) | (df[col].dt.year > 2100))]
    print(col, len(s))

# Baixa antes da distribuição (logicamente impossível)
print((df.data_baixa.notna() & (df.data_baixa < df.data_distribuicao)).sum())
```

## O payload JSON

Exemplo real (`data/03_refined/payload_tx_mensal.json`):

```json
{
  "schema_version": "1.0.0",
  "metadata": {
    "gerado_em": "2026-07-02T18:36:44Z",
    "metrica": "taxa_congestionamento_mensal",
    "janela_baixados_meses": 12,
    "grao": ["comarca", "serventia", "competencia"],
    "competencia_min": "2020-01",
    "competencia_max": "2025-12",
    "record_count": 72,
    "checksum_sha256": "994058153cb556b6fa7c96fa7513ae9ec5efbdce59db700ff813e01d9ea62132"
  },
  "data": [
    {
      "competencia": "2020-01",
      "comarca": "GOIÂNIA",
      "serventia": "1ª Vara Cível",
      "processos_distribuidos": 10,
      "processos_baixados": 9,
      "estoque_pendente": 662,
      "baixados_ultimos_12_meses": 68,
      "taxa_congestionamento_pct": 90.68
    }
  ]
}
```

`data` traz a **série histórica completa** (não só as competências
recém-calculadas) — o pipeline downstream de previsão precisa do histórico
inteiro para projetar 3/6/12 meses. O contrato completo está em
`contracts/payload.schema.json` e é validado antes de cada publicação.

## Solução de problemas

**`ValueError: Nenhum CSV encontrado ... e ainda não existe trusted persistida`**
Primeira execução sem nenhum CSV em `data/01_raw/` que bata com
`raw_glob_pattern`. Confira o nome do arquivo e o padrão no `settings.yaml`.

**Auto-detecção gerou um intervalo enorme (milhares de meses)**
Sinal de uma data corrompida no CSV bruto (ex.: ano digitado errado). Rode o
diagnóstico da seção [Qualidade de dados](#qualidade-de-dados); a linha
continua intacta na trusted, só precisa ser investigada na origem. Ajustar
`ano_minimo_plausivel`/`ano_maximo_plausivel` evita que isso trave a
auto-detecção novamente.

**Quero recalcular um mês específico já processado**
```bash
python -m src.main --competencias 2025-06
```

**`Falha na validação de contrato — payload NÃO publicado`**
O payload não foi gravado; o JSON anterior (se houver) permanece válido. Veja
a mensagem de erro para o campo específico que falhou contra
`contracts/payload.schema.json`.