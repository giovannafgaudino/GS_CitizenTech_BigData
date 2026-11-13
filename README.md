# CidadeViva — Pipeline de Dados (Big Data Architecture & Data Integration)

Global Solution FIAP 2026.2 · CitizenTech · Engenharia de Software 4º ano
Disciplina: Big Data Architecture & Data Integration · Atividade: Pipeline de Dados do MVP CitizenTech

**Integrantes**

| Nome | RM |
|---|---|
| Giovanna Franco Gaudino Rodrigues | RM553701 |
| Rafael de Almeida Sigoli | RM554019 |
| Rafael Jorge Del Padre | RM552765 |

Pipeline orquestrado (Apache Airflow) que coleta, trata e entrega os dados que alimentam o **CidadeViva**,
plataforma colaborativa de zeladoria urbana. Todo o stack é gratuito/open-source (Python, pandas, Airflow, PostgreSQL).

## O que o pipeline faz

| Fonte | Origem | Uso no MVP |
|---|---|---|
| **SP156** | Portal de Dados Abertos da Prefeitura de SP, obtido pela **API do portal (CKAN)** com os CSVs locais como alternativa (CSV trimestral, licença CC0) | Histórico real de demanda por subprefeitura/categoria → mapa de calor, priorização, tempo de resposta esperado |
| **Reportes CidadeViva** | API de exportação do app (ou simulador, ver abaixo) | Triagem por IA (categoria + confiança), fila de revisão humana, envio pelo RPA |
| **Referências** | `seeds/` (subprefeituras, categorias, regras de classificação) | Dimensões e taxonomia única entre as duas fontes |

```
ingerir_sp156 ───────► silver_sp156 ───────┐
ingerir_cidadeviva ──► silver_cidadeviva ──┼─► construir_gold ─► validar_qualidade ─► carregar_dw
ingerir_referencias ─► (usadas pelas duas) ┘
```

Arquitetura medallion: **bronze** (bruto, imutável, com manifest e hash) → **silver** (limpo e tipado) → **gold**
(modelo dimensional) → **Data Warehouse PostgreSQL**. Detalhes e dicionário de dados no PDF da entrega.

## Como executar

### A) Com Airflow (Docker) — o caminho da entrega

```bash
cp .env.example .env
docker compose up airflow-init
docker compose up -d
# Airflow:  http://localhost:8080   (admin / admin)  -> DAG "cidadeviva_pipeline" -> Trigger
# Warehouse: psql -h localhost -p 5433 -U dw -d cidadeviva_dw   (senha: dw)
```

O padrão é `SP156_MODE=real` com `SP156_FONTE=auto`: a task `ingerir_sp156` consulta a **API do portal de dados abertos (CKAN,
`package_show`)**, descobre os CSVs trimestrais do ano da execução ("Dados do SP156 - n° TRI <ano>"), baixa e valida cada um
(descarta se vier HTML) e os guarda em cache. **Se a API ou o download falharem**, usa como *fallback* os CSVs de `SP156_LOCAL_PATH`.
A fonte realmente usada (`api` ou `local`) e o erro da API, se houve, ficam no `_manifest.json` e no log da task.
`SP156_FONTE=api` exige a API (sem fallback); `SP156_FONTE=local` usa só os CSVs locais; `SP156_ANO` força o ano.
Após o *Trigger*, as 10 tasks devem ficar verdes e as tabelas aparecem no schema `public` do banco `cidadeviva_dw`.

### B) Sem Airflow (rápido, usa SQLite) — com os dados reais

Por padrão (`SP156_FONTE=auto`) o `run_local.py` também tenta primeiro a API do portal e, se ela falhar, usa os CSVs de
`SP156_LOCAL_PATH`. Para garantir o *fallback*, baixe os CSVs **pelo navegador** em
<https://dados.prefeitura.sp.gov.br/dataset/dados-do-sp156> e coloque em `data/raw/` (o portal pode bloquear download por
script). Os arquivos usados na entrega foram `arquivo-final-1/2/3-trim-26.csv`. Para usar só os arquivos locais:
`export SP156_FONTE=local`.

```bash
pip install -r requirements.txt
export SP156_LOCAL_PATH="data/raw/arquivo-final-*-trim-26.csv"      # aceita glob, vários arquivos ou vírgulas
python scripts/run_local.py --ds 2026-11-12                          # ~1 min, ~500 MB de RAM
sqlite3 data/cidadeviva_dw.sqlite < sql/consultas_exemplo.sql        # ou abra o .sqlite no DBeaver
python docs/gerar_pdf.py                                             # regera o PDF (ABNT) com os números da SUA execução
python docs/gerar_docx.py                                            # idem em Word (.docx)
pytest -q                                                            # 13 testes (usam amostra sintética/fixtures de propósito)
```

Teste rápido: `export SP156_MAX_ROWS=50000` (limita as linhas lidas de cada arquivo).
Sem os CSVs (ex.: avaliador): `SP156_MODE=sample python scripts/run_local.py --ds 2026-11-12` usa a amostra **sintética**.

## Evidências de execução

Prints da execução real no Airflow (Docker) com os 3 arquivos oficiais do SP156 de 2026, em [`docs/evidencias/`](docs/evidencias/):

| # | Arquivo | O que mostra |
|---|---|---|
| 1 | `01_AirFlow_dag_graph.png` | Graph da DAG `cidadeviva_pipeline` com as 10 tasks em `success`. |
| 2 | `02_AirFlow_gantt.png` | Gantt: duração de cada task e o paralelismo das 3 ingestões. |
| 3 | `03_ingerir_sp156.png` | Log da ingestão (bronze): os 3 CSVs lidos, tamanhos e manifest com hash. |
| 4 | `04_silver_sp156.png` | Log do silver: linhas lidas, fora de escopo, classificadas por categoria e sem subprefeitura. |
| 5 | `05_validar_qualidade.png` | Log do gate de qualidade: os 19 checks em `OK` (PK/FK, completude, domínio, coordenadas, LGPD). |
| 6 | `06_carregar_dw.png` | Log da carga no Data Warehouse com a contagem de linhas por tabela (`fato_solicitacao_sp156` = 472.524). |
| 7 | `07_contagem_solicitacao.png` | Contagem de linhas em `fato_solicitacao_sp156` no DW (472.524, sem duplicidade) |
| 8 | `08_idempotencia_reexecucao.png` | Mesma contagem após Clear/reexecução da DAG: continua 472.524 |

![Graph da DAG](docs/evidencias/01_AirFlow_dag_graph.png)

Os mesmos números aparecem no PDF [`docs/CidadeViva_Arquitetura_e_Dicionario_de_Dados.pdf`](docs/CidadeViva_Arquitetura_e_Dicionario_de_Dados.pdf)
(arquitetura, modelo dimensional e dicionário de dados).

## Decisões de projeto

* **Idempotência** — toda task recebe só a data lógica (`ds`); bronze/silver/gold gravam em `dt=<ds>`. Na carga, o SP156
  (um retrato completo do ano, não um lote do dia) **substitui** `fato_solicitacao_sp156` e o mart de demanda; os reportes do app
  (lote diário) removem só a partição `dt_ingestao=<ds>`. Assim, rodar a DAG de novo, na mesma data ou em outra, não duplica
  o histórico (coberto por testes).
* **Dados não trafegam por XCom** — o contrato entre tasks é o data lake em disco (Parquet quando há `pyarrow`).
* **Qualidade como gate** — `validar_qualidade` (19 verificações: PK/FK, completude, domínio, coordenadas, LGPD) falha a
  DAG antes da carga se algo estiver errado. Relatório em `data/gold/dt=<ds>/_dq_report.json`.
* **LGPD** — nome/e-mail do cidadão nunca chegam à silver: o usuário vira `usuario_hash` (SHA-256 + salt) e telefone/CPF/e-mail
  digitados na descrição são mascarados.
* **IA responsável** — reportes com confiança < 0,70 (ou sem categoria válida) recebem `requer_revisao_humana = 1`;
  o `mart_triagem_ia` acompanha a taxa por categoria.
* **Tratamento do SP156** — só entram solicitações de zeladoria (7 categorias, regras em `seeds/regras_categoria_sp156.csv`);
  o resto (IPTU, cadastro, ouvidoria, animais...) é descartado e contabilizado em `_stats.json`, que também lista os
  serviços **não classificados** (`top_nao_classificados`) e as subprefeituras **não reconhecidas** para você ajustar as regras.
* **Layouts do SP156** — o portal mudou o formato em 2026 (colunas *Prefeitura Operacional*, *Data de Finalização*,
  *Meio de Comunicação*; separador `;` nos tri 1–2 e `,` no tri 3; datas `dd/mm/aaaa` e, no tri 3, `mm/dd/aaaa`; só data, sem hora).
  O pipeline aceita os dois layouts e detecta separador, codificação e formato de data **por arquivo**. Se a detecção de data
  for ambígua, force com `SP156_DATE_FORMAT`.
* **Sem deduplicação por conteúdo** — o CSV não tem protocolo, então cada linha é uma solicitação (`id = arquivo:linha`);
  só arquivos idênticos (mesmo hash) são ignorados.

## O que é real e o que é simulado (transparência)

* **SP156: dados reais** (Portal de Dados Abertos da PMSP, CC0) — é o comportamento padrão. A amostra sintética
  `data/sample/sp156_amostra.csv` existe **somente para os testes automatizados** (`SP156_MODE=sample`).
* **Reportes do CidadeViva: simulados** enquanto `CIDADEVIVA_API_URL` não estiver definido, porque o app ainda não tem
  usuários reais. O simulador injeta sujeira de propósito (duplicados, lat/lon invertidos, PII no texto). Com a API
  (ou um JSON exportado do app no mesmo contrato, ver `ingest._simular_reportes`), nada mais muda.
* Os centroides em `seeds/subprefeituras.csv` são **aproximados** (didáticos), não limites oficiais do GeoSampa.
* As regras de classificação do SP156 (`seeds/regras_categoria_sp156.csv`) são lexicais e foram escritas sem acesso ao
  catálogo completo de serviços: confira `_stats.json` e a contagem por categoria após a primeira execução real.

## Estrutura

```
dags/cidadeviva_pipeline.py   DAG do Airflow
pipeline/                     ingest · transform · gold · quality · load · config · utils
seeds/                        subprefeituras, categorias, regras de classificação
data/sample/                  amostra offline do SP156
scripts/run_local.py          execução sem Airflow
sql/consultas_exemplo.sql     consultas que o MVP executa
tests/                        pytest (utils + pipeline ponta a ponta + idempotência)
docs/                         diagrama, log de execução, gerador do PDF
docker-compose.yml            Airflow + Postgres (metadados) + Postgres (DW)
```