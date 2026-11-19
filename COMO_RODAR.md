# Como rodar do zero (Windows) — CidadeViva · Big Data Architecture & Data Integration

Você precisa de: **Python 3.10+** e **Git Bash** (já tem). Só a parte do Airflow exige **Docker Desktop**.

## 1) Preparar a pasta
1. Clone o repositório.
2. Coloque os 3 CSVs do SP156 (que você baixou do portal) em **`data/raw/`**. Eles são o *fallback*: no Airflow o pipeline tenta
   primeiro a API do portal (`SP156_FONTE=auto`) e só usa estes arquivos se a API falhar; no `run_local.py` abaixo, `SP156_LOCAL_PATH` os usa direto:
   `arquivo-final-1-trim-26.csv`, `arquivo-final-2-trim-26.csv`, `arquivo-final-3-trim-26.csv`.
3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

## 2) Rodar o pipeline com os dados reais
Teste rápido (~15 s, lê só 50 mil linhas de cada arquivo):
```bash
export SP156_LOCAL_PATH="data/raw/arquivo-final-*-trim-26.csv"
export SP156_MAX_ROWS=50000
python scripts/run_local.py --ds 2026-11-12
```
Execução completa (~1 min):
```bash
unset SP156_MAX_ROWS
python scripts/run_local.py --ds 2026-11-12
```
**Deu certo se** o final do log mostrar `SP156 silver: {'linhas_lidas': 1525907, ... 'linhas_silver': 472524 ...}`, as **19 linhas `DQ ... OK`**
e `contagens no DW: {... 'fato_solicitacao_sp156': 472524 ...}`.
(PowerShell: `$env:SP156_LOCAL_PATH="data/raw/arquivo-final-*-trim-26.csv"` e `Remove-Item Env:SP156_MAX_ROWS`.)

## 3) Ver os dados e testar
- Abra `data/cidadeviva_dw.sqlite` no **DBeaver** (SQLite) ou rode: `sqlite3 data/cidadeviva_dw.sqlite < sql/consultas_exemplo.sql`
- `pytest -q tests → 13 testes passando.
- Estatísticas da classificação: `data/silver/sp156/dt=2026-11-12/_stats.json` (também em `docs/`).

## 4) PDF da entrega
O PDF **já vem pronto** em `docs/CidadeViva_Arquitetura_e_Dicionario_de_Dados.pdf`, gerado com os dados reais.
Para regerar depois de uma nova execução: `python docs/gerar_pdf.py`.

## 5) Airflow (para os prints/vídeo que o enunciado pede)
Precisa do **Docker Desktop** rodando.
```bash
cp .env.example .env
docker compose up airflow-init
docker compose up -d
```
1. Abra http://localhost:8080 (usuário `admin`, senha `admin`) → DAG **cidadeviva_pipeline** → botão ▶ *Trigger DAG*.
2. Tire print do **Graph** com as 10 tasks verdes, de um **log** de task (ex.: `validar_qualidade`) e das **tabelas no banco**
   (DBeaver → PostgreSQL, host `localhost`, porta `5433`, banco `cidadeviva_dw`, usuário `dw`, senha `dw`).


## 6) Entrega (prazo 19/11, 23h55)
1. Suba a pasta para um repositório **público** no GitHub (os CSVs em `data/raw/` já são ignorados pelo `.gitignore`).
2. Portal da disciplina (um integrante envia): **link do repositório + PDF único** + prints/vídeo do Airflow.
3. Confira que o repositório continua público durante a correção.

## Problemas comuns
| Sintoma | O que fazer |
|---|---|
| `não parece o CSV do SP156 ... HTML` | O arquivo baixado é uma página, não o CSV. Baixe de novo pelo navegador. |
| `arquivo(s) não encontrado(s)` | Confira se está na pasta certa (`pwd`) e se os CSVs estão em `data/raw/`. |
| `Formato de data ambíguo` (aviso) | Force: `export SP156_DATE_FORMAT="%d/%m/%Y"` (tri 1–2) ou `"%m/%d/%Y"` (tri 3). |
| `ModuleNotFoundError` | `pip install -r requirements.txt` |
| Qualquer outro erro | Copie o **texto completo** do terminal. |