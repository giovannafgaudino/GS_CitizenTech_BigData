"""Configuração central do pipeline CidadeViva.

Tudo é lido de variáveis de ambiente para que o mesmo código rode:
  * dentro do Airflow (docker-compose), gravando no Postgres do Data Warehouse;
  * fora do Airflow (scripts/run_local.py), gravando num SQLite local.
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.getenv("PIPELINE_ROOT", Path(__file__).resolve().parents[1]))

# Data lake local (camadas medallion)
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT / "data"))
BRONZE = DATA_DIR / "bronze"
SILVER = DATA_DIR / "silver"
GOLD = DATA_DIR / "gold"
SEEDS = ROOT / "seeds"
SAMPLE_DIR = ROOT / "data" / "sample"

# Data Warehouse. Ex.: postgresql+psycopg2://dw:dw@warehouse:5432/cidadeviva_dw
# Sem variável -> SQLite local (modo demonstração / testes).
WAREHOUSE_URL = os.getenv("WAREHOUSE_URL", f"sqlite:///{DATA_DIR / 'cidadeviva_dw.sqlite'}")

# Fonte 1 — SP156 (Portal de Dados Abertos da Prefeitura de SP, licença CC0)
#   real   -> baixa o CSV oficial (≈116 MB por trimestre)
#   sample -> usa a amostra em data/sample (para rodar offline / avaliação rápida)
SP156_MODE = os.getenv("SP156_MODE", "real")
SP156_URL = os.getenv(
    "SP156_URL",
    "https://dados.prefeitura.sp.gov.br/dataset/0aecfa2b-aa3a-40d4-8183-0d4351b7fd0a/"
    "resource/3c12fe0a-ec4f-4a20-a987-dd177330f4ce/download/arquivofinal3tride2024.csv",
)
# De onde vêm os CSVs do SP156 (SP156_MODE=real):
#   auto  (padrão) -> tenta a API do portal (CKAN) e, se falhar, usa os CSVs de SP156_LOCAL_PATH
#   api            -> só a API (falha se o portal estiver fora do ar)
#   local          -> só SP156_LOCAL_PATH
SP156_FONTE = os.getenv("SP156_FONTE", "auto").strip().lower() or "auto"
SP156_CKAN_PACKAGE_URL = os.getenv(
    "SP156_CKAN_PACKAGE_URL",
    "https://dados.prefeitura.sp.gov.br/api/3/action/package_show?id=0aecfa2b-aa3a-40d4-8183-0d4351b7fd0a",
)
# Ano dos arquivos trimestrais a baixar. Vazio = ano da data de execução (ds) da DAG.
SP156_ANO = os.getenv("SP156_ANO", "").strip()
# Caminho de um CSV já baixado (tem prioridade sobre a URL). Ex.: data/raw/sp156_3tri2024.csv
SP156_LOCAL_PATH = os.getenv("SP156_LOCAL_PATH", "")
# Formato das datas dos CSVs. Vazio = detecta por arquivo (no 3º tri/2026 vem mm/dd/aaaa; nos demais dd/mm/aaaa).
# Defina "%d/%m/%Y" ou "%m/%d/%Y" para forçar.
SP156_DATE_FORMAT = os.getenv("SP156_DATE_FORMAT", "")
SP156_MAX_ROWS = int(os.getenv("SP156_MAX_ROWS") or 0) or None  # 0 = sem limite
SP156_CHUNK = int(os.getenv("SP156_CHUNK") or 200000)

# Fonte 2 — reportes do app CidadeViva
#   Se CIDADEVIVA_API_URL estiver definido, o pipeline lê GET {url}/reportes/export?desde=AAAA-MM-DD
#   Caso contrário gera um lote simulado (justificado: o MVP ainda tem poucos usuários reais).
CIDADEVIVA_API_URL = os.getenv("CIDADEVIVA_API_URL", "")
CIDADEVIVA_SIM_N = int(os.getenv("CIDADEVIVA_SIM_N", "400"))

# Qualidade: % mínimo de solicitações SP156 com subprefeitura resolvida (no dado real, parte vem sem endereço)
DQ_MIN_SUBPREF = float(os.getenv("DQ_MIN_SUBPREF", "0.80"))

# Segurança / LGPD: salt da pseudonimização do usuário (trocar em produção)
HASH_SALT = os.getenv("HASH_SALT", "cidadeviva-dev-salt")

# IA: abaixo deste limiar de confiança o reporte vai para revisão humana
LIMIAR_CONFIANCA_IA = float(os.getenv("LIMIAR_CONFIANCA_IA", "0.70"))

# Bounding box do município de São Paulo (validação de coordenadas)
SP_BBOX = {"lat_min": -24.01, "lat_max": -23.35, "lon_min": -46.83, "lon_max": -46.36}

for p in (BRONZE, SILVER, GOLD):
    p.mkdir(parents=True, exist_ok=True)