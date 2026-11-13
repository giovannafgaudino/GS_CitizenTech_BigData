"""Baixa o CSV REAL do SP156 (Portal de Dados Abertos da Prefeitura de SP, CC0) para data/raw/.

    python scripts/baixar_sp156.py
    python scripts/baixar_sp156.py --url "<link do CSV copiado do portal>"

Se o portal devolver uma página HTML em vez do CSV, o script avisa e mostra como baixar manualmente
(abra o link no navegador, escolha o trimestre em CSV e clique em Baixar; salve em data/raw/).
"""
import argparse, shutil, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline import config  # noqa: E402
from pipeline.ingest import validar_csv_sp156  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--url", default=config.SP156_URL)
ap.add_argument("--nome", default=None, help="nome do arquivo de saída")
a = ap.parse_args()

destino = ROOT / "data" / "raw" / (a.nome or a.url.rsplit("/", 1)[-1].split("?")[0] or "sp156.csv")
destino.parent.mkdir(parents=True, exist_ok=True)
print("baixando", a.url)
req = urllib.request.Request(a.url, headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/csv,application/octet-stream,*/*", "Referer": "https://dados.prefeitura.sp.gov.br/dataset/dados-do-sp156"})
try:
    with urllib.request.urlopen(req, timeout=600) as r, open(destino, "wb") as f:
        shutil.copyfileobj(r, f, length=1 << 20)
    validar_csv_sp156(destino)
except Exception as e:  # noqa: BLE001
    destino.unlink(missing_ok=True)
    sys.exit(f"\nNão consegui baixar o CSV automaticamente: {e}\n\n"
             "FAÇA MANUALMENTE:\n"
             "  1. Abra https://dados.prefeitura.sp.gov.br/dataset/dados-do-sp156 no navegador\n"
             "  2. Escolha um trimestre em formato CSV (ex.: 'Dados do SP156 - 1º TRI 2026') e clique em 'Baixar'\n"
             f"  3. Salve o arquivo dentro de {destino.parent}\n"
             "  4. Rode: python scripts/run_local.py --ds 2026-11-12   (depois de definir SP156_LOCAL_PATH, ver README)")
print(f"ok: {destino} ({destino.stat().st_size / 1e6:.1f} MB)")
print(f"\nPróximo passo: defina SP156_LOCAL_PATH={destino.relative_to(ROOT)} e rode scripts/run_local.py")
