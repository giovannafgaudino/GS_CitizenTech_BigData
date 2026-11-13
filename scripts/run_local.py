"""Executa o pipeline ponta a ponta SEM Airflow (mesma ordem da DAG) — útil para testar e avaliar rápido.

    python scripts/run_local.py --ds 2026-11-12                 # usa o que estiver em SP156_MODE (padrão: real)
    SP156_MODE=sample python scripts/run_local.py --ds 2026-11-12
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import gold, ingest, load, quality, transform  # noqa: E402
from pipeline.utils import log  # noqa: E402


def rodar(ds: str) -> dict:
    etapas = [
        ("ingerir_sp156", ingest.ingerir_sp156), ("ingerir_cidadeviva", ingest.ingerir_cidadeviva),
        ("ingerir_referencias", ingest.ingerir_referencias),
        ("silver_sp156", transform.silver_sp156), ("silver_cidadeviva", transform.silver_cidadeviva),
        ("construir_gold", gold.construir_gold), ("validar_qualidade", quality.validar_gold),
        ("carregar_dw", load.carregar_warehouse),
    ]
    resultado = {}
    for nome, fn in etapas:
        log.info("=== %s (ds=%s) ===", nome, ds)
        resultado[nome] = fn(ds)
    resultado["contagens_dw"] = load.contagens()
    log.info("contagens no DW: %s", resultado["contagens_dw"])
    return resultado


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", default=date.today().isoformat(), help="data lógica AAAA-MM-DD")
    rodar(ap.parse_args().ds)
