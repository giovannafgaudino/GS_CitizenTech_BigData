"""Verificações de qualidade (data quality gates). Falha => a task do Airflow falha e nada é carregado."""
from __future__ import annotations

import json

import pandas as pd

from . import config
from .utils import gravar_json, ler, log

DIMS = ["dim_data", "dim_subprefeitura", "dim_categoria"]


class QualidadeErro(Exception):
    pass


def validar_gold(ds: str) -> dict:
    base = config.GOLD / f"dt={ds}"
    t = {n: ler(base / n) for n in DIMS + ["fato_solicitacao_sp156", "fato_reporte",
                                         "mart_demanda_subprefeitura_mes", "mart_triagem_ia"]}
    checks = []

    def check(nome: str, ok: bool, detalhe: str = ""):
        checks.append({"check": nome, "ok": bool(ok), "detalhe": detalhe})
        (log.info if ok else log.error)("DQ %-48s %s %s", nome, "OK" if ok else "FALHOU", detalhe)

    f_sp, f_cv = t["fato_solicitacao_sp156"], t["fato_reporte"]
    check("fato_solicitacao_sp156 não vazia", len(f_sp) > 0, f"{len(f_sp)} linhas")
    # dia sem reportes vale se o app respondeu e informou 0 registros (manifest); vazio sem essa prova = falha
    man = config.BRONZE / "cidadeviva" / f"dt={ds}" / "_manifest.json"
    dia_vazio_confirmado = man.exists() and json.loads(man.read_text(encoding="utf-8")).get("registros") == 0
    check("fato_reporte não vazia (ou dia sem reportes confirmado pelo app)", len(f_cv) > 0 or dia_vazio_confirmado, f"{len(f_cv)} linhas")
    check("PK única em fato_solicitacao_sp156", f_sp.solicitacao_id.is_unique)
    check("PK única em fato_reporte", f_cv.reporte_id.is_unique)
    check("PK única em dim_subprefeitura", t["dim_subprefeitura"].subprefeitura_id.is_unique)
    check("PK única em dim_data", t["dim_data"].data_id.is_unique)

    # integridade referencial (ignora FKs nulas, que são permitidas)
    for fato, nome in ((f_sp, "sp156"), (f_cv, "reporte")):
        fk = fato.subprefeitura_id.dropna().astype(int)
        check(f"FK subprefeitura válida ({nome})", fk.isin(t["dim_subprefeitura"].subprefeitura_id).all())
        fk = fato.categoria_id.dropna().astype(int)
        check(f"FK categoria válida ({nome})", fk.isin(t["dim_categoria"].categoria_id).all())
        check(f"FK data válida ({nome})", fato.data_id.isin(t["dim_data"].data_id).all())

    # completude
    pct_sub = f_sp.subprefeitura_id.notna().mean()
    check(f"≥ {config.DQ_MIN_SUBPREF:.0%} das solicitações SP156 com subprefeitura", pct_sub >= config.DQ_MIN_SUBPREF, f"{pct_sub:.1%}")
    check("campos obrigatórios do reporte sem nulos",
          f_cv[["reporte_id", "data_id", "usuario_hash"]].notna().all().all())

    # domínio / plausibilidade
    check("confiança da IA entre 0 e 1", f_cv.confianca_ia.dropna().between(0, 1).all())
    check("tempo de resposta SP156 ≥ 0", (f_sp.tempo_resposta_horas.dropna() >= 0).all())
    b = config.SP_BBOX
    lat, lon = f_cv.latitude.dropna(), f_cv.longitude.dropna()
    check("coordenadas dos reportes dentro de SP",
          lat.between(b["lat_min"], b["lat_max"]).all() and lon.between(b["lon_min"], b["lon_max"]).all())

    # LGPD: nenhum e-mail/telefone residual no texto livre
    padrao = r"[\w.+-]+@[\w-]+\.[\w.]+|\(?\d{2}\)?\s?9?\d{4}[-\s]\d{4}"
    check("sem PII (e-mail/telefone) em fato_reporte.descricao", not f_cv.descricao.astype(str).str.contains(padrao, regex=True).any())
    check("sem colunas de nome/e-mail em fato_reporte", not {"nome", "email", "usuario_nome"} & set(f_cv.columns))

    falhas = [c for c in checks if not c["ok"]]
    gravar_json({"ds": ds, "total": len(checks), "falhas": len(falhas), "checks": checks}, base / "_dq_report.json")
    if falhas:
        raise QualidadeErro(f"{len(falhas)} verificação(ões) de qualidade falharam: " + "; ".join(c["check"] for c in falhas))
    return {"total": len(checks), "falhas": 0}
