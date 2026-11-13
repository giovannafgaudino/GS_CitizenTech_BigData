"""Camada GOLD — modelo dimensional (star schema) + marts prontos para o MVP e para os modelos de IA.

Tabelas entregues (documentadas no dicionário de dados do PDF):
  dim_data, dim_subprefeitura, dim_categoria
  fato_solicitacao_sp156   (1 linha por solicitação de zeladoria da Prefeitura)
  fato_reporte             (1 linha por reporte feito no app CidadeViva)
  mart_demanda_subprefeitura_mes  (alimenta o mapa/ranking do MVP)
  mart_triagem_ia          (qualidade do modelo de triagem; alimenta a fila de revisão humana)
"""
from __future__ import annotations

import pandas as pd

from . import config
from .utils import gravar, gravar_json, ler, log


def _dim_data(datas: pd.Series) -> pd.DataFrame:
    d = pd.to_datetime(datas.dropna().dt.normalize().unique())
    if len(d) == 0:
        return pd.DataFrame(columns=["data_id", "data", "ano", "mes", "dia", "trimestre", "dia_semana", "fim_de_semana"])
    rng = pd.date_range(d.min(), d.max(), freq="D")
    return pd.DataFrame({
        "data_id": rng.strftime("%Y%m%d").astype(int),
        "data": rng.date,
        "ano": rng.year, "mes": rng.month, "dia": rng.day, "trimestre": rng.quarter,
        "dia_semana": rng.dayofweek + 1,  # 1=segunda ... 7=domingo
        "fim_de_semana": rng.dayofweek >= 5,
    })


def construir_gold(ds: str) -> dict:
    sp = ler(config.SILVER / "sp156" / f"dt={ds}" / "solicitacoes", parse_dates=["data_abertura", "data_parecer"])
    cv = ler(config.SILVER / "cidadeviva" / f"dt={ds}" / "reportes", parse_dates=["criado_em"])
    subs = pd.read_csv(config.SEEDS / "subprefeituras.csv")
    cats = pd.read_csv(config.SEEDS / "categorias.csv")
    cat_id = dict(zip(cats.codigo, cats.categoria_id))

    # ---- dimensões
    dim_sub = subs.rename(columns={"nome": "nome_subprefeitura", "centroide_lat_aprox": "centroide_lat",
                                   "centroide_lon_aprox": "centroide_lon"})
    dim_cat = cats[["categoria_id", "codigo", "nome", "descricao", "orgao_sugerido", "prioridade_base"]]
    dim_data = _dim_data(pd.concat([sp.data_abertura, cv.criado_em], ignore_index=True))

    # ---- fatos
    hora = sp.data_abertura.dt.hour.astype("Int64")
    if (hora.fillna(0) == 0).all():  # CSVs de 2026 trazem só a data (sem hora) -> hora não informada
        hora = pd.Series(pd.NA, index=sp.index, dtype="Int64")
    f_sp = pd.DataFrame({
        "solicitacao_id": sp.id_origem,
        "data_id": sp.data_abertura.dt.strftime("%Y%m%d").astype(int),
        "hora_abertura": hora,
        "subprefeitura_id": sp.subprefeitura_id.astype("Int64"),
        "categoria_id": sp.categoria_codigo.map(cat_id).astype("Int64"),
        "canal": sp.canal, "servico": sp.servico, "logradouro": sp.logradouro, "cep": sp.cep,
        "latitude": sp.latitude, "longitude": sp.longitude,
        "status": sp.status, "tempo_resposta_horas": sp.tempo_resposta_horas,
        "nivel_satisfacao": sp.nivel_satisfacao, "dt_ingestao": ds,
    })
    f_cv = pd.DataFrame({
        "reporte_id": cv.id_reporte,
        "data_id": cv.criado_em.dt.strftime("%Y%m%d").astype(int),
        "criado_em": cv.criado_em,
        "subprefeitura_id": cv.subprefeitura_id.astype("Int64"),
        "categoria_id": cv.categoria_ia.map(cat_id).astype("Int64"),
        "canal": cv.canal, "descricao": cv.descricao, "possui_foto": cv.possui_foto,
        "latitude": cv.latitude, "longitude": cv.longitude,
        "confianca_ia": cv.confianca_ia, "requer_revisao_humana": cv.requer_revisao_humana,
        "status": cv.status, "protocolo_prefeitura": cv.protocolo_prefeitura.astype("string"),
        "usuario_hash": cv.usuario_hash, "dt_ingestao": ds,
    })

    # ---- marts
    sp_mes = sp.assign(ano_mes=sp.data_abertura.dt.strftime("%Y-%m"))
    m = (sp_mes.groupby(["ano_mes", "subprefeitura_id", "categoria_codigo"], dropna=True)
         .agg(qtd_solicitacoes=("id_origem", "count"),
              qtd_finalizadas=("status", lambda s: int((s == "finalizado").sum())),
              tempo_medio_resposta_horas=("tempo_resposta_horas", "mean"))
         .reset_index())
    m["categoria_id"] = m.categoria_codigo.map(cat_id).astype("Int64")
    m["taxa_resolucao"] = (m.qtd_finalizadas / m.qtd_solicitacoes).round(4)
    m["tempo_medio_resposta_horas"] = m.tempo_medio_resposta_horas.round(2)
    m["subprefeitura_id"] = m.subprefeitura_id.astype("Int64")
    m["dt_ingestao"] = ds
    mart_demanda = m[["ano_mes", "subprefeitura_id", "categoria_id", "qtd_solicitacoes", "qtd_finalizadas",
                      "taxa_resolucao", "tempo_medio_resposta_horas", "dt_ingestao"]]

    t = (cv.assign(data=cv.criado_em.dt.date)
         .groupby(["data", "categoria_ia"], dropna=False)
         .agg(qtd_reportes=("id_reporte", "count"),
              confianca_media=("confianca_ia", "mean"),
              qtd_revisao_humana=("requer_revisao_humana", "sum"))
         .reset_index().rename(columns={"categoria_ia": "categoria_codigo"}))
    t["categoria_id"] = t.categoria_codigo.map(cat_id).astype("Int64")
    t["pct_revisao_humana"] = (t.qtd_revisao_humana / t.qtd_reportes).round(4)
    t["confianca_media"] = t.confianca_media.round(4)
    t["dt_ingestao"] = ds
    mart_triagem = t[["data", "categoria_id", "qtd_reportes", "confianca_media", "qtd_revisao_humana",
                      "pct_revisao_humana", "dt_ingestao"]]

    tabelas = {"dim_data": dim_data, "dim_subprefeitura": dim_sub, "dim_categoria": dim_cat,
               "fato_solicitacao_sp156": f_sp, "fato_reporte": f_cv,
               "mart_demanda_subprefeitura_mes": mart_demanda, "mart_triagem_ia": mart_triagem}
    for nome, df in tabelas.items():
        gravar(df, config.GOLD / f"dt={ds}" / nome)
    resumo = {n: len(d) for n, d in tabelas.items()}
    gravar_json(resumo, config.GOLD / f"dt={ds}" / "_resumo.json")
    log.info("gold: %s", resumo)
    return resumo
