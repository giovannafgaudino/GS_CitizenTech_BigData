"""Carga da camada gold no Data Warehouse (PostgreSQL no Docker; SQLite no modo local).

Idempotência: dimensões são recriadas (full refresh); fatos e marts removem a partição dt_ingestao=ds
antes de inserir, então reexecutar o mesmo dia não duplica linhas.
"""
from __future__ import annotations

import sqlite3

import pandas as pd

from . import config
from .utils import ler, log

DIMENSOES = ["dim_data", "dim_subprefeitura", "dim_categoria"]
PARTICIONADAS = ["fato_solicitacao_sp156", "fato_reporte", "mart_demanda_subprefeitura_mes", "mart_triagem_ia"]


def _conectar():
    url = config.WAREHOUSE_URL
    if url.startswith("sqlite:///"):
        caminho = url.replace("sqlite:///", "", 1)
        con = sqlite3.connect(caminho)
        return con, "sqlite", None
    from sqlalchemy import create_engine  # só necessário fora do modo local

    eng = create_engine(url)
    return eng, "sql", eng


def _tabela_existe(con, kind, nome) -> bool:
    if kind == "sqlite":
        return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nome,)).fetchone() is not None
    from sqlalchemy import inspect

    return inspect(con).has_table(nome)


def carregar_warehouse(ds: str) -> dict:
    con, kind, eng = _conectar()
    base = config.GOLD / f"dt={ds}"
    carregadas = {}
    # PostgreSQL: insere em lotes (muito mais rápido p/ ~470 mil linhas). Limite de 65.535 parâmetros -> 2000 linhas x ≤16 colunas.
    # SQLite (modo local) fica no padrão, que tem limite menor de variáveis.
    kw = {} if kind == "sqlite" else {"method": "multi", "chunksize": 2000}
    try:
        for nome in DIMENSOES:
            df = ler(base / nome)
            df.to_sql(nome, con, if_exists="replace", index=False, **kw)
            carregadas[nome] = len(df)
        for nome in PARTICIONADAS:
            df = ler(base / nome)
            if _tabela_existe(con, kind, nome):
                if kind == "sqlite":
                    con.execute(f"DELETE FROM {nome} WHERE dt_ingestao = ?", (ds,))
                    con.commit()
                else:
                    from sqlalchemy import text

                    with con.begin() as c:
                        c.execute(text(f"DELETE FROM {nome} WHERE dt_ingestao = :ds"), {"ds": ds})
            df.to_sql(nome, con, if_exists="append", index=False, **kw)
            carregadas[nome] = len(df)
        if kind == "sqlite":
            con.commit()
        log.info("warehouse (%s) carregado: %s", config.WAREHOUSE_URL.split("@")[-1], carregadas)
    finally:
        if kind == "sqlite":
            con.close()
        elif eng is not None:
            eng.dispose()
    return carregadas


def contagens() -> dict:
    """Usado como prova de carga: conta linhas de cada tabela já no banco."""
    con, kind, eng = _conectar()
    try:
        out = {}
        for nome in DIMENSOES + PARTICIONADAS:
            if kind == "sqlite":
                out[nome] = con.execute(f"SELECT COUNT(*) FROM {nome}").fetchone()[0]
            else:
                from sqlalchemy import text

                with con.connect() as c:
                    out[nome] = c.execute(text(f"SELECT COUNT(*) FROM {nome}")).scalar()
        return out
    finally:
        if kind == "sqlite":
            con.close()
        elif eng is not None:
            eng.dispose()
