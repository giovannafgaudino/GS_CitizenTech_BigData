"""Camada SILVER — limpeza, tipagem, padronização e enriquecimento.

Regras principais
  * nomes de coluna padronizados (snake_case, sem acento);
  * datas tipadas, CEP com 8 dígitos, coordenadas validadas contra o bbox de SP;
  * subprefeitura ligada à tabela de referência (chave normalizada);
  * SP156: cada solicitação é classificada na taxonomia CidadeViva por regras (seeds/regras_categoria_sp156.csv);
    solicitações que não são de zeladoria (IPTU, cadastro, ruído...) são descartadas e contadas;
  * CidadeViva: deduplicação por id, PII removida (nome/e-mail viram hash; telefone/CPF mascarados no texto),
    categoria da IA padronizada e marcação de revisão humana quando a confiança é baixa.
"""
from __future__ import annotations

import json
import re

import numpy as np
import pandas as pd

from . import config
from .utils import chave_subprefeitura, gravar, gravar_json, log, mascarar_pii, normalizar, normalizar_cep, para_float, sha256

# Nomes normalizados (normalizar()) das colunas do CSV oficial -> nomes silver.
# O layout mudou em 2026 (Prefeitura Operacional, Data de Finalização, Meio de Comunicação, Status); os dois são aceitos.
_MAPA_SP156 = {
    "data de abertura": "data_abertura",
    "data do parecer": "data_parecer", "data de finalizacao": "data_parecer",
    "canal": "canal", "meio de comunicacao": "canal",
    "tema": "tema", "assunto": "assunto", "servico": "servico",
    "logradouro": "logradouro", "numero": "numero", "cep": "cep",
    "subprefeitura": "subprefeitura_origem", "prefeitura operacional": "subprefeitura_origem",
    "distrito": "distrito", "latitude": "latitude", "longitude": "longitude",
    "status da solicitacao": "status_origem", "status": "status_origem",
    "orgao": "orgao", "nivel satisfacao": "nivel_satisfacao",
}

_STATUS = {
    "aberta": "aberto", "em andamento": "em_andamento", "em analise": "em_andamento",
    "finalizada": "finalizado", "indeferida": "indeferido", "cancelada": "cancelado",
    "novo": "aberto", "triado": "em_andamento", "enviado prefeitura": "em_andamento", "resolvido": "finalizado",
}


def _dentro_sp(lat: pd.Series, lon: pd.Series) -> pd.Series:
    b = config.SP_BBOX
    return lat.between(b["lat_min"], b["lat_max"]) & lon.between(b["lon_min"], b["lon_max"])


def _ref_subprefeituras(ds: str) -> dict[str, int]:
    ref = config.BRONZE / "referencias" / f"dt={ds}"
    subs = pd.read_csv(ref / "subprefeituras.csv")
    m = {chave_subprefeitura(n): int(i) for n, i in zip(subs.nome, subs.subprefeitura_id)}
    alias = ref / "aliases_subprefeitura.csv"
    if alias.exists():
        al = pd.read_csv(alias)
        m.update({chave_subprefeitura(a): int(i) for a, i in zip(al.alias, al.subprefeitura_id)})
    return m


def _regras(ds: str) -> list[tuple[re.Pattern, str]]:
    r = pd.read_csv(config.BRONZE / "referencias" / f"dt={ds}" / "regras_categoria_sp156.csv").sort_values("ordem")
    return [(re.compile(p), c) for p, c in zip(r.padrao_regex, r.categoria_codigo)]


def _abrir_csv(caminho):
    """Abre o CSV em chunks detectando codificação (UTF-8 ou cp1252) e separador (';' ou ',') pelo cabeçalho."""
    with open(caminho, "rb") as f:
        amostra = f.read(200_000)
    try:
        amostra[:-4].decode("utf-8")  # ignora possível caractere cortado no fim da amostra
        enc, extra = "utf-8-sig", {}
    except UnicodeDecodeError:
        enc, extra = "cp1252", {"encoding_errors": "replace"}
    cab = amostra.split(b"\n", 1)[0].decode("utf-8" if enc == "utf-8-sig" else "cp1252", errors="replace")
    sep = ";" if cab.count(";") > cab.count(",") else ","
    log.info("SP156: %s -> encoding=%s separador=%r", caminho.name, enc, sep)
    leitor = pd.read_csv(caminho, sep=sep, encoding=enc, dtype=str, chunksize=config.SP156_CHUNK,
                         nrows=config.SP156_MAX_ROWS, on_bad_lines="warn", **extra)
    return leitor, enc, sep


def _detectar_formato_data(*series: pd.Series, padrao: str = "%d/%m/%Y") -> str:
    """'%d/%m/%Y' | '%m/%d/%Y' | 'iso'. O 3º tri/2026 vem mm/dd/aaaa (ex.: 07/25/2026); os demais dd/mm/aaaa."""
    if config.SP156_DATE_FORMAT:
        return config.SP156_DATE_FORMAT
    partes = pd.concat([x.dropna().astype(str) for x in series]).str.extract(r"^\s*(\d{1,2})/(\d{1,2})/\d{4}").dropna()
    if partes.empty:
        return "iso"
    a, b = pd.to_numeric(partes[0]).max(), pd.to_numeric(partes[1]).max()
    if a > 12:
        return "%d/%m/%Y"
    if b > 12:
        return "%m/%d/%Y"
    log.warning("SP156: formato de data ambíguo (todos os valores têm dia e mês ≤ 12); usando %s. "
                "Se estiver errado, defina SP156_DATE_FORMAT='%%d/%%m/%%Y' ou '%%m/%%d/%%Y'.", padrao)
    return padrao


def _parse_data(serie: pd.Series, fmt: str) -> pd.Series:
    if fmt == "iso":
        return pd.to_datetime(serie, errors="coerce")
    d = pd.to_datetime(serie, format=fmt, errors="coerce")
    falta = d.isna() & serie.notna()
    if falta.any():  # valores com hora ("dd/mm/aaaa hh:mm:ss")
        d[falta] = pd.to_datetime(serie[falta], format=fmt + " %H:%M:%S", errors="coerce")
    return d


def silver_sp156(ds: str) -> dict:
    from collections import Counter
    from pathlib import Path

    pasta = config.BRONZE / "sp156" / f"dt={ds}"
    brutos = sorted(pasta.glob("sp156_raw_*.csv"))
    if not brutos:
        raise FileNotFoundError("Nenhum sp156_raw_*.csv na bronze: rode a task ingerir_sp156 antes.")
    manifesto = json.loads((pasta / "_manifest.json").read_text(encoding="utf-8"))
    nome_origem = {a["arquivo"]: Path(a["origem"]).stem for a in manifesto["arquivos"]}
    subs, regras = _ref_subprefeituras(ds), _regras(ds)

    partes, lidas, sem_data = [], 0, 0
    nao_class, sub_nao_resolvida, status_cont, por_cat = Counter(), Counter(), Counter(), Counter()
    info_arquivos, ultimo_fmt = {}, "%d/%m/%Y"

    for bruto in brutos:
        leitor, enc, sep = _abrir_csv(bruto)
        fmt = None
        for chunk in leitor:
            lidas += len(chunk)
            linha = pd.Series(chunk.index + 1, index=chunk.index).astype(str)  # nº da linha de dados no arquivo
            chunk.columns = [normalizar(c) for c in chunk.columns]
            chunk = chunk.loc[:, ~pd.Index(chunk.columns).duplicated()]
            chunk = chunk.rename(columns={k: v for k, v in _MAPA_SP156.items() if k in chunk.columns})
            obrigatorias = {"data_abertura", "tema", "assunto", "servico"}
            if not obrigatorias <= set(chunk.columns):
                raise ValueError(f"{bruto.name}: faltam colunas {sorted(obrigatorias - set(chunk.columns))}. "
                                 f"Colunas encontradas (normalizadas): {list(chunk.columns)[:25]}. "
                                 "Ajuste _MAPA_SP156 em pipeline/transform.py.")
            for col in set(_MAPA_SP156.values()) - set(chunk.columns):
                chunk[col] = None
            if fmt is None:
                fmt = ultimo_fmt = _detectar_formato_data(chunk.data_abertura, chunk.data_parecer, padrao=ultimo_fmt)
                info_arquivos[bruto.name] = {"origem": nome_origem[bruto.name], "encoding": enc, "separador": sep, "formato_data": fmt}
                log.info("SP156: %s -> formato de data %s", bruto.name, fmt)

            # Classifica pelo NOME DO SERVIÇO (é descritivo); tema/assunto só entram quando o serviço vem vazio.
            # (Tema+assunto juntos geravam falsos positivos: ex. assunto "Fiação e Postes" com serviço de guias e sarjetas.)
            texto = chunk.servico.where(chunk.servico.notna() & (chunk.servico.str.strip() != ""), chunk.assunto).fillna("").map(normalizar)
            cat = pd.Series(None, index=chunk.index, dtype="object")
            for padrao, codigo in regras:
                livre = cat.isna()
                cat[livre & texto.str.contains(padrao)] = codigo
            sobra = cat.isna()
            nao_class.update((chunk.tema.fillna("") + " | " + chunk.assunto.fillna("") + " | " + chunk.servico.fillna("")).loc[sobra].value_counts().to_dict())
            chunk = chunk.assign(categoria_codigo=cat, _linha=linha)[~sobra]
            if chunk.empty:
                continue

            abertura = _parse_data(chunk.data_abertura, fmt)
            sub_id = chunk.subprefeitura_origem.map(lambda x: subs.get(chave_subprefeitura(x)) if pd.notna(x) else None)
            sem_sub = sub_id.isna() & chunk.subprefeitura_origem.notna() & (chunk.subprefeitura_origem.str.strip() != "")
            sub_nao_resolvida.update(chunk.subprefeitura_origem[sem_sub].value_counts().to_dict())
            status_cont.update(chunk.status_origem.fillna("(vazio)").str.strip().str.upper().value_counts().to_dict())
            por_cat.update(chunk.categoria_codigo.value_counts().to_dict())

            df = pd.DataFrame({
                "id_origem": nome_origem[bruto.name] + ":" + chunk._linha,  # CSV sem protocolo: arquivo + linha
                "data_abertura": abertura,
                "data_parecer": _parse_data(chunk.data_parecer, fmt),
                "canal": chunk.canal.str.strip().str.upper(),
                "tema": chunk.tema.str.strip(),
                "assunto": chunk.assunto.str.strip(),
                "servico": chunk.servico.str.strip(),
                "categoria_codigo": chunk.categoria_codigo,
                "logradouro": chunk.logradouro.str.strip().str.title(),
                "numero": chunk.numero.str.strip(),
                "cep": chunk.cep.map(normalizar_cep),
                "subprefeitura_origem": chunk.subprefeitura_origem,
                "subprefeitura_id": sub_id,
                "distrito": chunk.distrito.str.strip().str.upper(),
                "latitude": para_float(chunk.latitude),
                "longitude": para_float(chunk.longitude),
                "status_origem": chunk.status_origem.str.strip().str.upper(),
                "orgao": chunk.orgao.str.strip(),
                "nivel_satisfacao": pd.to_numeric(chunk.nivel_satisfacao, errors="coerce"),
            })
            partes.append(df)

    df = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()
    if df.empty:
        raise ValueError("Nenhuma solicitação de zeladoria encontrada. Veja 'top_nao_classificados' em _stats.json e ajuste "
                         "seeds/regras_categoria_sp156.csv.")
    antes = len(df)
    df = df[df.data_abertura.notna()]
    sem_data = antes - len(df)

    # coordenada: 0/0 ou fora do município vira nula
    fora = ~_dentro_sp(df.latitude, df.longitude) & df.latitude.notna()
    df.loc[fora, ["latitude", "longitude"]] = np.nan
    df["status"] = df.status_origem.map(lambda s: _STATUS.get(normalizar(s), "outro"))
    horas = (df.data_parecer - df.data_abertura).dt.total_seconds() / 3600
    df["tempo_resposta_horas"] = horas.where(horas >= 0).round(2)
    df["subprefeitura_id"] = df.subprefeitura_id.astype("Int64")
    df["dt_ingestao"] = ds

    gravar(df, config.SILVER / "sp156" / f"dt={ds}" / "solicitacoes")
    stats = {"linhas_lidas": lidas, "nao_zeladoria_descartadas": lidas - antes, "sem_data_valida": sem_data,
             "coordenadas_invalidas_anuladas": int(fora.sum()), "linhas_silver": len(df),
             "sem_subprefeitura": int(df.subprefeitura_id.isna().sum())}
    detalhes = {"arquivos": info_arquivos, "por_categoria": dict(por_cat), "status_origem": dict(status_cont.most_common(15)),
                "top_nao_classificados": nao_class.most_common(40),
                "subprefeituras_nao_resolvidas": sub_nao_resolvida.most_common(15)}
    gravar_json({**stats, **detalhes}, config.SILVER / "sp156" / f"dt={ds}" / "_stats.json")
    log.info("SP156 silver: %s", stats)
    log.info("SP156 por categoria: %s", dict(por_cat))
    if detalhes["subprefeituras_nao_resolvidas"]:
        log.warning("SP156: subprefeituras não reconhecidas (adicione em seeds/aliases_subprefeitura.csv): %s",
                    detalhes["subprefeituras_nao_resolvidas"])
    return stats


def silver_cidadeviva(ds: str) -> dict:
    bruto = json.loads((config.BRONZE / "cidadeviva" / f"dt={ds}" / "reportes.json").read_text(encoding="utf-8"))
    df = pd.json_normalize(bruto, sep="_")
    lidas = len(df)
    if df.empty:  # dia sem reportes no app: grava silver vazio (tipos certos) em vez de falhar
        vazio = pd.DataFrame({
            "id_reporte": pd.Series(dtype="object"), "criado_em": pd.Series(dtype="datetime64[ns]"),
            "canal": pd.Series(dtype="object"), "descricao": pd.Series(dtype="object"),
            "possui_foto": pd.Series(dtype="bool"), "logradouro": pd.Series(dtype="object"),
            "numero": pd.Series(dtype="string"), "cep": pd.Series(dtype="object"),
            "subprefeitura_id": pd.Series(dtype="Int64"), "latitude": pd.Series(dtype="float64"),
            "longitude": pd.Series(dtype="float64"), "categoria_ia": pd.Series(dtype="object"),
            "confianca_ia": pd.Series(dtype="float64"), "modelo_ia": pd.Series(dtype="object"),
            "requer_revisao_humana": pd.Series(dtype="bool"), "status_app": pd.Series(dtype="object"),
            "status": pd.Series(dtype="object"), "protocolo_prefeitura": pd.Series(dtype="object"),
            "usuario_hash": pd.Series(dtype="object"), "dt_ingestao": pd.Series(dtype="object"),
        })
        gravar(vazio, config.SILVER / "cidadeviva" / f"dt={ds}" / "reportes")
        stats = {"linhas_lidas": 0, "duplicadas_removidas": 0, "latlon_invertidos_corrigidos": 0,
                 "coordenadas_ausentes_ou_invalidas": 0, "revisao_humana": 0, "linhas_silver": 0}
        gravar_json(stats, config.SILVER / "cidadeviva" / f"dt={ds}" / "_stats.json")
        log.info("CidadeViva silver: nenhum reporte em %s: %s", ds, stats)
        return stats
    subs = _ref_subprefeituras(ds)
    cats_validas = set(pd.read_csv(config.SEEDS / "categorias.csv").codigo)

    df = df.drop_duplicates("id", keep="last")
    lat, lon = para_float(df.endereco_lat), para_float(df.endereco_lon)
    trocado = _dentro_sp(lon, lat)  # lat/lon invertidos -> desinverte
    lat, lon = lat.where(~trocado, lon), lon.where(~trocado, lat)
    invalida = ~_dentro_sp(lat, lon)

    cat = df.ia_categoria.map(normalizar).str.replace(" ", "_")
    conf = pd.to_numeric(df.ia_confianca, errors="coerce")
    revisao = conf.isna() | (conf < config.LIMIAR_CONFIANCA_IA) | ~cat.isin(cats_validas)

    out = pd.DataFrame({
        "id_reporte": df.id,
        "criado_em": pd.to_datetime(df.created_at, errors="coerce", utc=True).dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None),
        "canal": df.canal.str.lower(),
        "descricao": df.descricao.map(mascarar_pii),
        "possui_foto": df.foto_url.notna(),
        "logradouro": df.endereco_logradouro.str.strip(),
        "numero": df.endereco_numero.astype("string"),
        "cep": df.endereco_cep.map(normalizar_cep),
        "subprefeitura_id": df.endereco_subprefeitura.map(lambda s: subs.get(chave_subprefeitura(s))).astype("Int64"),
        "latitude": lat.where(~invalida),
        "longitude": lon.where(~invalida),
        "categoria_ia": cat.where(cat.isin(cats_validas)),
        "confianca_ia": conf,
        "modelo_ia": df.ia_modelo,
        "requer_revisao_humana": revisao,
        "status_app": df.status,
        "status": df.status.map(lambda s: _STATUS.get(normalizar(s), "outro")),
        "protocolo_prefeitura": df.protocolo_prefeitura,
        "usuario_hash": df.usuario_id.map(lambda u: sha256(u, salt=config.HASH_SALT)),  # LGPD: sem nome/e-mail
        "dt_ingestao": ds,
    })
    out = out[out.criado_em.notna()]
    gravar(out, config.SILVER / "cidadeviva" / f"dt={ds}" / "reportes")
    stats = {"linhas_lidas": lidas, "duplicadas_removidas": lidas - len(df), "latlon_invertidos_corrigidos": int(trocado.sum()),
             "coordenadas_ausentes_ou_invalidas": int(invalida.sum()), "revisao_humana": int(revisao.sum()), "linhas_silver": len(out)}
    gravar_json(stats, config.SILVER / "cidadeviva" / f"dt={ds}" / "_stats.json")
    log.info("CidadeViva silver: %s", stats)
    return stats
