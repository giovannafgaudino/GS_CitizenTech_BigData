"""Camada BRONZE — ingestão do dado bruto, sem transformação.

Cada fonte é gravada em data/bronze/<fonte>/dt=<data_execucao>/ junto com um _manifest.json
(origem, hash, tamanho, horário). Reexecutar a mesma data sobrescreve a mesma partição -> idempotente.
"""
from __future__ import annotations

import os
import random
import shutil
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from . import config
from .utils import gravar_json, log, sha256


def _particao(fonte: str, ds: str) -> Path:
    p = config.BRONZE / fonte / f"dt={ds}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def validar_csv_sp156(caminho: Path) -> None:
    """Recusa arquivo que não seja o CSV do SP156 (ex.: página HTML devolvida pelo portal em vez do download)."""
    with open(caminho, "rb") as f:
        cab = f.readline(4000)
    if cab.lstrip()[:1] == b"<" or (b";" not in cab and b"," not in cab) or b"abertura" not in cab.lower():
        raise ValueError(
            f"{caminho} não parece o CSV do SP156 (começa com {cab[:60]!r}). Provavelmente é uma página HTML. "
            "Baixe o CSV manualmente em https://dados.prefeitura.sp.gov.br/dataset/dados-do-sp156 "
            "e use SP156_LOCAL_PATH=<caminho do arquivo>."
        )


def _hash_arquivo(caminho: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


# --------------------------------------------------------------------------- SP156

def _arquivos_locais() -> list[Path]:
    """SP156_LOCAL_PATH aceita um arquivo, vários separados por vírgula, ou um padrão (glob), ex.: data/raw/arquivo-final-*-trim-26.csv"""
    import glob

    achados: list[Path] = []
    for pedaco in [p.strip() for p in config.SP156_LOCAL_PATH.split(",") if p.strip()]:
        achados += [Path(x) for x in sorted(glob.glob(pedaco))] or [Path(pedaco)]
    faltando = [str(p) for p in achados if not p.exists()]
    if faltando:
        raise FileNotFoundError(f"SP156_LOCAL_PATH: arquivo(s) não encontrado(s): {faltando}. Confira a pasta atual (pwd).")
    return achados


_UA = {"User-Agent": "Mozilla/5.0 (compatible; cidadeviva-pipeline/1.0)"}


def _http_json(url: str, timeout: int = 60) -> dict:
    import json

    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _http_download(url: str, destino: Path, timeout: int = 600) -> None:
    tmp = destino.with_suffix(destino.suffix + ".part")
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=timeout) as r, open(tmp, "wb") as f:
        shutil.copyfileobj(r, f, length=1 << 20)
    tmp.rename(destino)


def _recursos_sp156_api(ano: int) -> list[dict]:
    """Consulta a API CKAN do portal e devolve os CSVs trimestrais 'Dados do SP156 - nº TRI <ano>' (ordem do trimestre)."""
    import re

    resp = _http_json(config.SP156_CKAN_PACKAGE_URL)
    if not resp.get("success"):
        raise RuntimeError(f"API do portal respondeu success=false: {str(resp)[:200]}")
    achados = []
    for r in resp["result"]["resources"]:
        nome = (r.get("name") or "").strip()
        m = re.search(r"SP156\s*-\s*(\d)\D{0,3}\s*TRI\s*(\d{4})", nome, re.I)
        url = r.get("url") or ""
        formato = (r.get("format") or "").strip().lower().lstrip(".")
        if m and int(m.group(2)) == ano and formato == "csv" and url.lower().endswith(".csv"):
            achados.append({"trimestre": int(m.group(1)), "nome": nome, "url": url, "size": r.get("size")})
    if not achados:
        raise RuntimeError(f"nenhum CSV 'Dados do SP156 - <n> TRI {ano}' encontrado na API do portal")
    return sorted(achados, key=lambda x: x["trimestre"])


def _baixar_sp156_api(ds: str) -> tuple[list[Path], str]:
    ano = int(config.SP156_ANO or ds[:4])
    cache_dir = config.BRONZE / "sp156" / "_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    recursos = _recursos_sp156_api(ano)
    log.info("API do portal: %d arquivo(s) de %d: %s", len(recursos), ano, [r["nome"] for r in recursos])
    caminhos = []
    for r in recursos:
        destino = cache_dir / r["url"].rsplit("/", 1)[-1]
        tam = r.get("size")
        if destino.exists() and (not tam or destino.stat().st_size == int(tam)):
            try:
                validar_csv_sp156(destino)
                log.info("SP156 já em cache (%s) — download pulado", destino.name)
                caminhos.append(destino)
                continue
            except ValueError:
                destino.unlink(missing_ok=True)
        log.info("baixando %s", r["url"])
        _http_download(r["url"], destino)
        try:
            validar_csv_sp156(destino)
        except ValueError:
            destino.unlink(missing_ok=True)
            raise
        caminhos.append(destino)
    return caminhos, f"API do portal (CKAN): {config.SP156_CKAN_PACKAGE_URL}"


def ingerir_sp156(ds: str) -> dict:
    """Coloca o(s) CSV(s) do SP156 em bronze/sp156/dt=ds/sp156_raw_<n>.csv (+ manifest com hash de cada arquivo).

    Origem (SP156_MODE=sample usa a amostra sintética de testes). No modo real, SP156_FONTE decide:
      auto (padrão) -> API do portal (CKAN) e, se falhar, os CSVs de SP156_LOCAL_PATH (fallback);
      api -> só a API;  local -> só SP156_LOCAL_PATH (ou SP156_URL, se não houver caminho local).
    A fonte realmente usada e o erro da API (se houve) ficam no _manifest.json.
    """
    pasta = _particao("sp156", ds)
    fonte_usada, erro_api = "sample" if config.SP156_MODE == "sample" else "url", None
    for velho in pasta.glob("sp156_raw*.csv"):  # reexecução da mesma data não deixa arquivo antigo para trás
        velho.unlink()

    if config.SP156_MODE == "sample":
        origens = [config.SAMPLE_DIR / "sp156_amostra.csv"]
        descricao = "amostra SINTÉTICA local"
    elif config.SP156_FONTE in ("auto", "api"):
        try:
            origens, descricao = _baixar_sp156_api(ds)
            fonte_usada = "api"
        except Exception as e:  # rede fora, portal mudou, HTML no lugar do CSV...
            erro_api = f"{type(e).__name__}: {e}"
            if config.SP156_FONTE == "api" or not config.SP156_LOCAL_PATH:
                raise
            log.warning("API do portal indisponível (%s) — usando CSVs locais (fallback)", erro_api)
            origens = _arquivos_locais()
            descricao = "arquivos locais (fallback: API indisponível)"
            fonte_usada = "local"
    elif config.SP156_LOCAL_PATH:
        origens = _arquivos_locais()
        descricao = "arquivos locais"
        fonte_usada = "local"
    else:
        cache = config.BRONZE / "sp156" / "_cache" / (sha256(config.SP156_URL)[:16] + ".csv")
        cache.parent.mkdir(parents=True, exist_ok=True)
        if not cache.exists():
            log.info("baixando SP156 de %s", config.SP156_URL)
            tmp = cache.with_suffix(".part")
            req = urllib.request.Request(config.SP156_URL, headers={"User-Agent": "cidadeviva-pipeline/1.0"})
            with urllib.request.urlopen(req, timeout=600) as r, open(tmp, "wb") as f:
                shutil.copyfileobj(r, f, length=1 << 20)
            try:
                validar_csv_sp156(tmp)
            except ValueError:
                tmp.unlink(missing_ok=True)
                raise
            tmp.rename(cache)
        else:
            log.info("SP156 já em cache (%s) — download pulado", cache.name)
        origens = [cache]
        descricao = config.SP156_URL

    arquivos, vistos = [], set()
    for origem in origens:
        if config.SP156_MODE != "sample":
            validar_csv_sp156(origem)
        h = _hash_arquivo(origem)
        if h in vistos:
            log.warning("SP156: %s é idêntico a outro arquivo já lido — ignorado", origem.name)
            continue
        vistos.add(h)
        destino = pasta / f"sp156_raw_{len(arquivos) + 1}.csv"
        shutil.copyfile(origem, destino)
        arquivos.append({"origem": str(origem), "arquivo": destino.name, "bytes": destino.stat().st_size, "sha256": h})
        log.info("SP156 bronze: %s <- %s (%.1f MB)", destino.name, origem.name, destino.stat().st_size / 1e6)

    manifest = {
        "fonte": "SP156 - Portal de Dados Abertos PMSP (CC0)",
        "origem": f"{descricao}: " + "; ".join(a["origem"] for a in arquivos),
        "modo": "sample" if config.SP156_MODE == "sample" else "real",
        "fonte_usada": fonte_usada,
        "erro_api": erro_api,
        "arquivos": arquivos,
        "bytes": sum(a["bytes"] for a in arquivos),
        "ingerido_em": datetime.now(timezone.utc).isoformat(),
    }
    gravar_json(manifest, pasta / "_manifest.json")
    return manifest


# --------------------------------------------------------------------------- CidadeViva

_TEXTOS = {
    "pavimento": [
        "Buraco enorme no meio da rua, carros desviando pela contramão",
        "Asfalto afundou perto do ponto de ônibus, já tem água parada",
        "Cratera na esquina, moto caiu ontem à noite",
        "Remendo do tapa-buraco abriu de novo depois da chuva",
    ],
    "iluminacao": [
        "Poste com lâmpada apagada há duas semanas, rua muito escura",
        "Lâmpada do poste fica acesa o dia inteiro",
        "Três postes seguidos sem luz perto da escola",
        "Fiação do poste pendurada e lâmpada piscando",
    ],
    "limpeza_urbana": [
        "Descarte irregular de entulho e sofá na calçada",
        "Lixo acumulado na esquina há dias, cheiro forte e ratos",
        "Caçamba abandonada transbordando de entulho",
        "Coleta de lixo não passou essa semana na rua toda",
    ],
    "arvores_areas_verdes": [
        "Árvore com galho quebrado ameaçando cair sobre a fiação",
        "Mato alto na praça, precisa de roçada",
        "Árvore caída bloqueando parte da calçada depois do temporal",
        "Raiz da árvore levantou o piso e os galhos encostam no poste",
    ],
    "calcadas_acessibilidade": [
        "Calçada toda quebrada, cadeirante não consegue passar",
        "Falta rampa de acessibilidade na travessia em frente ao posto de saúde",
        "Piso tátil arrancado e buracos na calçada da estação",
        "Calçada estreita com degrau alto, idosos tropeçam",
    ],
    "sinalizacao_semaforo": [
        "Semáforo de pedestre não abre, botão quebrado",
        "Semáforo apagado no cruzamento, risco de acidente",
        "Botoeira do semáforo não funciona, pedestres atravessam no vermelho",
        "Faixa de pedestre apagada em frente à escola",
        "Placa de pare caída na esquina",
    ],
    "drenagem": [
        "Boca de lobo entupida, rua alaga com qualquer chuva",
        "Bueiro sem tampa no meio da calçada",
        "Alagamento constante na baixada, água entra nas casas",
        "Córrego transbordando e lixo travando a galeria",
    ],
}
_LOGRADOUROS = [
    "Rua Augusta", "Avenida Paulista", "Rua Vergueiro", "Avenida Celso Garcia", "Rua Teodoro Sampaio",
    "Avenida Sapopemba", "Estrada do M'Boi Mirim", "Avenida Inajar de Souza", "Rua Domingos de Morais",
    "Avenida Jabaquara", "Rua Clélia", "Avenida Itaquera", "Rua Voluntários da Pátria", "Avenida Robert Kennedy",
    "Rua Cachoeira", "Avenida Marechal Tito", "Rua Taquari", "Avenida Ragueb Chohfi", "Rua Heitor Penteado",
]
_NOMES = ["Ana Souza", "Carlos Lima", "Mariana Alves", "João Pereira", "Fernanda Rocha", "Lucas Martins", "Beatriz Costa"]


def _simular_reportes(ds: str, n: int) -> list[dict]:
    """Gera um lote realista do que a API do app exportaria — inclusive com sujeira proposital
    (duplicados, coordenadas fora de SP, CEP com hífen, PII no texto) para exercitar o tratamento."""
    rnd = random.Random(f"cidadeviva-{ds}")
    subs = pd.read_csv(config.SEEDS / "subprefeituras.csv")
    base = datetime.fromisoformat(ds)
    cats = list(_TEXTOS)
    pesos = [22, 14, 20, 12, 10, 12, 10]
    reportes = []
    for i in range(n):
        cat = rnd.choices(cats, pesos)[0]
        sub = subs.sample(1, random_state=rnd.randint(0, 10**6)).iloc[0]
        criado = base + timedelta(seconds=rnd.randint(0, 86399))
        texto = rnd.choice(_TEXTOS[cat])
        if rnd.random() < 0.06:
            texto += f". Me liguem no (11) 9{rnd.randint(1000, 9999)}-{rnd.randint(1000, 9999)}"
        confianca = round(rnd.uniform(0.45, 0.99), 3)
        cat_ia = cat if confianca > 0.55 or rnd.random() < 0.5 else rnd.choice(cats)
        lat = float(sub.centroide_lat_aprox) + rnd.uniform(-0.02, 0.02)
        lon = float(sub.centroide_lon_aprox) + rnd.uniform(-0.02, 0.02)
        if rnd.random() < 0.03:
            lat, lon = None, None
        elif rnd.random() < 0.02:
            lat, lon = lon, lat  # erro clássico do front-end: lat/lon trocados
        nome = rnd.choice(_NOMES)
        status = rnd.choices(["novo", "triado", "enviado_prefeitura", "resolvido"], [25, 35, 30, 10])[0]
        protocolo = f"2026{rnd.randint(10**7, 10**8 - 1)}" if status in ("enviado_prefeitura", "resolvido") else None
        reportes.append(
            {
                "id": f"cv-{ds.replace('-', '')}-{i:05d}",
                "created_at": criado.isoformat(),
                "canal": rnd.choices(["app", "web", "whatsapp"], [60, 25, 15])[0],
                "descricao": texto,
                "foto_url": f"https://storage.cidadeviva.app/fotos/{ds}/{i:05d}.jpg" if rnd.random() < 0.8 else None,
                "endereco": {
                    "logradouro": rnd.choice(_LOGRADOUROS),
                    "numero": str(rnd.randint(1, 3500)),
                    "cep": f"0{rnd.randint(1000, 8499)}-{rnd.randint(0, 999):03d}" if rnd.random() < 0.9 else None,
                    "subprefeitura": sub.nome if rnd.random() < 0.9 else str(sub.nome).upper(),
                    "lat": lat,
                    "lon": lon,
                },
                "ia": {"categoria": cat_ia.upper() if rnd.random() < 0.1 else cat_ia,
                       "confianca": confianca if rnd.random() > 0.02 else None,
                       "modelo": "cidadeviva-triagem-v1"},
                "status": status,
                "protocolo_prefeitura": protocolo,
                "usuario": {"id": f"u{rnd.randint(1, 900):04d}", "nome": nome,
                            "email": nome.lower().replace(" ", ".") + "@email.com"},
            }
        )
    reportes += rnd.sample(reportes, k=max(1, n // 50))  # duplicados de reenvio do app
    return reportes


def ingerir_cidadeviva(ds: str) -> dict:
    destino = _particao("cidadeviva", ds) / "reportes.json"
    if config.CIDADEVIVA_API_URL:
        import json

        url = f"{config.CIDADEVIVA_API_URL.rstrip('/')}/reportes/export?desde={ds}&ate={ds}"
        log.info("lendo reportes da API do app: %s", url)
        req = urllib.request.Request(url, headers={"X-API-Key": os.getenv("CIDADEVIVA_API_KEY", "")})
        with urllib.request.urlopen(req, timeout=60) as r:
            reportes = json.loads(r.read().decode("utf-8"))
        origem = url
    else:
        reportes = _simular_reportes(ds, config.CIDADEVIVA_SIM_N)
        origem = "simulador (CIDADEVIVA_API_URL não definido)"
    gravar_json(reportes, destino)
    manifest = {"fonte": "App CidadeViva", "origem": origem, "registros": len(reportes),
                "sha256": _hash_arquivo(destino), "ingerido_em": datetime.now(timezone.utc).isoformat()}
    gravar_json(manifest, destino.parent / "_manifest.json")
    log.info("CidadeViva bronze: %d reportes -> %s", len(reportes), destino)
    return manifest


# --------------------------------------------------------------------------- Referências

def ingerir_referencias(ds: str) -> dict:
    destino = _particao("referencias", ds)
    arquivos = []
    for f in sorted(config.SEEDS.glob("*.csv")):
        shutil.copyfile(f, destino / f.name)
        arquivos.append(f.name)
    gravar_json({"arquivos": arquivos, "ingerido_em": datetime.now(timezone.utc).isoformat()},
                destino / "_manifest.json")
    log.info("referências bronze: %s", arquivos)
    return {"arquivos": arquivos}