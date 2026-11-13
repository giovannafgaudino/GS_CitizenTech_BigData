"""Funções utilitárias: normalização de texto, hashing, anonimização e I/O do data lake."""
from __future__ import annotations

import hashlib
import json
import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd

log = logging.getLogger("cidadeviva")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

try:  # Parquet quando disponível (imagem Docker); CSV comprimido como fallback
    import pyarrow  # noqa: F401
    HAS_PARQUET = True
except ImportError:  # pragma: no cover
    HAS_PARQUET = False


def normalizar(texto) -> str:
    """minúsculas, sem acento, só [a-z0-9 ], espaços colapsados."""
    if texto is None or (isinstance(texto, float) and pd.isna(texto)):
        return ""
    t = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode()
    t = re.sub(r"[^a-z0-9]+", " ", t.lower())
    return re.sub(r"\s+", " ", t).strip()


def chave_subprefeitura(nome) -> str:
    """Chave de comparação sem acento/pontuação/espaço: 'M'BOI MIRIM', 'M BOI MIRIM' e 'MBOI MIRIM' -> 'mboimirim';
    'CASA VERDE-CACHOEIRINHA' e 'Casa Verde/Cachoeirinha' -> 'casaverdecachoeirinha'."""
    return normalizar(nome).replace(" ", "") if nome is not None else ""


def sha256(*partes, salt: str = "") -> str:
    raw = salt + "|".join("" if p is None else str(p) for p in partes)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


_RE_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
_RE_CPF = re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b")
_RE_FONE = re.compile(r"(\(?\d{2}\)?\s?)?9?\d{4}[-\s]?\d{4}\b")


def mascarar_pii(texto: str) -> str:
    """Remove e-mail, CPF e telefone de textos livres antes de irem para silver/gold (LGPD)."""
    if not isinstance(texto, str):
        return texto
    texto = _RE_EMAIL.sub("[EMAIL]", texto)
    texto = _RE_CPF.sub("[CPF]", texto)
    return _RE_FONE.sub("[TELEFONE]", texto)


def normalizar_cep(cep) -> str | None:
    if cep is None or (isinstance(cep, float) and pd.isna(cep)):
        return None
    d = re.sub(r"\D", "", str(cep))
    if not d:
        return None
    d = d.zfill(8)
    return d if len(d) == 8 else None


def para_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype("string").str.replace(",", ".", regex=False).str.strip(), errors="coerce")


# ---------- I/O do data lake ----------

def gravar(df: pd.DataFrame, caminho_sem_ext: Path) -> Path:
    caminho_sem_ext.parent.mkdir(parents=True, exist_ok=True)
    if HAS_PARQUET:
        p = caminho_sem_ext.with_suffix(".parquet")
        df.to_parquet(p, index=False)
    else:
        p = caminho_sem_ext.with_suffix(".csv.gz")
        df.to_csv(p, index=False, compression="gzip")
    log.info("gravado %s (%d linhas)", p, len(df))
    return p


def ler(caminho_sem_ext: Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    pq = caminho_sem_ext.with_suffix(".parquet")
    if pq.exists():
        return pd.read_parquet(pq)
    gz = caminho_sem_ext.with_suffix(".csv.gz")
    if gz.exists():
        df = pd.read_csv(gz, parse_dates=parse_dates or [], dtype={"cep": "string", "numero": "string", "protocolo_prefeitura": "string"})
        for c in df.columns:  # CSV perde o tipo inteiro quando há nulos -> restaura chaves como Int64
            if (c.endswith("_id") or c == "hora_abertura") and pd.api.types.is_float_dtype(df[c]):
                df[c] = df[c].astype("Int64")
        return df
    raise FileNotFoundError(f"Nenhum arquivo para {caminho_sem_ext} (.parquet/.csv.gz)")


def gravar_json(obj, caminho: Path) -> Path:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(json.dumps(obj, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    return caminho
