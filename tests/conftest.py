import os, sys
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def lake(tmp_path_factory):
    """Roda o pipeline completo (amostra) num diretório temporário e devolve (módulos, ds, caminho do DW)."""
    d = tmp_path_factory.mktemp("lake")
    os.environ.update(DATA_DIR=str(d), SP156_MODE="sample", WAREHOUSE_URL=f"sqlite:///{d/'dw.sqlite'}",
                      HASH_SALT="teste", CIDADEVIVA_SIM_N="300")
    for m in [m for m in sys.modules if m.startswith("pipeline") or m == "scripts"]:
        del sys.modules[m]
    sys.path.insert(0, str(ROOT / "scripts"))
    import run_local
    ds = "2026-11-12"
    run_local.rodar(ds)
    return ds, d / "dw.sqlite"
