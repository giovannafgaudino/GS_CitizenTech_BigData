import sqlite3
import pandas as pd


def q(db, sql):
    with sqlite3.connect(db) as c:
        return pd.read_sql(sql, c)


def test_tabelas_carregadas(lake):
    _, db = lake
    for t, minimo in [("fato_solicitacao_sp156", 1000), ("fato_reporte", 200), ("dim_subprefeitura", 32), ("dim_categoria", 7)]:
        assert q(db, f"select count(*) n from {t}").n[0] >= minimo, t


def test_nao_zeladoria_foi_descartada(lake):
    _, db = lake
    cats = q(db, "select nome from dim_categoria").nome.tolist()
    assert "IPTU" not in " ".join(cats)
    # nenhum serviço de IPTU/ruído/cadastro chegou ao fato
    n = q(db, "select count(*) n from fato_solicitacao_sp156 where servico like '%IPTU%' or servico like '%cadastro%' or servico like '%sonora%'").n[0]
    assert n == 0


def test_sem_duplicidade_e_sem_pii(lake):
    _, db = lake
    assert q(db, "select count(*)-count(distinct reporte_id) d from fato_reporte").d[0] == 0
    df = q(db, "select descricao from fato_reporte")
    assert not df.descricao.str.contains("@|\\(11\\)", regex=True).any()
    cols = set(q(db, "select * from fato_reporte limit 1").columns)
    assert not ({"nome", "email", "usuario_id"} & cols)


def test_fk_subprefeitura(lake):
    _, db = lake
    orf = q(db, """select count(*) n from fato_reporte f left join dim_subprefeitura d using(subprefeitura_id)
                   where f.subprefeitura_id is not null and d.nome_subprefeitura is null""").n[0]
    assert orf == 0


def test_revisao_humana_coerente(lake):
    _, db = lake
    bad = q(db, "select count(*) n from fato_reporte where confianca_ia < 0.70 and requer_revisao_humana = 0").n[0]
    assert bad == 0


def test_idempotencia(lake):
    ds, db = lake
    import run_local
    antes = q(db, "select (select count(*) from fato_reporte) r, (select count(*) from fato_solicitacao_sp156) s, "
                  "(select count(*) from mart_demanda_subprefeitura_mes) m")
    run_local.rodar(ds)  # segunda execução da mesma data lógica
    depois = q(db, "select (select count(*) from fato_reporte) r, (select count(*) from fato_solicitacao_sp156) s, "
                   "(select count(*) from mart_demanda_subprefeitura_mes) m")
    assert antes.equals(depois)


def test_csv_em_cp1252_e_arquivo_local(tmp_path, monkeypatch):
    """O CSV oficial pode vir em cp1252; o modo 'real' com SP156_LOCAL_PATH deve lê-lo sem corromper acentos."""
    import importlib, os, pathlib, sys
    root = pathlib.Path(__file__).resolve().parents[1]
    src = (root / "data/sample/sp156_amostra.csv").read_text(encoding="utf-8")
    arq = tmp_path / "sp156_cp1252.csv"
    arq.write_bytes(src.encode("cp1252"))
    for k, v in dict(DATA_DIR=str(tmp_path / "lake"), SP156_MODE="real", SP156_LOCAL_PATH=str(arq),
                     WAREHOUSE_URL=f"sqlite:///{tmp_path/'dw.sqlite'}").items():
        monkeypatch.setenv(k, v)
    for m in [m for m in sys.modules if m.startswith("pipeline")]:
        del sys.modules[m]
    from pipeline import ingest, transform
    ingest.ingerir_sp156("2026-11-13"); ingest.ingerir_referencias("2026-11-13")
    st = transform.silver_sp156("2026-11-13")
    from pipeline.utils import ler
    from pipeline import config
    df = ler(config.SILVER / "sp156" / "dt=2026-11-13" / "solicitacoes")
    assert st["linhas_silver"] > 1000
    assert df.servico.str.contains("Lâmpada|Semáforo|Calçada", regex=True).any()  # acentos preservados
    assert not df.servico.str.contains("�|Ã", regex=True).any()


# --------------------------------------------------------------------------------------------
# Layout REAL de 2026 (cabeçalho e as 2 primeiras linhas de cada arquivo foram copiados do terminal do grupo;
# as linhas marcadas "extra" foram acrescentadas para exercitar a classificação).
#   tri 1 e 2: ';' , cp1252 , datas dd/mm/aaaa      tri 3: ',' , UTF-8 , datas mm/dd/aaaa
CAB = "Data de Abertura;Data de Finalização;Meio de Comunicação;Tema;Assunto;Serviço;Status;Órgão;Logradouro;Número;CEP;Latitude;Longitude;Prefeitura Operacional;Distrito"
Q1 = [
    "01/01/2026;01/01/2026;CRM;Cidadania e assistência social;População ou pessoa em situação de rua;Baixas Temperaturas - Solicitar abordagem social para pessoas em situação de rua;Finalizada;SMADS-SEAS República regional;Avenida Paulista;542;01310000;-23.5677346;-46.6483768;SE;BELA VISTA",
    "01/01/2026;06/01/2026;PORTAL;Animais;Castração;Castrar Cães e Gatos Gratuitamente;Finalizada;SMS-DVZ-COSAP-Mboi Mirim;;;;;;;",
    "15/01/2026;20/01/2026;PORTAL;Rua e bairro;Reparos em asfalto, pontes e viadutos;Tapa-buraco - solicitar reparo;Finalizada;SMSUB;Rua Augusta;100;01305000;-23.5530;-46.6560;SE;CONSOLACAO",  # extra
    "20/02/2026;;APLICATIVO;Rua e bairro;Iluminação pública;Lâmpada apagada - solicitar reparo;Aberta;ILUME;Avenida Sapopemba;2420;03345000;-23.6000;-46.5100;SAPOPEMBA;SAPOPEMBA",  # extra
]
Q2 = [
    "01/04/2026;02/04/2026;CRM;Ouvidoria Geral do Município;Ouvidoria Geral do Município;Reclamação Anônima (Uso exclusivo da Ouvidoria);Finalizada;OGM - SMS;Rua Doutor Siqueira Campos;172;01509020;-23.5637222;-46.63729379999999;SE;LIBERDADE",
    "01/04/2026;02/04/2026;PORTAL;Rua e bairro;Drenagem de água de chuva;Reformar galerias de águas de chuvas (pluviais);Finalizada;INTEGRAÇÃO-SMGI/SGZ;Rua dos Pinheiros;14;04966050;-23.7269671;-46.7809829;M BOI MIRIM;JARDIM ANGELA",
    "05/05/2026;06/05/2026;CRM;Trânsito e transporte;Semáforos;Semáforo - solicitar manutenção;Finalizada;CET;Avenida Jabaquara;1000;04045000;-23.6460;-46.6410;JABAQUARA;JABAQUARA",  # extra
]
Q3 = [
    "07/01/2026,07/25/2026,PORTAL,Animais,Registro de animais - RGA,Registro Geral do Animal (RGA) - Solicitar identidade do animal,Finalizada,SMS-DVZ-COSAP-Ermelino Matarazzo,,,,,,,",
    "07/01/2026,07/25/2026,PORTAL,Animais,Castração,Castrar Cães e Gatos Gratuitamente,Finalizada,SMS-DVZ-COSAP-Ermelino Matarazzo,,,,,,,",
    '07/15/2026,07/20/2026,PORTAL,Rua e bairro,"Reparos em asfalto, pontes e viadutos",Tapa-buraco - solicitar reparo,Finalizada,SMSUB,Rua Cachoeira,50,03018000,-23.5400,-46.5800,MOOCA,MOOCA',  # extra
]


def test_layout_real_2026(tmp_path, monkeypatch):
    import sys
    pasta = tmp_path / "raw"
    pasta.mkdir()
    (pasta / "arquivo-final-1-trim-26.csv").write_bytes("\r\n".join([CAB] + Q1).encode("cp1252"))
    (pasta / "arquivo-final-2-trim-26.csv").write_bytes("\r\n".join([CAB] + Q2).encode("cp1252"))
    (pasta / "arquivo-final-3-trim-26.csv").write_bytes("\r\n".join([CAB.replace(";", ",")] + Q3).encode("utf-8"))
    for k, v in dict(DATA_DIR=str(tmp_path / "lake"), SP156_MODE="real", SP156_LOCAL_PATH=str(pasta / "arquivo-final-*-trim-26.csv"),
                     SP156_MAX_ROWS="", WAREHOUSE_URL=f"sqlite:///{tmp_path/'dw.sqlite'}").items():
        monkeypatch.setenv(k, v)
    for m in [m for m in sys.modules if m.startswith("pipeline")]:
        del sys.modules[m]
    import json
    from pipeline import config, ingest, transform
    from pipeline.utils import ler
    ingest.ingerir_sp156("2026-11-13"); ingest.ingerir_referencias("2026-11-13")
    st = transform.silver_sp156("2026-11-13")
    df = ler(config.SILVER / "sp156" / "dt=2026-11-13" / "solicitacoes", parse_dates=["data_abertura", "data_parecer"])

    assert st["linhas_lidas"] == len(Q1) + len(Q2) + len(Q3)
    assert st["linhas_silver"] == 5          # só as 5 de zeladoria; castração/ouvidoria/rua... ficam de fora
    ids = df.id_origem
    assert ids.is_unique                      # linhas sem endereço no mesmo dia NÃO podem se fundir
    fmts = json.loads((config.SILVER / "sp156" / "dt=2026-11-13" / "_stats.json").read_text())["arquivos"]
    assert [v["formato_data"] for v in fmts.values()] == ["%d/%m/%Y", "%d/%m/%Y", "%m/%d/%Y"]

    por = df.set_index("servico")
    assert por.loc["Semáforo - solicitar manutenção", "data_abertura"] == pd.Timestamp("2026-05-05")   # dd/mm
    tri3 = df[df.orgao == "SMSUB"].sort_values("data_abertura").iloc[-1]
    assert tri3.data_abertura == pd.Timestamp("2026-07-15")                                           # mm/dd !
    assert tri3.tempo_resposta_horas == 5 * 24
    # Prefeitura Operacional -> subprefeitura (M BOI MIRIM sem apóstrofo, SE, MOOCA, JABAQUARA, SAPOPEMBA)
    assert df.subprefeitura_id.notna().all()
    assert set(df.subprefeitura_id) == {29, 28, 17, 14, 18}
    assert "Lâmpada apagada - solicitar reparo" in set(df.servico)                                     # acentos ok
    assert df.latitude.between(-24.01, -23.35).all()
