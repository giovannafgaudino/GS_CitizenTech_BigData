from pipeline.utils import chave_subprefeitura, mascarar_pii, normalizar_cep, normalizar, sha256


def test_normalizar():
    assert normalizar("  Poluição  Sonora - PSIU ") == "poluicao sonora psiu"


def test_chave_subprefeitura_une_grafias():
    assert chave_subprefeitura("CASA VERDE-CACHOEIRINHA") == chave_subprefeitura("Casa Verde/Cachoeirinha")
    assert chave_subprefeitura("M'BOI MIRIM") == chave_subprefeitura("M'Boi Mirim")


def test_mascarar_pii():
    t = mascarar_pii("Me liguem (11) 98765-4321 ou ana@email.com, CPF 123.456.789-09")
    assert "98765" not in t and "@" not in t and "123.456" not in t


def test_cep():
    assert normalizar_cep("05368-040") == "05368040"
    assert normalizar_cep("5368040") == "05368040"
    assert normalizar_cep(None) is None


def test_hash_estavel_e_com_salt():
    assert sha256("u1", salt="a") == sha256("u1", salt="a") != sha256("u1", salt="b")
