"""Gera docs/CidadeViva_Arquitetura_e_Dicionario_de_Dados.pdf a partir do DW já carregado.
    SP156_MODE=sample python scripts/run_local.py --ds 2026-11-12 && python docs/gerar_pdf.py
"""
import json
import sqlite3
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (BaseDocTemplate, Frame, Image, NextPageTemplate, PageBreak, PageTemplate, Paragraph,
                                Spacer, Table, TableStyle)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "CidadeViva_Arquitetura_e_Dicionario_de_Dados.pdf"

ss = getSampleStyleSheet()
H1 = ParagraphStyle("h1", parent=ss["Heading1"], fontSize=17, spaceAfter=8, textColor=colors.HexColor("#0F172A"))
H2 = ParagraphStyle("h2", parent=ss["Heading2"], fontSize=12.5, spaceBefore=10, spaceAfter=4, textColor=colors.HexColor("#1E3A8A"))
B = ParagraphStyle("b", parent=ss["BodyText"], fontSize=9.5, leading=13)
SM = ParagraphStyle("sm", parent=B, fontSize=8, leading=10)
MONO = ParagraphStyle("mono", parent=SM, fontName="Courier", fontSize=7, leading=8.6)

# --------------------------------------------------------------------- dicionário (descrições)
# (coluna: (descrição, origem/regra))
DIC = {
 "dim_data": ("Calendário. Uma linha por dia entre a menor e a maior data presentes nos fatos.", {
  "data_id": ("Chave no formato AAAAMMDD.", "gerada"), "data": ("Data civil.", "gerada"), "ano": ("Ano.", "gerada"),
  "mes": ("Mês (1–12).", "gerada"), "dia": ("Dia do mês.", "gerada"), "trimestre": ("Trimestre (1–4).", "gerada"),
  "dia_semana": ("1 = segunda … 7 = domingo.", "gerada"), "fim_de_semana": ("Verdadeiro para sábado/domingo.", "gerada")}),
 "dim_subprefeitura": ("As 32 subprefeituras de São Paulo.", {
  "subprefeitura_id": ("Chave substituta.", "seeds/subprefeituras.csv"), "nome_subprefeitura": ("Nome padronizado.", "seed"),
  "zona": ("Zona da cidade (Norte, Sul, Leste, Oeste, Centro).", "seed"),
  "centroide_lat": ("Latitude aproximada do centro (didática; não é limite oficial).", "seed"),
  "centroide_lon": ("Longitude aproximada do centro.", "seed")}),
 "dim_categoria": ("Taxonomia CidadeViva de problemas urbanos (7 categorias). Une SP156 e reportes.", {
  "categoria_id": ("Chave substituta.", "seeds/categorias.csv"), "codigo": ("Código estável (slug).", "seed"),
  "nome": ("Nome exibido ao cidadão.", "seed"), "descricao": ("O que a categoria cobre.", "seed"),
  "orgao_sugerido": ("Órgão normalmente responsável.", "seed"),
  "prioridade_base": ("Gravidade base 1–4 (4 = risco imediato).", "seed")}),
 "fato_solicitacao_sp156": ("Uma linha por solicitação de zeladoria registrada nos canais SP156 (histórico real da Prefeitura).", {
  "solicitacao_id": ("Identificador sintético 'arquivo:linha' (o CSV oficial não traz protocolo; solicitações iguais no mesmo dia continuam separadas).", "silver_sp156"),
  "data_id": ("FK dim_data (data de abertura).", "Data de abertura"), "hora_abertura": ("Hora de abertura (0–23). Nula nos CSVs de 2026, que trazem só a data.", "Data de Abertura"),
  "subprefeitura_id": ("FK dim_subprefeitura. Nulo se a solicitação não trouxe endereço.", "Subprefeitura (2024) / Prefeitura Operacional (2026) + normalização"),
  "categoria_id": ("FK dim_categoria, atribuída por regras sobre Tema+Assunto+Serviço.", "seeds/regras_categoria_sp156.csv"),
  "canal": ("Canal de entrada (portal, central, aplicativo, CRM).", "Canal / Meio de Comunicação"), "servico": ("Serviço solicitado.", "Serviço"),
  "logradouro": ("Rua/avenida em caixa de título.", "Logradouro"), "cep": ("CEP com 8 dígitos.", "CEP"),
  "latitude": ("Graus decimais; nulo se fora do município de SP.", "Latitude + bbox"),
  "longitude": ("Graus decimais; nulo se fora do município de SP.", "Longitude + bbox"),
  "status": ("aberto · em_andamento · finalizado · indeferido · cancelado.", "Status da solicitação / Status"),
  "tempo_resposta_horas": ("Horas entre abertura e parecer; nulo se negativo ou sem parecer.", "Data do Parecer / Data de Finalização − abertura"),
  "nivel_satisfacao": ("Nota da pesquisa de satisfação, quando existe na fonte (ausente nos CSVs de 2026).", "Nível Satisfação"),
  "dt_ingestao": ("Data lógica da execução (partição).", "Airflow ds")}),
 "fato_reporte": ("Uma linha por reporte feito por cidadãos no app CidadeViva. Sem dados pessoais diretos (LGPD).", {
  "reporte_id": ("Identificador do reporte no app.", "id"), "data_id": ("FK dim_data.", "created_at"),
  "criado_em": ("Timestamp de criação.", "created_at"),
  "subprefeitura_id": ("FK dim_subprefeitura.", "endereco.subprefeitura"),
  "categoria_id": ("FK dim_categoria — categoria sugerida pela IA (nula se inválida).", "ia.categoria"),
  "canal": ("app · web · whatsapp.", "canal"), "descricao": ("Texto do cidadão com telefone/CPF/e-mail mascarados.", "descricao + mascarar_pii"),
  "possui_foto": ("Indica se há foto anexada.", "foto_url"),
  "latitude": ("Graus decimais validados; lat/lon invertidos são corrigidos.", "endereco.lat"),
  "longitude": ("Graus decimais validados.", "endereco.lon"),
  "confianca_ia": ("Confiança do modelo de triagem (0–1); nula se ausente.", "ia.confianca"),
  "requer_revisao_humana": ("Verdadeiro se confiança < 0,70, ausente ou categoria inválida (human-in-the-loop).", "regra de negócio"),
  "status": ("aberto · em_andamento · finalizado.", "status (app) padronizado"),
  "protocolo_prefeitura": ("Protocolo devolvido pelo portal da Prefeitura após o envio do RPA.", "protocolo_prefeitura"),
  "usuario_hash": ("SHA-256(salt + id do usuário). Permite contar usuários sem identificá-los.", "usuario.id"),
  "dt_ingestao": ("Data lógica da execução (partição).", "Airflow ds")}),
 "mart_demanda_subprefeitura_mes": ("Demanda histórica por mês × subprefeitura × categoria (base do mapa de calor).", {
  "ano_mes": ("AAAA-MM.", "fato_solicitacao_sp156"), "subprefeitura_id": ("FK dim_subprefeitura.", "agregação"),
  "categoria_id": ("FK dim_categoria.", "agregação"), "qtd_solicitacoes": ("Total de solicitações.", "COUNT"),
  "qtd_finalizadas": ("Solicitações com status finalizado.", "COUNT"), "taxa_resolucao": ("finalizadas ÷ total (0–1).", "calculado"),
  "tempo_medio_resposta_horas": ("Média do tempo de resposta.", "AVG"), "dt_ingestao": ("Data lógica da execução.", "Airflow ds")}),
 "mart_triagem_ia": ("Qualidade diária da triagem por IA por categoria (monitoramento do modelo).", {
  "data": ("Dia dos reportes.", "fato_reporte"), "categoria_id": ("FK dim_categoria (nula = IA sem categoria válida).", "agregação"),
  "qtd_reportes": ("Reportes no dia/categoria.", "COUNT"), "confianca_media": ("Média da confiança da IA.", "AVG"),
  "qtd_revisao_humana": ("Reportes que foram para revisão humana.", "SUM"),
  "pct_revisao_humana": ("qtd_revisao_humana ÷ qtd_reportes.", "calculado"), "dt_ingestao": ("Data lógica da execução.", "Airflow ds")}),
}
TIPO = {"INTEGER": "inteiro", "TEXT": "texto", "REAL": "decimal"}
# tipos lógicos (o SQLite mostra INTEGER para booleanos; no Postgres viram boolean/timestamp)
LOGICO = {"fim_de_semana": "booleano", "possui_foto": "booleano", "requer_revisao_humana": "booleano",
          "criado_em": "timestamp", "data": "data", "dt_ingestao": "data"}


def p(t, s=B):
    return Paragraph(t, s)


def tabela(dados, larguras, cab=colors.HexColor("#1E3A8A"), fonte=7.6):
    t = Table(dados, colWidths=larguras, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), cab), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), fonte),
        ("VALIGN", (0, 0), (-1, -1), "TOP"), ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#CBD5E1")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F1F5F9")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3)]))
    return t


def rodape(canvas, doc):
    canvas.saveState(); canvas.setFont("Helvetica", 7.5); canvas.setFillColor(colors.HexColor("#64748B"))
    w, _ = canvas._pagesize
    canvas.drawString(1.5 * cm, 0.9 * cm, "FIAP · Global Solution 2026.2 · CitizenTech · CidadeViva — Big Data Architecture & Data Integration")
    canvas.drawRightString(w - 1.5 * cm, 0.9 * cm, f"pág. {doc.page}"); canvas.restoreState()


def main():
    DS = sorted(d.name.split("=")[1] for d in (ROOT / "data" / "gold").glob("dt=*"))[-1]  # execução mais recente
    man = json.loads((ROOT / f"data/bronze/sp156/dt={DS}/_manifest.json").read_text())
    real = man["modo"] == "real"
    con = sqlite3.connect(ROOT / "data" / "cidadeviva_dw.sqlite")
    contagens = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in DIC}
    sp_stats = json.loads((ROOT / f"data/silver/sp156/dt={DS}/_stats.json").read_text())
    cv_stats = json.loads((ROOT / f"data/silver/cidadeviva/dt={DS}/_stats.json").read_text())
    dq = json.loads((ROOT / f"data/gold/dt={DS}/_dq_report.json").read_text())

    P, L = A4, landscape(A4)
    doc = BaseDocTemplate(str(OUT), pagesize=P, leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.5 * cm, bottomMargin=1.6 * cm,
                          title="CidadeViva — Arquitetura e Dicionário de Dados", author="Grupo CitizenTech")
    fp = Frame(1.5 * cm, 1.6 * cm, P[0] - 3 * cm, P[1] - 3.1 * cm, id="p")
    fl = Frame(1.5 * cm, 1.6 * cm, L[0] - 3 * cm, L[1] - 3.1 * cm, id="l")
    doc.addPageTemplates([PageTemplate("retrato", [fp], pagesize=P, onPage=rodape),
                          PageTemplate("paisagem", [fl], pagesize=L, onPage=rodape)])
    s = []

    # ---------- 1. capa/visão geral
    s += [p("CidadeViva — Pipeline de Dados", H1),
          p("Big Data Architecture &amp; Data Integration · Global Solution FIAP 2026.2 · CitizenTech · Engenharia de Software 4º ano · Prof.ª Ana Machado", B),
          Spacer(1, 4),
          p("Grupo: Giovanna Franco Gaudino Rodrigues (RM553701) · Rafael de Almeida Sigoli (RM554019) · Rafael Jorge Del Padre (RM552765)", SM),
          Spacer(1, 8), p("1. Papel dos dados no MVP", H2),
          p("O CidadeViva é uma plataforma colaborativa de zeladoria urbana: o cidadão reporta um problema (buraco, lâmpada apagada, bueiro "
            "entupido, semáforo com defeito...), a IA classifica o reporte e um robô (RPA) o registra no portal da Prefeitura. O pipeline "
            "materializa o componente <b>Dados</b>: junta o <b>histórico real</b> de demanda do SP156 com os <b>reportes do app</b> e entrega, "
            "num Data Warehouse, tabelas prontas para (a) o mapa de calor e o ranking do MVP, (b) a fila de revisão humana da triagem por IA "
            "e (c) a priorização do envio automático pelo RPA."),
          p("2. Fontes de dados", H2)]
    s.append(tabela([
        ["Fonte", "Natureza", "Formato / volume", "Justificativa"],
        [p("SP156 — Portal de Dados Abertos da Prefeitura de SP", SM), p("Real, pública (CC0)", SM),
         p("CSV trimestral (≈100–120 MB). O layout mudou: 2024 tem 21 colunas; os de 2026 têm 15, com nomes novos, separador ';' ou ',', cp1252 ou UTF-8 e datas dd/mm ou mm/dd. O pipeline detecta tudo por arquivo.", SM),
         p("Dá ao produto base histórica de onde e o que mais se reclama, sem custo e sem dado pessoal.", SM)],
        [p("Reportes do app CidadeViva", SM), p("Simulada de forma justificada (a API do app ainda não tem volume real)", SM),
         p("JSON aninhado por reporte (texto, endereço, IA, usuário). Lote diário", SM),
         p("O simulador gera o mesmo contrato da API de exportação, com sujeira proposital (duplicados, lat/lon invertidos, PII). Ao definir CIDADEVIVA_API_URL o pipeline lê dados reais.", SM)],
        [p("Referências (seeds)", SM), p("Curadas pelo grupo", SM), p("3 CSVs pequenos", SM),
         p("Subprefeituras, taxonomia de 7 categorias e regras que ligam serviços SP156 à taxonomia.", SM)]],
        [3.6 * cm, 3.2 * cm, 5.2 * cm, 5.9 * cm]))
    s += [p("3. Decisões técnicas", H2),
          p("• <b>Arquitetura medallion</b> (bronze → silver → gold → DW): o bruto é preservado com manifest e hash; erros de regra são corrigíveis reprocessando.<br/>"
            "• <b>ETL em Python/pandas orquestrado por Airflow</b>: o volume (centenas de milhares de linhas por trimestre) cabe em processamento por chunks; "
            "troca-se por Spark sem mudar a DAG se o volume crescer.<br/>"
            "• <b>Idempotência</b>: toda task recebe só a data lógica; a carga apaga a partição do dia antes de inserir.<br/>"
            "• <b>Qualidade como gate</b>: 19 verificações bloqueiam a carga em caso de falha.<br/>"
            "• <b>LGPD e IA responsável</b>: nome/e-mail nunca chegam ao DW; confiança &lt; 0,70 vai para revisão humana.<br/>"
            "• <b>Robustez ao dado real</b>: separador, codificação e formato de data (dd/mm × mm/dd) são detectados por arquivo; as estatísticas listam serviços não classificados e subprefeituras não reconhecidas.<br/>"
          "• <b>Custo zero</b>: Airflow, PostgreSQL e Python open-source.", B)]

    # ---------- 2. arquitetura (paisagem)
    s += [PageBreak(), p("4. Diagrama de arquitetura", H1),
          Image(str(ROOT / "docs/arquitetura.png"), width=14.2 * cm, height=14.2 * cm * 2044 / 1711),
          Spacer(1, 6),
          p("<b>Orquestração.</b> A DAG <font face='Courier'>cidadeviva_pipeline</font> (agendada @daily, 10 tasks, 2 retentativas) executa as três ingestões em "
            "paralelo; cada silver espera sua ingestão e as referências; <font face='Courier'>construir_gold</font> espera as duas silvers; "
            "<font face='Courier'>validar_qualidade</font> bloqueia <font face='Courier'>carregar_dw</font> se qualquer verificação falhar. "
            "As tasks trocam dados pelo data lake (<font face='Courier'>data/bronze|silver|gold/dt=AAAA-MM-DD</font>), nunca por XCom.", B)]

    s += [PageBreak(), p("5. Modelo dimensional (camada gold / Data Warehouse)", H1),
          Image(str(ROOT / "docs/modelo_dimensional.png"), width=11.4 * cm, height=11.4 * cm * 976 / 946),
          p("Dois fatos (solicitações da Prefeitura e reportes do app) compartilham as dimensões de data, subprefeitura e categoria — por isso é possível "
            "cruzar o histórico oficial com o que o cidadão está reportando agora. Linhas tracejadas: marts agregados.", SM)]

    # ---------- 3. dicionário (retrato)
    s += [PageBreak(), p("6. Dicionário de dados", H1),
          p("Esquema lido diretamente do banco carregado. Tipos lógicos: o SQLite do modo local guarda booleanos como inteiro; no PostgreSQL (Docker) "
            "as colunas são criadas a partir dos mesmos DataFrames. Chaves terminadas em <i>_id</i> podem ser nulas nos fatos quando o dado de origem não permite resolver a dimensão.", SM)]
    for t, (desc, cols) in DIC.items():
        schema = [(r[1], r[2]) for r in con.execute(f"pragma table_info({t})")]
        faltando = set(c for c, _ in schema) ^ set(cols)
        assert not faltando, (t, faltando)  # dicionário tem de bater com o banco
        linhas = [["Coluna", "Tipo", "Descrição", "Origem / regra"]]
        for c, tp in schema:
            d, o = cols[c]
            linhas.append([p(f"<font face='Courier'>{c}</font>", SM), LOGICO.get(c, TIPO[tp]), p(d, SM), p(o, SM)])
        s += [p(f"<font face='Courier'>{t}</font> — {contagens[t]:,} linhas".replace(",", "."), H2), p(desc, SM), Spacer(1, 2),
              tabela(linhas, [4.4 * cm, 1.6 * cm, 7.6 * cm, 4.3 * cm])]

    # ---------- 4. qualidade e evidências
    s += [PageBreak(), p("7. Qualidade, resultados e evidências de execução", H1),
          p("Resultado da execução ponta a ponta (data lógica " + DS + ", " + ("SP156 REAL — " + str(len(man["arquivos"])) + " arquivos: " + ", ".join(Path(a["origem"]).name for a in man["arquivos"]) if real else "AMOSTRA SINTÉTICA do SP156") + " + lote simulado de reportes):", B), Spacer(1, 4)]
    s.append(tabela([
        ["Etapa", "Métrica", "Valor"],
        ["silver_sp156", "linhas lidas / fora de zeladoria / sem data válida / ao final",
         f"{sp_stats['linhas_lidas']} / {sp_stats['nao_zeladoria_descartadas']} / {sp_stats['sem_data_valida']} / {sp_stats['linhas_silver']}"],
        ["silver_sp156", "coordenadas inválidas anuladas · linhas sem subprefeitura",
         f"{sp_stats['coordenadas_invalidas_anuladas']} · {sp_stats['sem_subprefeitura']}"],
        ["silver_cidadeviva", "lidos / duplicados removidos / ao final",
         f"{cv_stats['linhas_lidas']} / {cv_stats['duplicadas_removidas']} / {cv_stats['linhas_silver']}"],
        ["silver_cidadeviva", "lat/lon invertidos corrigidos · enviados à revisão humana",
         f"{cv_stats['latlon_invertidos_corrigidos']} · {cv_stats['revisao_humana']}"],
        ["validar_qualidade", "verificações executadas / falhas", f"{dq['total']} / {dq['falhas']}"]],
        [3.3 * cm, 9.2 * cm, 5.4 * cm], fonte=8))
    nomes_cat = dict(con.execute("select codigo, nome from dim_categoria").fetchall())
    total_cat = sum(sp_stats["por_categoria"].values())
    linhas_cat = [["Categoria CidadeViva", "Solicitações SP156", "%"]] + [
        [nomes_cat.get(c, c), f"{n:,}".replace(",", "."), f"{n / total_cat:.1%}"]
        for c, n in sorted(sp_stats["por_categoria"].items(), key=lambda x: -x[1])]
    s += [Spacer(1, 6), p("Solicitações de zeladoria por categoria (após classificação):", H2),
          p("A classificação usa o <b>nome do serviço</b> do SP156 (regras em <font face='Courier'>seeds/regras_categoria_sp156.csv</font>, escritas a partir do catálogo "
            "real de serviços dos arquivos). Tema e assunto não entram na regra porque geravam falsos positivos (ex.: assunto 'Fiação e Postes' com serviço de guias e sarjetas). "
            "Serviços fora das 7 categorias (carteirinhas, situação de rua, castração, IPTU, ouvidoria, ônibus etc.) são descartados e contabilizados em "
            "<font face='Courier'>_stats.json</font> (<i>top_nao_classificados</i>).", SM),
          Spacer(1, 3), tabela(linhas_cat, [8.5 * cm, 4 * cm, 2.5 * cm], fonte=8)]
    if real:
        s.append(p("Observação: o SP156 de 2026 <b>não tem serviço de iluminação pública</b>; a categoria <i>Iluminação pública</i> só recebe do SP156 'poste em mau estado' e 'fiação solta'. "
                   "Ela continua na taxonomia porque o cidadão pode reportá-la pelo app.", SM))
    s += [Spacer(1, 6), p("Verificações de qualidade (gate):", H2)]
    s.append(tabela([["#", "Verificação", "Resultado"]] + [[str(i), c["check"], "OK" if c["ok"] else "FALHOU"] for i, c in enumerate(dq["checks"], 1)],
                    [0.8 * cm, 14.3 * cm, 2.8 * cm], fonte=7.6))
    s += [Spacer(1, 6), p("Carga no Data Warehouse (contagem de linhas por tabela após a execução):", H2),
          tabela([["Tabela", "Linhas"]] + [[t, f"{n:,}".replace(",", ".")] for t, n in contagens.items()], [9 * cm, 3 * cm], fonte=8),
          Spacer(1, 6), p("Idempotência: reexecutar a mesma data lógica mantém as contagens inalteradas (teste automatizado <font face='Courier'>test_idempotencia</font>; "
                          "11 testes pytest passam). Log completo em <font face='Courier'>docs/evidencia_execucao_local.log</font>.", B),
          p("8. Limitações assumidas", H2),
          p(("• O SP156 desta execução é o dado oficial da Prefeitura (CC0), jan–set/2026; os números descrevem esse dado.<br/>" if real else
             "• ATENÇÃO: esta execução usou amostra SINTÉTICA do SP156; os números NÃO descrevem a cidade. Regere com SP156_MODE=real.<br/>") +
            "• Os reportes do CidadeViva são simulados até haver volume real no app.<br/>"
            "• Os CSVs de 2026 trazem só a data (sem hora) e não têm nota de satisfação: <i>hora_abertura</i> e <i>nivel_satisfacao</i> ficam nulos; "
            "o tempo de resposta é em múltiplos de 24 h.<br/>"
            "• A classificação é lexical sobre o nome do serviço: é transparente e auditável, mas não é um modelo; serviços novos exigem atualizar as regras.<br/>"
            "• 'Status' e 'Data de Finalização' do SP156 refletem o fechamento administrativo, não necessariamente a resolução do problema na rua.<br/>"
            "• Os centroides das subprefeituras são aproximados.", B)]
    doc.build(s)
    print("PDF:", OUT)


if __name__ == "__main__":
    main()
