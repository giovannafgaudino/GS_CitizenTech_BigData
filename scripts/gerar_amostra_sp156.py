"""Gera data/sample/sp156_amostra.csv: AMOSTRA SINTÉTICA com o mesmo esquema/cabeçalho do CSV oficial do SP156
(2024, separador ';'). Serve para rodar o pipeline offline. Os dados reais vêm de SP156_MODE=real."""
import csv, random
from datetime import datetime, timedelta
from pathlib import Path

rnd = random.Random(156)
HEADER = ["Data de abertura","Canal","Tema","Assunto","Serviço","Logradouro","Numero","CEP","Subprefeitura","Distrito",
          "Latitude","Longitude","Data do Parecer","Status da solicitação","Órgão","Órgão",
          "Data Avaliação Pesquisa Satisfação","Nível Satisfação","Qualidade Serviço","Atendeu Solicitação","Compreensão da Resposta"]
SUBS = [("BUTANTA","RIO PEQUENO",-23.571,-46.735),("SANTO AMARO","SANTO AMARO",-23.652,-46.711),("SE","REPUBLICA",-23.550,-46.634),
        ("ITAQUERA","ITAQUERA",-23.540,-46.455),("CAMPO LIMPO","CAMPO LIMPO",-23.648,-46.760),("MOOCA","MOOCA",-23.556,-46.598),
        ("CASA VERDE-CACHOEIRINHA","LIMAO",-23.502,-46.660),("M'BOI MIRIM","JARDIM ANGELA",-23.700,-46.770),
        ("PINHEIROS","PINHEIROS",-23.567,-46.700),("VILA MARIANA","VILA MARIANA",-23.589,-46.637),
        ("SAPOPEMBA","SAPOPEMBA",-23.603,-46.510),("ITAIM PAULISTA","ITAIM PAULISTA",-23.498,-46.398)]
SERV = [  # (tema, assunto, serviço, peso)
 ("Rua e bairro","Reparos em asfalto, pontes e viadutos","Tapa-buraco - solicitar reparo",18),
 ("Rua e bairro","Iluminação pública","Lâmpada apagada - solicitar reparo",14),
 ("Rua e bairro","Limpeza urbana","Descarte irregular de entulho - solicitar remoção",14),
 ("Rua e bairro","Capinação e roçada de áreas verdes","Praça e canteiro central - solicitar roçada",8),
 ("Rua e bairro","Calçadas","Calçada - solicitar reparo",7),
 ("Trânsito e transporte","Semáforos","Semáforo - solicitar manutenção",9),
 ("Trânsito e transporte","Sinalização de trânsito","Placa de trânsito - solicitar manutenção",4),
 ("Rua e bairro","Boca de lobo","Boca de lobo / bueiro - solicitar limpeza",8),
 ("Obras, imóveis e habitação","IPTU - Imposto Predial e Territorial Urbano","IPTU - Segunda via",6),
 ("Canais de Atendimento","Portal SP156","Corrigir cadastro no Portal SP156",5),
 ("Rua e bairro","Poluição Sonora - PSIU","Reclamação de poluição sonora",4),
 ("Animais","Criação inadequada de animais","Denunciar condições inadequadas de criação",3)]
CANAIS = ["PORTAL ONLINE","CENTRAL TELEFÔNICA","APLICATIVO"]
RUAS = ["Rua Augusta","Avenida Celso Garcia","Rua Teodoro Sampaio","Avenida Sapopemba","Avenida Jabaquara","Rua Vergueiro","Rua Cachoeira"]
base = datetime(2024,7,1); rows=[]
for i in range(3000):
    tema,assunto,serv,_ = rnd.choices(SERV,[s[3] for s in SERV])[0]
    sub,dist,la,lo = rnd.choice(SUBS)
    ab = base+timedelta(seconds=rnd.randint(0,90*86400))
    status = rnd.choices(["ABERTA","FINALIZADA","INDEFERIDA","CANCELADA"],[30,52,10,8])[0]
    parecer = "" if status=="ABERTA" else (ab+timedelta(hours=rnd.randint(2,24*40))).strftime("%Y-%m-%d %H:%M:%S")
    com_end = tema!="Canais de Atendimento" and rnd.random()<.93
    lat = f"{la+rnd.uniform(-.02,.02):.7f}" if com_end and rnd.random()<.85 else ""
    lon = f"{lo+rnd.uniform(-.02,.02):.7f}" if lat else ""
    if lat and rnd.random()<.01: lat,lon="0","0"   # coordenada inválida
    rows.append([ab.strftime("%Y-%m-%d %H:%M:%S"),rnd.choice(CANAIS),tema,assunto,serv,
                 rnd.choice(RUAS) if com_end else "",str(rnd.randint(1,3000)) if com_end else "",
                 f"{rnd.randint(1000,8499)}{rnd.randint(0,999):03d}" if com_end else "",sub if com_end else "",dist if com_end else "",
                 lat,lon,parecer,status,"SMSUB","","","","","",""])
rows += rnd.sample(rows,30)  # linhas duplicadas
out = Path(__file__).resolve().parents[1]/"data/sample/sp156_amostra.csv"
with open(out,"w",newline="",encoding="utf-8") as f:
    w=csv.writer(f,delimiter=";"); w.writerow(HEADER); w.writerows(rows)
print(out, len(rows))
