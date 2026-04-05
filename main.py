import pandas as pd
import requests
from questdb.ingress import Sender, TimestampNanos
from datetime import datetime, timezone


df = pd.read_csv("questdb-usuarios-dataset.csv")

# Tratamento da conluna 'conexaoCliente'
df['conexaoCliente'] = pd.to_numeric(df['conexaoCliente'], errors='coerce')

# Mapeamento e conversão da coluna statusInternet 
statusInternet_map = {
    0:"Offline",
    1:"Online",
    2:"Desativado",
    3:"Bloqueio Manual",
    4:"Bloqueio Automático",
    5:"Financeiro em Atraso",
    6:"Aguardando Assinatura"
}
df['statusInternet'] = df['statusInternet'].map(statusInternet_map)

# Tratamento do nome para fica só o primeiro e o último
def ajustar_nome(nome):
    partes = nome.split()
    if len(partes) > 1:
        return partes[0] + " " + partes[-1]
    return nome

df['nomeCliente'] = df['nomeCliente'].apply(ajustar_nome)

# Pegando as colunas não tratadas

colunas_tratadas = {'statusInternet', 'nomeCliente', 'conexaoCliente', 'timestamp'}

colunas_outras = [col for col in df.columns if col not in colunas_tratadas]

# Manter só o ultimo registro do cliente
df = df.sort_values('timestamp').drop_duplicates(subset=['nomeCliente'], keep='last')

# Limpa a tabela antes de inserir
requests.get(
    "http://questdb:9000/exec",
    params={"query": "TRUNCATE TABLE clientes;"}
)

# Conexão com QuestDB
conf = "http::addr=questdb:9000;"
with Sender.from_conf(conf) as sender:
    for _, row in df.iterrows():

        outras_cols = {
            col: (row[col] if pd.notna(row[col]) else None)
            for col in colunas_outras
        }

        sender.row(
            "clientes",
            symbols={'statusInternet': row['statusInternet']},
            columns={
                'nomeCliente': row['nomeCliente'],
                'conexaoCliente': row['conexaoCliente'] if pd.notna(row['conexaoCliente']) else None,
                **outras_cols
            },
            at=datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
        )

    sender.flush()



print("Dados inseridos com sucesso no QuestDB!")





# print(df.columns)
# print(df_result.head()) 
# print (df)
# print (df["statusInternet"])
# print (df["nomeCliente"])
