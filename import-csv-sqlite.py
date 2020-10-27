#https://medium.com/@fernando.gama/movendo-arquivos-csv-para-o-sqlite-com-python-e-r-8a2b40f3cb66

import os
import sqlite3
import pandas as pd

# ler todos os arquivos csv do diretório e guardar em um objeto
all_files = list(filter(lambda x: '.csv' in x, os.listdir('bases/')))

# realizar a leitura para cada arquivo
full_dataset = []
for elem in all_files:
    data = pd.read_csv('bases/'+ elem)
    full_dataset.append(data)

# estabelecendo a conexão e criando o banco
con = sqlite3.connect('test-join.db')

# obtendo os nomes das tabelas
table_names = [os.path.splitext(elem)[0] for elem in all_files]

# obtendo os campos de todas as tabelas
table_fields = []
for i in range(0,len(table_names)):
    columnNames = list(full_dataset[i].head(0))
    table_fields.append(columnNames)

cur = con.cursor()

# criando as tabelas no SQLite
for item in range(0,len(table_names)):
    print("CREATE TABLE IF NOT EXISTS " + table_names[item] + " ('" + "','".join(table_fields[item])+ "')")
    cur.execute("CREATE TABLE IF NOT EXISTS " + table_names[item] + " ('" + "','".join(table_fields[item])+ "')")
# Varre todas as tabelas e para cada uma é realizado a inserção dos dados
for ind in range(0, len(table_names)):
    query = "INSERT INTO " + str(table_names[ind]) + "(" + ",".join(table_fields[ind]) + ") VALUES ("+ ",".join(map(str,"?"*len(full_dataset[ind].columns))) +")"
    print(query)
    full_dataset[ind] = full_dataset[ind].astype(str)
    for i in range(0, len(full_dataset[ind])):
        insert_register = tuple(full_dataset[ind].iloc[i])
        print(insert_register)
        cur.execute(query, insert_register)
        con.commit()
