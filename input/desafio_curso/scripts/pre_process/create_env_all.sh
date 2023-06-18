#!/bin/bash

# Criação das pastas

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

echo "Inicio da criacao em $(date)"

cd ../../raw

for i in "${DADOS[@]}"
do
	echo "$i"
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    #beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_clientes.hql
done

echo "Termino da criacao em $(date)"