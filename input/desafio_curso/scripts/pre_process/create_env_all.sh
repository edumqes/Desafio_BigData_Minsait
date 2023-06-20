#!/bin/bash

# Criação das pastas

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")
PARTICAO="$(date --date="-0 day" "+%Y%m%d")"

echo "Inicio da criacao em $(date)"

cd ../../raw

for i in "${DADOS[@]}"
do

    TARGET_DATABASE="DESAFIO_CURSO"
    HDFS_DIR="/datalake/raw/$i"
    TARGET_TABLE_EXTERNAL="$i"
    TARGET_TABLE_GERENCIADA="TBL_$i"

    beeline -u jdbc:hive2://localhost:10000 \
    --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
    --hivevar HDFS_DIR="${HDFS_DIR}"\
    --hivevar TARGET_TABLE_EXTERNAL="${TARGET_TABLE_EXTERNAL}"\
    --hivevar TARGET_TABLE_GERENCIADA="${TARGET_TABLE_GERENCIADA}"\
    --hivevar PARTICAO="${PARTICAO}"\
    -f ../scripts/hql/create_table_${i,,}.hql 
    #beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_clientes.hql
done

echo "Termino da criacao em $(date)"