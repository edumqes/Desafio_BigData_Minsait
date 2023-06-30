# Desafio_BigData_Minsait
Avaliação do Treinamento Big Data Minsait

Desenvolvimento de Projeto utilizando ferramentas de big data como Hive, Hadoop, Apache PySpark e Power BI considerando os arquivos abaixo disponibilizados em /input/desafio_curso/raw:

    CLIENTES.csv
    DIVISAO.csv
    ENDERECO.csv
    REGIAO.csv
    VENDAS.csv

# Para podermos iniciar as tratativas de desnvolvimento, se faz necessário executar o Docker para carga dos containers nos quais as ferramentas já estão diponibilizadas conforme arquivo docker-compose.yaml na pasta bigdata_docker

1) Cópia automática dos arquivos .csv para hdfs dfs /datalake/raw/$tabela/$tabela.csv:
 - Executada por shell script copy_files.sh em /input/desafio_curso/scripts/pre_process.

2) Carga das tabelas no hive:
 - Após criar database DESAFIO_CURSO, as tabelas foram carregadas utilizando script create_env_all.sh em /input/desafio_curso/scripts/pre_process.
 - Este script irá executar os comandos "create table..." em scripts hql individuais para cada arquivo de carga, os mesmos constam em /input/desafio_curso/scripts/hql.
 - Cada script .hql irá gerar duas tabelas sendo uma tabela externa e uma tabela gerenciada particionada para posterior processamento no PySpark (ex: tabela externa clientes e tabela gerenciada tbl_clientes com coluna adicional DTFOTO no qual constará a data em que os registros foram carregados).

  - Observação: no script create_table_vendas.hql foi adicionada uma propriedade "serialization.null.format"='' para que os campos vazios no arquivo .csv fossem tratados como NULL na carga da tabela Hive.

  - Abaixo seguem tabelas carregadas no Hive após execução dos scripts:
    
    Tabelas externas:
        CLIENTES
        DIVISAO
        ENDERECO
        REGIAO
        VENDAS

    Tabelas gerenciadas (com coluna adicional DTFOTO):
        TBL_CLIENTES
        TBL_DIVISAO
        TBL_ENDERECO
        TBL_REGIAO
        TBL_VENDAS

3) Criação dos dataframes para cada tabela hive
 - Conforme solicitado no treinamento, os dataframes foram criados considerando todas as tabelas gerenciadas carregadas no hive utilizando o comando df_$tabela = spark.sql("select * from tbl_$tabela")
 - Para as tabelas endereco e clientes, os campos que continham somente espaços foram carregados no data frame com valor 'Não Informado' conforme script process.py
 - Para a tabela de vendas, devido à carga de campos nulos foi utilizado nvl(campo_null,0) para campos numéricos e nvl(Campo_null,'Não Informado') para campos do tipo string, para maiores detalhes consultar o script process.py em /input/desafio_curso/script/process
 - Para carga da tabela stage foi utilizado comando join para relacionar todas as tabelas conforme modelo DER anexo em /input/desafio_curso/gold
 - Para desenvolvimento sa dimensão tempo, foram adicionadas à tabela Stage as colunas Ano, Mês, Dia e Trimestre utilizando o campo invoice_date.
 - As chaves para o modelo dimensional (DW_TEMPO, DW_LOCALIDADE e DW_CLIENTE) foram criadas com criptografia sha256 conforme consta no script process.py
 - Após criação das chaves, os valores hash foram adicionados em novas colunas à tabela STAGE para posterior criação das dimensões conforme script pySpark.

4) Criar dimensões e tabela fato
 - Abaixo seguem dimensões geradas a partir das chaves gravadas na tabela Stage:

        DIM_TEMPO -> Campos DW_TEMPO, invoice_date, stage.ano, stage.mes, stage.dia e stage.trimestre
        DIM_LOCALIDADE -> Campos DW_LOCALIDADE, address_number, city, country, customer_address_1, customer_address_2, customer_address_3, customer_address_4, state e zip_code
        DIM_CLIENTES -> Campos DW_CLIENTES, address_number, business_family, business_unit, customer, customerkey, customer_type, division, division_name, line_of_busines, phone, region_code, region_name, regional_sales_mgre e search_type.

 - A tabela fato FT_VENDAS foi criada utilizando as três chaves já mencionadas anteriormente além dos campos invoice_date e sales_amount sumarizados "group by" considerando so demais critérios agrupados para que não hajam chaves duplicadas.

 - As demais dimensões criadas não possuem campos duplicados conforme solicitado no treinemanto

5) As tabelas dimensiomais e fato foram exportada para arquivos .csv disponibilizados em  /input/desafio_curso/gold, lembrando que ambos os arquivos relacionados abaixoque servirão de fonte para o dashboard Power BI:

    DIM_CLIENTES.csv
    DIM_LOCALIDADE.csv
    DIM_TEMPO.csv
    FT_VENDAS.csv

6) Os arquivos mencionados acima foram consumidos pelo dashboard app Projeto Vendas.pbix no qual consta os seguintes indicadores:

     - Total de vendas
     - Quantidade de faturas
     - Vendas por cliente (customerkey)
     - Vendas por País
     - Vendas por Ano e Mês

Observação: valores podem ser exibidos de forma totalizada ou restrita (ex: vendas para um único cliente por ano/mês).






