#Inport functions
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

#Criar dataframes a partir do hive
df_regiao = spark.sql("select * from desafio_curso.tbl_regiao")
df_divisao = spark.sql("select * from desafio_curso.tbl_divisao")

#Campos com espaço "Não Informado" - Clientes
query_clientes = '''
select address_number,business_family,business_unit,customer,customerkey,customer_type,division,
case when length(trim(line_of_business)) = 0 then 'Não Informado' else line_of_business end as line_of_business,
phone,region_code,regional_sales_mgr,search_type,dt_foto
from desafio_curso.tbl_clientes'''
df_clientes = spark.sql(query_clientes)

#Campos com espaço "Não Informado" - Endereço
query_endereco = '''
select address_number,
case when length(trim(city)) = 0 then 'Não Informado' else city end as city,country,
case when length(trim(customer_address_1)) = 0 then 'Não Informado' else customer_address_1 end as customer_address_1,
case when length(trim(customer_address_2)) = 0 then 'Não Informado' else customer_address_2 end as customer_address_2,
case when length(trim(customer_address_3)) = 0 then 'Não Informado' else customer_address_3 end as customer_address_3,
case when length(trim(customer_address_4)) = 0 then 'Não Informado' else customer_address_4 end as customer_address_4,
case when length(trim(state)) = 0 then 'Não Informado' else state end as state,
case when length(trim(zip_code)) = 0 then 'Não Informado' else zip_code end as zip_code,dt_foto
from desafio_curso.tbl_endereco'''
df_endereco = spark.sql(query_endereco)

#Campos null como 'Não Informado' ou zero
query_vendas = '''
select actual_delivery_date,customerkey,datekey,
nvl(replace(discount_amount,',','.'),0) as discount_amount,
invoice_date,invoice_number,
nvl(item_class,'Não Informado') as item_class,
nvl(item_number,0) as item_number,
item,line_number,
replace(list_price,',','.') as list_price,
order_number,promise_delivery_date,
replace(sales_amount,',','.') as sales_amount,
replace(sales_amount_based_on_list_price,',','.') as sales_amount_based_on_list_price,
replace(sales_cost_amount,',','.') as sales_cost_amount,
replace(sales_margin_amount,',','.') as sales_margin_amount,
nvl(replace(sales_price,',','.'),0) as sales_price,
sales_quantity,sales_rep,u_m
from desafio_curso.tbl_vendas'''
df_vendas = spark.sql(query_vendas)

#Views a partir dos dataframes gerados
df_clientes.createOrReplaceTempView('clientes')
df_regiao.createOrReplaceTempView('regiao')
df_divisao.createOrReplaceTempView('divisao')
df_endereco.createOrReplaceTempView('endereco')
df_vendas.createOrReplaceTempView('vendas')

#Tabela Stage
query_stage = '''
select
    c.address_number,
    c.business_family,
    c.business_unit,
    c.customer,
    c.customerkey,
    c.customer_type,
    c.division,
    c.line_of_business,
    c.phone,
    c.region_code,
    c.regional_sales_mgr,
    c.search_type,
    d.division_name,
    r.region_name,
    e.city,
    e.country,
    e.customer_address_1,
    e.customer_address_2,
    e.customer_address_3,
    e.customer_address_4,
    e.state,
    e.zip_code,
    v.actual_delivery_date,
    v.datekey,
    v.discount_amount,
    v.invoice_date,
    v.invoice_number,
    v.item_class,
    v.item_number,
    v.item,
    v.line_number,
    v.list_price,
    v.order_number,
    v.promise_delivery_date,
    v.sales_amount,
    v.sales_amount_based_on_list_price,
    v.sales_cost_amount,
    v.sales_margin_amount,
    v.sales_price,
    v.sales_quantity,
    v.sales_rep,
    v.u_m
from vendas v
inner join clientes c on c.customerkey = v.customerkey
inner join divisao d on c.division = d.division
inner join regiao r on c.region_code = r.region_code
left join endereco e on c.address_number = e.address_number
'''
df_stage = spark.sql(query_stage)

#Adicionar medidas de tempo (Ano, Mês, Dia, Trimestre) considerando invoice_date
df_stage = (df_stage
            .withColumn('Ano',f.year(f.to_timestamp('invoice_date','dd/MM/yyyy')))
            .withColumn('Mes',f.month(f.to_timestamp('invoice_date','dd/MM/yyyy')))
            .withColumn('Dia',f.dayofmonth(f.to_timestamp('invoice_date','dd/MM/yyyy')))
            .withColumn('Trimestre',f.quarter(f.to_timestamp('invoice_date','dd/MM/yyyy')))
           )

#Criação de chaves para modelo dimensional
df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.address_number,df_stage.city,df_stage.country,df_stage.customer_address_1,df_stage.customer_address_2,df_stage.customer_address_3,df_stage.customer_address_4,df_stage.state,df_stage.zip_code), 256))
df_stage = df_stage.withColumn("DW_CLIENTES", sha2(concat_ws("", df_stage.address_number,df_stage.business_family,df_stage.business_unit,df_stage.customer,df_stage.customerkey,df_stage.customer_type,df_stage.division,df_stage.division_name,df_stage.line_of_business,df_stage.phone,df_stage.region_code,df_stage.region_name,df_stage.regional_sales_mgr,df_stage.search_type), 256))
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("", df_stage.invoice_date,df_stage.Ano,df_stage.Mes,df_stage.Dia,df_stage.Trimestre), 256))

df_stage.createOrReplaceTempView('stage')

#Criando a dimensão Localidade (campos nulos tratados como 'Não Informado')
dim_localidade = spark.sql('''
    SELECT DISTINCT
        DW_LOCALIDADE,
        address_number,
        nvl(city,'Não Informado') as city,
        nvl(country,'Não Informado') as country,
        nvl(customer_address_1,'Não Informado') as customer_address_1,
        nvl(customer_address_2,'Não Informado') as customer_address_2,
        nvl(customer_address_3,'Não Informado') as customer_address_3,
        nvl(customer_address_4,'Não Informado') as customer_address_4,
        nvl(state,'Não Informado') as state,
        nvl(zip_code,'Não Informado') as zip_code
    FROM stage    
''')

#Criando a dimensão Tempo
dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        invoice_date,
        ano,
        mes,
        dia,
        trimestre
    FROM stage    
''')

#Criando a dimensão Cliente
dim_cliente = spark.sql('''
    SELECT DISTINCT
        DW_CLIENTES,
        address_number,
        business_family,
        business_unit,
        customer,
        customerkey,
        customer_type,
        division,
        division_name,
        line_of_business,
        phone,
        region_code,
        region_name,
        regional_sales_mgr,
        search_type
    FROM stage    
''')

#Criando tabela fato Vendas
ft_vendas = spark.sql('''
    SELECT 
        DW_CLIENTES,
        DW_LOCALIDADE,
        DW_TEMPO,
        invoice_number,
        sum(sales_amount) as vl_total
    FROM stage
    group by 
        DW_CLIENTES,
        DW_LOCALIDADE,
        DW_TEMPO,
        invoice_number
''')

# Função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
    print(rename)

    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

# Salvando os dados em input/desafio_curso/gold
salvar_df(ft_vendas, 'ft_vendas')
salvar_df(dim_cliente, 'dim_cliente')
salvar_df(dim_tempo, 'dim_tempo')
salvar_df(dim_localidade, 'dim_localidade')