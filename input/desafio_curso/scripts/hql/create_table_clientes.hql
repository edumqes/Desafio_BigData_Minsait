CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.CLIENTES ( 
        address_number string,
        business_family string,
        business_unit string,
        customer string,
        customerkey string,
        customer_type string,
        division string,
        line_of_business string,
        phone string,
        regional_code string,
        regional_sales_mgr string,
        search_type string
    )
COMMENT 'Tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/CLIENTES/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_CLIENTES (
        address_number string,
        business_family string,
        business_unit string,
        customer string,
        customerkey string,
        customer_type string,
        division string,
        line_of_business string,
        phone string,
        regional_code string,
        regional_sales_mgr string,
        search_type string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    DESAFIO_CURSO.TBL_CLIENTES
PARTITION(DT_FOTO) 
SELECT
    address_number string,
    business_family string,
    business_unit string,
    customer string,
    customerkey string,
    customer_type string,
    division string,
    case when length(trim(line_of_business)) = 0 then 'Nao Informado' else line_of_business end string,
    phone string,
    regional_code string,
    regional_sales_mgr string,
    search_type string,
	'20230618' as DT_FOTO
FROM DESAFIO_CURSO.CLIENTES
;

