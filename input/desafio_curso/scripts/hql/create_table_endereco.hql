CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL} ( 
        address_number string,
        city string,
        country string,
        customer_address_1 string,
        customer_address_2 string,
        customer_address_3 string,
        customer_address_4 string,
        state string,
        zip_code string
    )
COMMENT 'TABELA DE $i'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
        address_number string,
        city string,
        country string,
        customer_address_1 string,
        customer_address_2 string,
        customer_address_3 string,
        customer_address_4 string,
        state string,
        zip_code string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA}
PARTITION(DT_FOTO) 
SELECT
    address_number string,
    city string,
    country string,
    customer_address_1 string,
    customer_address_2 string,
    customer_address_3 string,
    customer_address_4 string,
    state string,
    zip_code string,
	${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL}
;