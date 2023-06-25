CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.vendas ( 
        actual_delivery_date string,
        customerkey string,
        datekey string,
        discount_amount string,
        invoice_date string,
        invoice_number string,
        item_class string,
        item_number string,
        item string,
        line_number string,
        list_price string,
        order_number string,
        promise_delivery_date string,
        sales_amount string,
        sales_amount_based_on_list_price string,
        sales_cost_amount string,
        sales_margin_amount string,
        sales_price string,
        sales_quantity string,
        sales_rep string,
        u_m string
    )
COMMENT 'TABELA DE $i'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/VENDAS'
TBLPROPERTIES ("skip.header.line.count"="1","serialization.null.format"='');

CREATE TABLE IF NOT EXISTS desafio_curso.tbl_vendas (
                actual_delivery_date string,
        customerkey string,
        datekey string,
        discount_amount string,
        invoice_date string,
        invoice_number string,
        item_class string,
        item_number string,
        item string,
        line_number string,
        list_price string,
        order_number string,
        promise_delivery_date string,
        sales_amount string,
        sales_amount_based_on_list_price string,
        sales_cost_amount string,
        sales_margin_amount string,
        sales_price string,
        sales_quantity string,
        sales_rep string,
        u_m string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ('orc.compress'='SNAPPY','serialization.null.format'='');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    desafio_curso.tbl_vendas
PARTITION(DT_FOTO) 
SELECT
        actual_delivery_date string,
        customerkey string,
        datekey string,
        discount_amount string,
        invoice_date string,
        invoice_number string,
        item_class string,
        item_number string,
        item string,
        line_number string,
        list_price string,
        order_number string,
        promise_delivery_date string,
        sales_amount string,
        sales_amount_based_on_list_price string,
        sales_cost_amount string,
        sales_margin_amount string,
        sales_price string,
        sales_quantity string,
        sales_rep string,
        u_m string,
	'20230624' as DT_FOTO
FROM desafio_curso.vendas
;