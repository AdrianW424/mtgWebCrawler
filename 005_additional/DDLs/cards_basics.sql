CREATE EXTERNAL TABLE IF NOT EXISTS cards_basics(
        id INT,
        image_id STRING,
        name STRING,
        type STRING
) COMMENT 'MTG Cards' PARTITIONED BY (partition_year int, partition_month int, partition_day int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    "separatorChar" = ",",
    "quoteChar"     = "\\\""
)
STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/cards_basics'
TBLPROPERTIES ('skip.header.line.count'='1');