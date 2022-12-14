# -*- coding: utf-8 -*-

"""
Title: MTGCrawler DAG
Author: Adrian Waldera
Description: DAG for the MTG crawler project
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsMkdirFileOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MySqlOperator
import libs.hiveMySQLLink as HiveMySQLLink

args = {
    'owner': 'airflow'
}

hiveSQL_create_table_cards_basics='''
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
'''

hiveSQL_add_partition_cards_basics='''
ALTER TABLE cards_basics
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/cards_basics/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/';
'''

mySQL_create_database="""
CREATE DATABASE IF NOT EXISTS mtg;
"""

mySQL_drop_table_cards_basics="""
DROP TABLE IF EXISTS mtg.cards_basics;
"""

mySQL_create_table_cards_basics="""
CREATE TABLE mtg.cards_basics (
    id INT,
    image_id TEXT,
    name TEXT,
    type TEXT
);
"""

dag = DAG('MTGCrawler', default_args=args, description='Crawl MTG Cards',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='mtg',
    dag=dag,
    )

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/mtg',
    pattern='*',
    dag=dag,
)

run_python_crawler = BashOperator(
    task_id='run_python_crawler',
    bash_command='python3.9 /home/airflow/airflow/dags/libs/crawler.py {{ ds }}',
    dag=dag,
)

create_hdfs_mtg_cards_basics_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_mtg_cards_basics_dir',
    directory='/user/hadoop/mtg/cards_basics/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_mtg_basics = HdfsPutFileOperator(
    task_id='upload_cards_basics_to_hdfs',
    local_file='/home/airflow/mtg/cards.basics_{{ ds }}.csv',
    remote_file='/user/hadoop/mtg/cards_basics/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/cards.basics_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_HiveTable_cards_basics = HiveOperator(
    task_id='create_cards_basics_table',
    hql=hiveSQL_create_table_cards_basics,
    hive_cli_conn_id='beeline',
    dag=dag,
    )

addPartition_HiveTable_cards_basics = HiveOperator(
    task_id='add_partition_cards_basics_table',
    hql=hiveSQL_add_partition_cards_basics,
    hive_cli_conn_id='beeline',
    dag=dag,
)

mySQL_create_mtg_database = MySqlOperator(
    sql=mySQL_create_database,
    task_id="sql_create_database",
    mysql_conn_id="mysql",
    dag=dag,
)

mySQL_drop_mtg_cards_basics = MySqlOperator(
    sql=mySQL_drop_table_cards_basics,
    task_id="sql_query_drop_mtg_cards_basics",
    mysql_conn_id="mysql",
    dag=dag,
)

mySQL_create_mtg_cards_basics = MySqlOperator(
    sql=mySQL_create_table_cards_basics,
    task_id="sql_query_create_mtg_cards_basics",
    mysql_conn_id="mysql",
    dag=dag,
)

transfer_Hive_to_MySQL = PythonOperator(
    task_id='hive_to_mysql',
    python_callable=HiveMySQLLink.main,
    op_args=["{{ ds }}"],
    dag=dag,
)

create_local_import_dir >> clear_local_import_dir >> run_python_crawler >> create_hdfs_mtg_cards_basics_partition_dir >> hdfs_put_mtg_basics >> create_HiveTable_cards_basics >> addPartition_HiveTable_cards_basics >> mySQL_create_mtg_database >> mySQL_drop_mtg_cards_basics >> mySQL_create_mtg_cards_basics >> transfer_Hive_to_MySQL