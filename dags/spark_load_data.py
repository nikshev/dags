#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 14:30:17 2019

@author: iperepecha
"""

import airflow
import logging
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

logging.info("Start")

dag = DAG(
    dag_id='Databricks_spark', 
    default_args=args,
    schedule_interval='@hourly')

notebook_task_params = {
    'existing_cluster_id': '0508-130010-films3',
    'notebook_task': {
        'notebook_path': '/Users/perepe4a@gmail.com/spark-load-data',
    },
}
    
spark_load_data = DatabricksSubmitRunOperator(
    task_id='run_spark_load_data',
    dag=dag,
    json=notebook_task_params)

logging.info(spark_load_data)

spark_load_data