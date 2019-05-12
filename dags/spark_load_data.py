#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 14:30:17 2019

@author: iperepecha
"""

import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='Databricks_spark', 
    default_args=args,
    schedule_interval='@hourly')

#new_cluster = {
#    'spark_version': '4.0.x-scala2.11',
#    'node_type_id': 'i3.xlarge',
#    'aws_attributes': {
#        'availability': 'ON_DEMAND'
#    },
#    'num_workers': 1
#}

#cluster = {'existing_cluster_id': 'airflow-test'}

notebook_task_params = {
    'existing_cluster_id': 'airflow-test',
    'notebook_task': {
        'notebook_path': '/Users/perepe4a@gmail.com/spark-load-data',
    },
}
    
spark_load_data = DatabricksSubmitRunOperator(
    task_id='run_spark_load_data',
    dag=dag,
    json=notebook_task_params)

spark_load_data