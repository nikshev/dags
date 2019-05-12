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
    dag_id='Data_bricks', 
    default_args=args,
    schedule_interval='@hourly')

new_cluster = {
    'spark_version': '4.0.x-scala2.11',
    'node_type_id': 'i3.xlarge',
    'aws_attributes': {
        'availability': 'ON_DEMAND'
    },
    'num_workers': 1
}

notebook_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {
        'notebook_path': '/Users/perepe4a@gmail.com/airflow',
    },
}
    
notebook_task = DatabricksSubmitRunOperator(
    task_id='run_notebook_task',
    dag=dag,
    json=notebook_task_params)

notebook_task