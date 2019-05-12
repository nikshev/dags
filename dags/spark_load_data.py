#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 14:30:17 2019

@author: iperepecha
"""

import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='Databricks_spark', 
    default_args=args,
    schedule_interval='@hourly')

notebook_spark_load_data_params = {
    'existing_cluster_id': '0508-130010-films3',
    'notebook_task': {
        'notebook_path': '/Users/perepe4a@gmail.com/spark-load-data',
    },
}


'''Load data to AWS''' 
spark_load_data = DatabricksSubmitRunOperator(
    task_id='run_spark_load_data_to_aws',
    dag=dag,
    json=notebook_spark_load_data_params)


notebook_spark_daily_calculations_params = {
    'existing_cluster_id': '0508-130010-films3',
    'notebook_task': {
        'notebook_path': '/Users/perepe4a@gmail.com/spark-calculate-data',
    },
}

'''Load data to AWS''' 
spark_daily_calculations = DatabricksSubmitRunOperator(
    task_id='run_spark_daily_calculations',
    dag=dag,
    json=notebook_spark_daily_calculations_params)


''' Start Dummy operator '''
start_operator = DummyOperator(task_id='start')


''' End Dummy operator '''
end_operator = DummyOperator(task_id='end')


start_operator >> spark_load_data >> spark_daily_calculations >> end_operator