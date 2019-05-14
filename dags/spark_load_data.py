#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 14:30:17 2019

@author: iperepecha
"""

import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import logging


def x_com_pull(**context):
    run_id = context['ti'].xcom_pull(task_ids='run_spark_load_data_to_aws')
    logging.log(run_id)
    

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='Databricks_spark', 
    default_args=args,
    schedule_interval='@hourly')

new_cluster = {
    'spark_version': '4.0.x-scala2.11',
    'node_type_id': 'i3.xlarge',
    'aws_attributes': {'availability': 'ON_DEMAND',
                       'zone_id': "eu-west-1c",
                       'instance_profile_arn': 'arn:aws:iam::948458241037:instance-profile/role-ec2-s3'},
    'num_workers': 1
}


    
notebook_spark_load_data_params = {
    'new_cluster': new_cluster,
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
    'new_cluster': new_cluster,
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
start_operator = DummyOperator(task_id='start', dag=dag)


''' End Dummy operator '''
end_operator = DummyOperator(task_id='end', dag=dag)

x_xom_operator = PythonOperator(
    task_id='x_xom_operator', python_callable=x_com_pull, provide_context=True, dag=dag
)


start_operator >> spark_load_data >> spark_daily_calculations >> x_xom_operator >> end_operator