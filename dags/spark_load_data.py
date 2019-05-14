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
    ti = context['task_instance']
    logging.log(level=20,msg=ti)
    run_id = ti.xcom_pull(key='run_id', task_ids=['run_spark_load_data_to_aws'])
    logging.log(level=20,msg=run_id)
    return run_id
    

args = {
    'owner': 'airflow',
    'provide_context':True,
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='Databricks_spark', 
    default_args=args,
    schedule_interval='@hourly')

new_cluster = {
    'spark_version': '4.0.x-scala2.11',
    "node_type_id": "m4.large",
    "driver_node_type_id": "m4.large",
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    'aws_attributes': {'availability': 'ON_SPOT',
                       'zone_id': "eu-west-1c",
                       'spot_bid_price_percent': 30,
                       'instance_profile_arn': 'arn:aws:iam::948458241037:instance-profile/role-ec2-s3',
                       "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                       "ebs_volume_count": 1,
                       "ebs_volume_size": 100},
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
    new_cluster=new_cluster,
    notebook_task={'notebook_path': '/Users/perepe4a@gmail.com/spark-load-data'},
    do_xcom_push=True,
    dag=dag)


notebook_spark_daily_calculations_params = {
    'new_cluster': new_cluster,
    'notebook_task': {
        'notebook_path': '/Users/perepe4a@gmail.com/spark-calculate-data',
    },
}

'''Load data to AWS''' 
spark_daily_calculations = DatabricksSubmitRunOperator(
    task_id='run_spark_daily_calculations',
    do_xcom_push=True,
    dag=dag,
    json=notebook_spark_daily_calculations_params)


''' Start Dummy operator '''
start_operator = DummyOperator(task_id='start', dag=dag)


''' End Dummy operator '''
end_operator = DummyOperator(task_id='end', dag=dag)

x_xom_operator = PythonOperator(
    task_id='x_com_operator', python_callable=x_com_pull, dag=dag
)


start_operator >> spark_load_data >> x_xom_operator >> spark_daily_calculations >> end_operator