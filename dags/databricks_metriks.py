#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 13 23:39:22 2019

@author: iperepecha
"""

import airflow
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from influxdb import InfluxDBClient


def set_metrics():
    influx = InfluxDBClient(Variable.get("influx_host"),
                            Variable.get("influx_port"), 
                            Variable.get("influx_user"), 
                            Variable.get("influx_password"), 
                            Variable.get("influx_db"))
    
    m = [{
        "measurement": "cluster_run_time",
        "tags": {
            "host": "airflow"
        },
        "fields": {
            "elapsed_time": random.randint(10000, 40000)
        }}]
    
    influx.write_points(m)
    
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='Databricks_metriks', 
    default_args=args,
    schedule_interval='*/5 * * * *') 

set_metrics = PythonOperator(
    task_id='set_metrics', python_callable=set_metrics, dag=dag
)

set_metrics






