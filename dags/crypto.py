from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from google.cloud import bigquery
from time import mktime
import json
import logging


def crypto_pull_rates():
    url = Variable.get("crypto_url")
    parameters = {
        'id': Variable.get("crypto_id")
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': Variable.get("crypto_key"),
    }

    # Create, populate and persist an entity with key
    client = bigquery.Client()
    table_ref = client.dataset(Variable.get("crypto_dataset")).table(
        Variable.get("crypto_table"))
    table = client.get_table(table_ref)

    session = Session()
    session.headers.update(headers)

    response = session.get(url, params=parameters)
    data = json.loads(response.text)
    logging.info(response.text)
    rows_to_insert = []
    for index in data['data']:
        r = data['data'][index]
        """ Insert article to article table """
        rows_to_insert.append((r['symbol'], float(r['quote']['USD']['price']),
                               mktime(datetime.now().timetuple())))
    errors = client.insert_rows(
        table, rows_to_insert)  # API request
    logging.error(errors)


dag = DAG('crypto', description='Pull crypto rates from coinmarketcap.com',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2019, 5, 9),
          catchup=False)

''' Start Dummy operator '''
start_operator = DummyOperator(task_id='start')

''' End Dummy operator '''
end_operator = DummyOperator(task_id='end')


''' Crypto pull rates '''
crypto_pull_rates_operator = PythonOperator(
    task_id='crypto_pull_rates', python_callable=crypto_pull_rates, dag=dag)


''' BTC 5min BigQueryOperator '''
btc_five_minutes_operator = BigQueryOperator(
    task_id='bq_btc_five_minutes_operator',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    SELECT 
        TIMESTAMP_SECONDS(300 * DIV(UNIX_SECONDS(created_at) + 450, 300)) as timestamp,
        symbol,
        ARRAY_AGG(price ORDER BY created_at LIMIT 1)[SAFE_OFFSET(0)] open,
        MAX(price) high,
        MIN(price) low,
        ARRAY_AGG(price ORDER BY created_at DESC LIMIT 1)[SAFE_OFFSET(0)] close
    FROM `composer-236006.crypto.ticks`
    WHERE symbol ='BTC'
    GROUP BY symbol, timestamp
    ORDER BY symbol, timestamp DESC
    LIMIT 5
    ''',    destination_dataset_table='composer-236006.crypto.ohlc5m',
    dag=dag)


crypto_pull_rates_operator.set_upstream(start_operator)
btc_five_minutes_operator.set_upstream(crypto_pull_rates_operator)
end_operator.set_upstream(btc_five_minutes_operator)
