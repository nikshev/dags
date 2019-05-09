from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        logging.info(response.text)
        for index in data['data']:
            r = data['data'][index]
            """ Insert article to article table """
            try:
                rows_to_insert = [
                    (r['symbol'], r['quote']['USD']['price'],
                     mktime(datetime.now().timetuple()))
                ]
                errors = client.insert_rows(
                    table, rows_to_insert)  # API request
                logging.error(errors)
            except Exception as e:
                logging.error("Exception insert rates:" + str(e))
                pass
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        logging.error(e)


dag = DAG('crypto', description='Pull crypto rates from coinmarketcap.com',
          schedule_interval='*/1 * * * *',
          catchup=False)


crypto_pull_rates_operator = PythonOperator(
    task_id='crypto_pull_rates', python_callable=crypto_pull_rates, dag=dag)

crypto_pull_rates_operator
