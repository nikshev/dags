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
from influxdb import InfluxDBClient
import json
import logging


def btc_five_minutes_rc():
    influx = InfluxDBClient(Variable.get("influx_host"),
                            Variable.get("influx_port"), Variable.get("influx_user"), Variable.get("influx_password"), Variable.get("influx_db"))
    bq = bigquery.Client()
    query = """SELECT COUNT(1) FROM `composer-236006.crypto.ohlc5m`"""
    query_job = bq.query(query)
    data = query_job.result()
    itr = iter(data)
    first_row = next(itr)
    logging.info(first_row[0])

    # Set row count to influxdb
    m = [{
        "measurement": "ohlc5m_rows",
        "tags": {
            "host": "airflow"
        },
        "fields": {
            "rows": data.total_rows
        }}]
    # influx.write_points(m)


def crypto_pull_rates():
    influx = InfluxDBClient(Variable.get("influx_host"),
                            Variable.get("influx_port"), Variable.get("influx_user"), Variable.get("influx_password"), Variable.get("influx_db"))
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

    # Set row count to influxdb
    m = [{
        "measurement": "crypto_rows",
        "tags": {
            "host": "airflow"
        },
        "fields": {
            "rows": len(rows_to_insert)
        }}]
    influx.write_points(m)


dag = DAG('crypto', description='Pull crypto rates from coinmarketcap.com',
          schedule_interval='*/2 * * * *',
          start_date=datetime(2019, 5, 9),
          catchup=False)

''' Start Dummy operator '''
start_operator = DummyOperator(task_id='start')

''' End Dummy operator '''
end_operator = DummyOperator(task_id='end')


''' Crypto pull rates '''
crypto_pull_rates_operator = PythonOperator(
    task_id='crypto_pull_rates', python_callable=crypto_pull_rates, dag=dag)


''' BTC 5  min BigQueryOperator '''
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
    ''',    destination_dataset_table='composer-236006.crypto.ohlc5m',
    dag=dag)


''' BTC 5 min record count PythonOperator '''
btc_five_minutes_operator_rc = PythonOperator(
    task_id='btc_five_minutes_rc', python_callable=btc_five_minutes_rc, dag=dag
)


''' BTC 15 min BigQueryOperator '''
btc_fifteen_minutes_operator = BigQueryOperator(
    task_id='bq_btc_fifteen_minutes_operator',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    SELECT 
        TIMESTAMP_SECONDS(900 * DIV(UNIX_SECONDS(created_at) + 450, 900)) as timestamp,
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
    ''',    destination_dataset_table='composer-236006.crypto.ohlc15m',
    dag=dag)

''' BTC 30 min BigQueryOperator '''
btc_thirty_minutes_operator = BigQueryOperator(
    task_id='bq_btc_thirty_minutes_operator',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    SELECT 
        TIMESTAMP_SECONDS(1800 * DIV(UNIX_SECONDS(created_at) + 450, 1800)) as timestamp,
        symbol,
        ARRAY_AGG(price ORDER BY created_at LIMIT 1)[SAFE_OFFSET(0)] open,
        MAX(price) high,
        MIN(price) low,
        ARRAY_AGG(price ORDER BY created_at DESC LIMIT 1)[SAFE_OFFSET(0)] close
    FROM `composer-236006.crypto.ticks`
    WHERE symbol ='BTC'
    GROUP BY symbol, timestamp
    ORDER BY symbol, timestamp DESC
    ''',    destination_dataset_table='composer-236006.crypto.ohlc30m',
    dag=dag)

''' BTC 1 hour BigQueryOperator '''
btc_one_hour_operator = BigQueryOperator(
    task_id='bq_btc_one_hour_operator',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    SELECT 
        TIMESTAMP_SECONDS(3600 * DIV(UNIX_SECONDS(created_at) + 450, 3600)) as timestamp,
        symbol,
        ARRAY_AGG(price ORDER BY created_at LIMIT 1)[SAFE_OFFSET(0)] open,
        MAX(price) high,
        MIN(price) low,
        ARRAY_AGG(price ORDER BY created_at DESC LIMIT 1)[SAFE_OFFSET(0)] close
    FROM `composer-236006.crypto.ticks`
    WHERE symbol ='BTC'
    GROUP BY symbol, timestamp
    ORDER BY symbol, timestamp DESC
    ''',    destination_dataset_table='composer-236006.crypto.ohlc1h',
    dag=dag)

''' BTC 4 hour BigQueryOperator '''
btc_four_hour_operator = BigQueryOperator(
    task_id='bq_btc_four_hour_operator',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    SELECT 
        TIMESTAMP_SECONDS(14400 * DIV(UNIX_SECONDS(created_at) + 450, 14400)) as timestamp,
        symbol,
        ARRAY_AGG(price ORDER BY created_at LIMIT 1)[SAFE_OFFSET(0)] open,
        MAX(price) high,
        MIN(price) low,
        ARRAY_AGG(price ORDER BY created_at DESC LIMIT 1)[SAFE_OFFSET(0)] close
    FROM `composer-236006.crypto.ticks`
    WHERE symbol ='BTC'
    GROUP BY symbol, timestamp
    ORDER BY symbol, timestamp DESC
    ''',    destination_dataset_table='composer-236006.crypto.ohlc4h',
    dag=dag)

''' BTC 1 day BigQueryOperator '''
btc_daily_operator = BigQueryOperator(
    task_id='bq_btc_daily_operator',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    SELECT 
        TIMESTAMP_SECONDS(86400 * DIV(UNIX_SECONDS(created_at) + 450, 86400)) as timestamp,
        symbol,
        ARRAY_AGG(price ORDER BY created_at LIMIT 1)[SAFE_OFFSET(0)] open,
        MAX(price) high,
        MIN(price) low,
        ARRAY_AGG(price ORDER BY created_at DESC LIMIT 1)[SAFE_OFFSET(0)] close
    FROM `composer-236006.crypto.ticks`
    WHERE symbol ='BTC'
    GROUP BY symbol, timestamp
    ORDER BY symbol, timestamp DESC
    ''',    destination_dataset_table='composer-236006.crypto.ohlc1d',
    dag=dag)


crypto_pull_rates_operator.set_upstream(start_operator)

btc_five_minutes_operator.set_upstream(crypto_pull_rates_operator)
btc_fifteen_minutes_operator.set_upstream(crypto_pull_rates_operator)
btc_thirty_minutes_operator.set_upstream(crypto_pull_rates_operator)
btc_one_hour_operator.set_upstream(crypto_pull_rates_operator)
btc_four_hour_operator.set_upstream(crypto_pull_rates_operator)
btc_daily_operator.set_upstream(crypto_pull_rates_operator)

btc_five_minutes_operator_rc.set_upstream(btc_five_minutes_operator)

end_operator.set_upstream(btc_five_minutes_operator_rc)
end_operator.set_upstream(btc_fifteen_minutes_operator)
end_operator.set_upstream(btc_thirty_minutes_operator)
end_operator.set_upstream(btc_one_hour_operator)
end_operator.set_upstream(btc_four_hour_operator)
end_operator.set_upstream(btc_daily_operator)
