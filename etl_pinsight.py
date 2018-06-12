from datetime import datetime, timedelta, time
import json
import logging

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor  # in Airflow 2.0 should be  "from airflow.sensors ..."
from pandas.io.json import json_normalize
import pandas as pd


http_hook = HttpHook(method='GET', http_conn_id='http_pinsight_api')
mysql_hook = MySqlHook(mysql_conn_id='mysql_us_marketing')

default_args = {
    'owner': XXX,
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2018, 5, 10),
    'email': XXXX,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def reformat(df, date):
    # Add yesterday's date and network_name
    df['date'] = date
    df['network'] = 'pinsight'

    # Both 'cost' and 'conversions' are given as strings
    df['cost'] = df['cost'].replace('[\$,]', '', regex=True).astype(float)
    df['conversions'] = df['conversions'].replace(',', '', regex=True).astype(int)
    return df


def download(tmp_file, **kwargs):
    # Refer to http://partners.pinsightmedia.com/stats/stats_api#stats
    request_data = {
        'start_date': kwargs['ds'],
        'end_date': kwargs['ds'],
        'api_key': Variable.get('pinsight_api_key'),
    }

    logging.info('Sending request to Pinsight API with start_date={} and end_date={}'.format(
        request_data['start_date'], request_data['end_date'])
    )
    response = http_hook.run(
        endpoint='/stats/stats.json',
        data=request_data,
    )
    data = json.loads(response.content)

    try:
        df = pd.DataFrame(json_normalize(data['data']))
        df = reformat(df, kwargs['ds'])
    except Exception as e:
        msg = 'Failed to process downloaded data'
        logging.exception(msg)
        raise ValueError(msg)

    if df.empty:
        raise ValueError('No records found.')

    # Write out DataFrame to TSV file
    df.to_csv(
        path_or_buf=tmp_file,
        sep='\t',
        header=False,
        index=False,
        columns=[
            'date',
            'network',
            'cost',
            'conversions'
        ]
    )


def insert(table, tmp_file, **kwargs):
    mysql_hook.run("DELETE FROM {} WHERE date = '{}'".format(table, kwargs['ds']))
    mysql_hook.bulk_load(table=table, tmp_file=tmp_file)


with DAG('etl_pinsight', default_args=default_args, schedule_interval='@daily', concurrency=10, max_active_runs=1) as dag:
    table = 'daily_stats_pinsight'
    tmp_file = '/tmp/pinsight.tsv'

    (
        TimeSensor(
            task_id='hold_on',
            target_time=time(hour=2),
            dag=dag
        )
        >> PythonOperator(
            task_id='download',
            python_callable=download,
            op_args=[
                tmp_file,
            ],
            # The following is required to pass macros to the PythonOperator
            # See https://stackoverflow.com/a/45870153
            provide_context=True,
            dag=dag
        )
        >> PythonOperator(
            task_id='insert_to_mysql',
            python_callable=insert,
            op_args=[
                table,
                tmp_file,
            ],
            # The following is required to pass macros to the PythonOperator
            # See https://stackoverflow.com/a/45870153
            provide_context=True,
            dag=dag
        )
        >> BashOperator(
            task_id='remove_tmp_file',
            bash_command='rm -f {}'.format(tmp_file),
            dag=dag
        )
    )
