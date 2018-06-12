from datetime import datetime, timedelta, time
import json
import logging

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor  # in Airflow 2.0 should be  "from airflow.sensors ..."
from pandas.io.json import json_normalize
import pandas as pd


http_hook = HttpHook(method='GET', http_conn_id='http_iron_source_api')
mysql_hook = MySqlHook(mysql_conn_id='mysql_us_marketing')

default_args = {
    'owner': XXX,
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2018, 5, 7),
    'email': XXX,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


def reformat(df, date):
    
    # Unpack data column and create final DataFrame
    df1 = pd.DataFrame(df.data.values.tolist(), index= df.index, columns=['data'])
    objs = [df1, pd.DataFrame(df1['data'].tolist()).iloc[:, :100]]
    df3 = pd.concat(objs, axis=1).drop('data', axis=1)
    final_df = df.join(df3)

    final_df['spend'] = final_df['expense'] / 100

    # Add yesterday's date to DataFrame
    final_df['date'] = date

    # Add additional columns
    final_df['network'] = 'iron_source'

    # Replace None with '\N', which gets interpreted as a NULL with LOAD DATA INFILE,
    # which gets called under the hood with mysql_hook.bulk_load
    final_df = final_df.fillna('\\N')
    return final_df


def download(tmp_file, **kwargs):
    request_data = {
        'start_date': kwargs['ds'],
        'end_date': kwargs['ds'],
        'add_campaign_name': 1,
    }
    headers = {
        'Authorization': 'Basic XXX',
    }

    logging.info('Sending request to ironSource API with data: {}'.format(request_data))
    response = http_hook.run(
        endpoint='/partners/advertiser/campaigns/stats',
        data=request_data,
        headers=headers,
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
            'campaign_name',
            'campaign_id',
            'spend',
            'conversions',
            'clicks',
            'installs',
            'country_code',
        ]
    )


def insert(table, tmp_file, **kwargs):
    mysql_hook.run("DELETE FROM {} WHERE date = '{}'".format(table, kwargs['ds']))
    mysql_hook.bulk_load(table=table, tmp_file=tmp_file)


with DAG('etl_iron_source', default_args=default_args, schedule_interval='@daily', concurrency=10, max_active_runs=1) as dag:
    table = 'daily_stats_iron_source'
    tmp_file = '/tmp/iron_source.tsv'

    (
        TimeSensor(
            task_id='hold_on',
            target_time=time(hour=1),
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
