from datetime import datetime, timedelta, time
import json
import logging

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor  # in Airflow 2.0 should be  "from airflow.sensors ..."
from pandas.io.json import json_normalize
import pandas as pd

mysql_hook = MySqlHook(mysql_conn_id='mysql_us_marketing')
http_hook = HttpHook(method='GET', http_conn_id='http_facebook_api')

default_args = {
    'owner': XXX,
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2018, 4, 1),
    'email': XXX,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


def reformat(df, date):


    df['network'] = 'facebook'
    df = df.drop(columns=['date_stop'])
    df = df.rename(index=str, columns={"date_start": "date"})


    # Replace None with '\N', which gets interpreted as a NULL with LOAD DATA INFILE,
    # which gets called under the hood with mysql_hook.bulk_load

    df = df.fillna('\\N')
    return df


def download(tmp_file, **kwargs):
    

    logging.info('Sending request to Facebook API to pull Ad Insights Data')
    
    payload = {'level': 'ad',
           'time_range[since]': kwargs['ds'],
           'time_range[until]' : kwargs['ds'],
           'limit':'3000',
           'access_token':Variable.get('facebook_access_token'),
           'fields': ['account_name,account_id,campaign_name,campaign_id,adset_name,adset_id,ad_name,ad_id,frequency,impressions,clicks,reach,spend']
           }

    url ='/v3.0/act_1441339432840272/insights'

    response = http_hook.run(
        endpoint=url,
        data=payload
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
            'account_name',
            'account_id',
            'campaign_name',
            'campaign_id',
            'adset_name',
            'adset_id',
            'ad_name',
            'ad_id',
            'frequency',
            'impressions',
            'clicks',
            'reach',
            'spend'
        ]
    )


def insert(table, tmp_file, **kwargs):
    mysql_hook.run("DELETE FROM {} WHERE date = '{}'".format(table, kwargs['ds']))
    mysql_hook.bulk_load(table=table, tmp_file=tmp_file)


with DAG('etl_facebook', default_args=default_args, schedule_interval='@daily', concurrency=10, max_active_runs=1) as dag:
    table = 'daily_stats_facebook'
    tmp_file = '/tmp/facebook.tsv'

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

 
