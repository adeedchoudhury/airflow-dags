from datetime import datetime, timedelta, time
import json
import logging

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor  # in Airflow 2.0 should be  "from airflow.sensors ..."
from pytz import timezone
from pandas.io.json import json_normalize
import pandas as pd


http_hook = HttpHook(method='GET', http_conn_id='http_digital_turbine_api')
mysql_hook = MySqlHook(mysql_conn_id='mysql_us_marketing')

default_args = {
    'owner': xxx,
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2018, 5, 2),
    'email': xxx,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


def reformat(df, date):
     
    # Unpack data column and create final DataFrame
    df1 = df['campaigns'][0]
    df2 = pd.DataFrame(df1)
    df3 = pd.DataFrame(df2.countries.values.tolist(), index=df2.index, columns=['data'])
    objs = [df3, pd.DataFrame(df3['data'].tolist()).iloc[:, :100]]
    df4 = pd.concat(objs, axis=1).drop('data', axis=1)
    final_df = df2.join(df4)

    # Add date to DataFrame
    final_df['date'] = date

    # Add additional columns
    final_df['network'] = 'digital_turbine'

    # Replace None with '\N', which gets interpreted as a NULL with LOAD DATA INFILE,
    # which gets called under the hood with mysql_hook.bulk_load
    final_df = final_df.fillna('\\N')
    return final_df


def download(tmp_file, **kwargs):
    # This DAG is ran on a @daily scheduled_interval. This means that a run stamped
    # 2017-01-01 will be triggered soon after 2017-01-01T23:59. In other words,
    # the job instance is started once the period it covers has ended.
    # See https://airflow.apache.org/scheduler.html for more details.

    # Since we want yesterday's data to be ready by 7AM in New York,
    # and since Airflow's execution_date stamp is "one day late" as described above,
    # this means that we should request "today"'s data from Digital Turbine.
    today = today_est_date(kwargs['execution_date'])

    request_data = {
        'startDate': today,
        'endDate': today,
    }
    headers = {
        'Authorization': 'Basic XXX',
    }

    logging.info('Sending request to Digital Turbine API with data: {}'.format(request_data))
    response = http_hook.run(
        endpoint='/api/report/advertiser/v1/performance.json',
        data=request_data,
        headers=headers,
    )
    data = json.loads(response.content)

    try:
        df = pd.DataFrame(json_normalize(data['performance']))
        df = reformat(df, today)
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
            'campaignName',
            'campaignId',
            'spend',
            'installCount',
            'preloadCount',
            'clickCount',
            'countryCode',
            'cpi',
            'ctr',
        ]
    )


def insert(table, tmp_file, **kwargs):
    today = today_est_date(kwargs['execution_date'])

    mysql_hook.run("DELETE FROM {} WHERE date = '{}'".format(table, today))
    mysql_hook.bulk_load(table=table, tmp_file=tmp_file)


def as_date(dt):
    return dt.isoformat()[:10]


def today_est_date(dt):
    return as_date(timezone('EST').localize(dt))


with DAG('etl_digital_turbine', default_args=default_args, schedule_interval='@daily', concurrency=10, max_active_runs=1) as dag:
    table = 'daily_stats_digital_turbine'
    tmp_file = '/tmp/digital_turbine.tsv'

    (
        TimeSensor(
            task_id='hold_on',
            target_time=time(hour=12),  # 7AM EST
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
