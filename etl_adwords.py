from datetime import datetime, timedelta, time

from airflow import DAG
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.operators.sensors import TimeSensor  # in Airflow 2.0 should be  "from airflow.sensors ..."


default_args = {
    'owner': XXX,
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 1),
    'email': XXX,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG('etl_adwords', default_args=default_args, schedule_interval='@daily', max_active_runs=1) as dag:
    (
        TimeSensor(
            task_id='hold_on',
            target_time=time(hour=3),
            dag=dag
        )
        >> ECSOperator(
            task_id='run_ecs',
            task_definition='airflow-etl-adwords',
            cluster='ecs',
            overrides={
                'containerOverrides': [
                    {
                        'name': 'app',
                        'environment': [
                            {
                                'name': 'EXECUTION_DATE',
                                'value': '{{ ds }}'
                            },
                            {
                                'name': 'RDS_ENDPOINT',
                                'value': 'XXX'
                            },
                            {
                                'name': 'RDS_USER',
                                'value': 'XXX'
                            },
                            {
                                'name': 'RDS_PASSWORD',
                                'value': 'XXX'
                            },
                            {
                                'name': 'RDS_DATABASE',
                                'value': 'XXX'
                            }
                        ]
                    }
                ]
            },
            aws_conn_id='',  # This is required or else it will throw an AttributeError ('NoneType' object has no attribute 'upper')
            region_name='ap-northeast-1',
            dag=dag
        )
    )
