from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('first_dag',
          default_args=default_args,
          description='A simple tutorial DAG',
          schedule_interval=timedelta(days=1),
          catchup=False) as dag:

    t1 = DummyOperator(
        task_id='start_SUCCESSFULLY',
        dag=dag
    )
    print('>>>>>>>>>>>>>>>>>>>>>')

    t2 = DummyOperator(
        task_id='end_SUCCESSFULLY',
        dag=dag
    )
    
    t3 = DummyOperator(
        task_id='FINAL_SUCCESS',
        dag=dag
    )

    t1 >> t2 >> t3
