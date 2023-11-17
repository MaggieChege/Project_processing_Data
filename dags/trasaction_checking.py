from datetime import datetime, timedelta
import boto3
import logging
from airflow import DAG
import sys



from airflow.operators.python_operator import PythonOperator
# from airflow.models import Variable
# import subprocess
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

sys.path.append('/opt/airflow/dags/process_transaction_data')
import process_transaction_data
# from process_transaction_data import CSVToDB, Transaction, db_conn

#logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

aws_access_key_id='X'
aws_secret_access_key= 'X'
# bucket = s3.Bucket('transactionlogging')
s3_bucket_name = 'transactionlogging'
s3_prefix = 'transactions-'
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Define the default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Define the Dag

dag = DAG(
          's3_check_file_and_process',
          default_args=default_args,
          description='A dag to check files in S3 and process it',
          schedule_interval=timedelta(days=1),  # TODO how to ensure it runs multiples times a day
)
def check_file_exists(**kwargs):

   
    
    current_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


    logger.info("Checking for the file in bucket '%s'", s3_bucket_name)
    response = s3_client.list_objects(Bucket=s3_bucket_name, Prefix=s3_prefix)
    file_key = None
    for obj in response.get('Contents', []):
        if current_date in obj['Key']:
            file_key = obj['Key']
            logger.info('Found file: %s', file_key)
            return file_key
    if file_key is None:
        logger.warning("file not found in S3 bucket.")
        
    return file_key

def run_python_script(**context):
    task_instance = context['task_instance']
    file_key = task_instance.xcom_pull(task_ids='check_file_exists')
    print("FILE::::", task_instance.xcom_pull(task_ids='check_file_exists'))
    if file_key:
        
        logger.info("Processing file: %s", file_key)
        db_conn = process_transaction_data.psycopg2.connect(
            dbname='airflow',
            user='airflow',
            password='airflow',
            host='postgres')
        processor = process_transaction_data.CSVToDB(db_conn, 'transaction', process_transaction_data.Transaction)
            # processor = CSVToDB(db_conn, 'transaction', Transaction)
        s3_path = f"s3://{s3_bucket_name}/{file_key}"
        obj = s3_client.get_object(Bucket=s3_bucket_name, Key=file_key)
        data = obj['Body'].read().decode('utf-8').splitlines(True)
        processor.process_csv(data)

    else:
        logger.info("No file to process or skipping processing.")
        
        
        
def send_slack_notification(**kwargs):
    execution_time = datetime.now().hour
    # current_time = datetime.now()
    
    if execution_time >= 10:
        return SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message='NO TRANSACTIONS FOUND AT 1PM',
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
        ).execute()
    
    
# Python Operator to check if file exists
check_file_task = PythonOperator(
    task_id ='check_file_exists',
    provide_context=True,
    python_callable=check_file_exists,
    dag=dag,
)

#Define PythonOperator to process the file
process_file_task = PythonOperator(
    task_id='process_file',
    provide_context=True,
    python_callable=run_python_script,
    dag=dag
    
)
#Define Python Operator to send slack notifaction if not file has arrived by 1PM
send_slack_notification_task = PythonOperator(
    task_id='send_slack_notification',
    provide_context=True,
    python_callable=send_slack_notification,
    dag=dag,
)
        
#set order of exceution

check_file_task >>process_file_task >> send_slack_notification_task


# check_file_task >>process_file_task
