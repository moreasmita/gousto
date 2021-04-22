from airflow import DAG, macros
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import requests
import os

default_args = {
    'owner': 'Asmita More',
    'depends_on_past': False,
    'start_date': datetime(2014,9,15), 
    'email': ['more.asmita11@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG(
    'gousto_daily_report_v1', 
    default_args = default_args,
    description = 'Gousto Data Engineering Test',
    schedule_interval = '0 0 * * *',    
    max_active_runs = 1,
    catchup = True
)
 
def http_download_data(**kwargs): 
    counter = 0 
    attempts = 4
    download_path = '/home/airflow/gcs/data/input_data/events.gz'
    url = 'https://s3-eu-west-1.amazonaws.com/gousto-hiring-test/data-engineer/events.gz'

    while counter < attempts :
        response = requests.get(url)
        if response.status_code != 200:
            counter = counter+1
            response = requests.get(url)
            print("retry",counter)
            return "data_unavailable"
        else:
            open(download_path, 'wb').write(response.content)
            return "load_event_csv"

def data_unavailable(**kwargs):
    print("Send Alert...")
    # Can send Notification to EMAIL(email) or SLACK (from python library SlackClient)
    

start = DummyOperator(
    # Dummy start operator
    task_id='start',
    dag=dag
)

get_data = PythonOperator(
    # Download data from http URL
    task_id = "get_data",
    python_callable = http_download_data,
    provide_context=True,
    dag = dag,
)

load_event_csv = GoogleCloudStorageToBigQueryOperator(
    # Bigquery initail 'events' table createion on top of GCS bucket
    task_id='load_event_csv',
    bucket='europe-west1-gousto-exam-eff7d006-bucket',
    source_objects=['data/input_data/*'],
    destination_project_dataset_table='gousto.events',
    schema_fields=[
        {'name': 'event_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp_', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'user_fingerprint', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'domain_userid', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'network_userid', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'page', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    dag=dag)

set_date_to_daily_report = BigQueryOperator(
    # Adding initial entry as date for daily report numbers update
    task_id = "set_date_to_daily_report",
    sql = """ INSERT INTO gousto.daily_report (date_,entry_timestamp) VALUES ('{{ ds }}',CURRENT_TIMESTAMP()); """,
    dag = dag,
    use_legacy_sql = False,
)

active_user = BigQueryOperator( 
    # Number of active users​ (users who visited our website on day X and day X-1)
    task_id = "active_user",
    sql = """
            CREATE TABLE gousto.active_users AS 
            SELECT
            DISTINCT(user_fingerprint) AS user_fingerprint, DATE(timestamp_) AS date_ 
            FROM gousto.events 
            WHERE DATE(timestamp_) >= '{{ macros.ds_add(ds, -1) }}'  
            AND   DATE(timestamp_) <= '{{ ds }}'
            AND user_fingerprint != '';
  
            UPDATE gousto.daily_report a SET active_user = (
                SELECT COUNT(DISTINCT(user_fingerprint)) FROM gousto.active_users) WHERE date_ = '{{ ds}}';
            
        """,
    dag = dag,
    use_legacy_sql = False,
)

inactive_user = BigQueryOperator(
    # Number of inactive users (users who didn't visit our website on day X and neither on day X-1)
    task_id = "inactive_user",
    sql = """
            CREATE TABLE gousto.inactive_users AS 
            SELECT i.user_fingerprint 
            FROM gousto.events i
            LEFT JOIN gousto.active_users a
            ON i.user_fingerprint = a.user_fingerprint
            WHERE a.user_fingerprint IS NULL
            AND i.user_fingerprint != '';
  
            UPDATE gousto.daily_report a SET inactive_user = (
                SELECT COUNT(DISTINCT(user_fingerprint)) FROM gousto.inactive_users) WHERE date_ = '{{ ds}}';
            
        """,
    dag = dag,
    use_legacy_sql = False,
)

churn_user = BigQueryOperator(
    # Number of churned users (users who visited our website on day X-1, but didn't visit on day X)
    task_id = "churn_user",
    sql = """
            CREATE TABLE gousto.churn_users AS
            SELECT DISTINCT(prev_day.user_fingerprint)
            FROM gousto.active_users prev_day
            LEFT JOIN gousto.active_users curr_day
            ON curr_day.user_fingerprint = prev_day.user_fingerprint
            AND curr_day.date_ = DATE_ADD(prev_day.date_,INTERVAL 1 DAY)
            WHERE curr_day.user_fingerprint IS NULL
            AND prev_day.date_ = '{{ ds}}' ;

            UPDATE gousto.daily_report a SET churn_user = (
                SELECT COUNT(DISTINCT(user_fingerprint)) FROM gousto.churn_users) WHERE date_ = '{{ ds}}';
            
        """,
    dag = dag,
    use_legacy_sql = False,
)

reactive_users = BigQueryOperator(
    # Number of reactivated users (users who visited our website on day X, but didn't visit on day X-1)
    task_id = "reactive_users",
    sql = """
            CREATE TABLE gousto.fisrt_active AS
            SELECT DISTINCT user_fingerprint,
            MIN(DATE(timestamp_)) AS date_
            FROM gousto.events
            GROUP BY user_fingerprint;

            CREATE TABLE gousto.reactivate_users AS
            SELECT DISTINCT(curr_day.user_fingerprint),curr_day.date_
            FROM gousto.active_users curr_day
            LEFT JOIN gousto.active_users prev_day
            ON curr_day.user_fingerprint = prev_day.user_fingerprint
            AND curr_day.date_ = DATE_ADD(prev_day.date_,INTERVAL 1 DAY)
            JOIN gousto.fisrt_active firs
            ON curr_day.user_fingerprint= firs.user_fingerprint
            AND firs.date_ != curr_day.date_
            WHERE prev_day.user_fingerprint IS NULL
            AND curr_day.date_ = '{{ ds}}';


            UPDATE gousto.daily_report a SET reactive_user = (
                SELECT COUNT(DISTINCT(user_fingerprint)) FROM gousto.reactivate_users) WHERE date_ = '{{ ds}}';
            

        """,
    dag = dag,
    use_legacy_sql = False,
)

clean_bq_table = BigQueryOperator(
    # Clean up of existing table for next run
    task_id = "clean_bq_table",
    sql = """
            DROP TABLE gousto.active_users;
            DROP TABLE gousto.inactive_users;
            DROP TABLE gousto.churn_users;
            DROP TABLE gousto.reactivate_users;
            DROP TABLE gousto.fisrt_active;
        """,
    dag = dag,
    use_legacy_sql = False,
)

data_unavailable = PythonOperator(
    # Sending Alert if data does not get downloaded
    task_id='data_unavailable',
    python_callable=data_unavailable,
    dag=dag )

end = DummyOperator(
    # Dummy end operator
    task_id='end',
    dag=dag
)    


start >> get_data >> data_unavailable >> end

start >> get_data >> load_event_csv >> set_date_to_daily_report >> active_user >> inactive_user >> churn_user >> reactive_users >> clean_bq_table >> end 
