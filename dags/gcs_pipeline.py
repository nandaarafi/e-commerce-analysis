from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

local_data_path = '/opt/airflow/raw_data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    "retry_delay": timedelta(minutes=5),

}

dag = DAG(
    'upload_to_gcs',
    default_args=default_args,
    description='A simple DAG to upload files to GCS',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# dataset_list_check = BashOperator(
#     task_id='list_fraud_detection_datasets',
#     bash_command="/home/airflow/.local/bin/kaggle datasets list -s 'fraud detection'",
#     env={'KAGGLE_CONFIG_DIR': '/opt/airflow/credentials'},
#     dag=dag,
# )
download_kaggle_data = BashOperator(
    task_id='download_kaggle_data',
    bash_command=f'mkdir -p {local_data_path} && /home/airflow/.local/bin/kaggle datasets download -d nadyinky/sephora-products-and-skincare-reviews -p {local_data_path} --unzip',
    env={'KAGGLE_CONFIG_DIR': '/opt/airflow/credentials'},
    dag=dag,
)

upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=f'{local_data_path}/*.csv',
        dst='raw_data/',
        bucket="data-23539",
        gcp_conn_id='google_cloud_main',
        dag=dag
)

# Set the task sequence
download_kaggle_data >> upload_file