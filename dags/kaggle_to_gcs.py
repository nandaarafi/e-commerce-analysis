from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
from dotenv import load_dotenv
from pathlib import Path


dotenv_path = Path('/opt/airflow/resources/.env')
load_dotenv(dotenv_path=dotenv_path)
gcs_bucket_name = os.getenv('GCS_BUCKET_NAME')
local_data_path = os.getenv('LOCAL_DATA_PATH')
kaggle_config_dir = os.getenv('KAGGLE_CONFIG_DIR')
gcp_conn_id = 'google_cloud_main'


def upload_to_gcs(data_folder, gcs_path,**kwargs):
    bucket_name = gcs_bucket_name
    # List all CSV files in the data folder
    # Note : you can filter the files extentions with file.endswith('.csv')
    # Examples : file.endswith('.csv')
    #            file.endswith('.json')
    #            file.endswith('.csv')

    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

    # Upload each CSV file to GCS
    for csv_file in csv_files:
        local_file_path = os.path.join(data_folder, csv_file)
        gcs_file_path = f"{gcs_path}/{csv_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcp_conn_id,
        )
        upload_task.execute(context=kwargs)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    'kaggle_to_gcs',
    default_args=default_args,
    description='A simple DAG to upload files to GCS',
    schedule_interval='@daily',
    start_date=days_ago(0),
    catchup=False,
)

download_kaggle_data = BashOperator(
    task_id='download_kaggle_data',
    bash_command=f'mkdir -p {local_data_path} && /home/airflow/.local/bin/kaggle datasets download -d nadyinky/sephora-products-and-skincare-reviews -p {local_data_path} --unzip',
    env={'KAGGLE_CONFIG_DIR': kaggle_config_dir},
    dag=dag,
)

upload_to_gcs= PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_args=[f'{local_data_path}', 'raw_data'],
    provide_context=True,
)

download_kaggle_data >> upload_to_gcs