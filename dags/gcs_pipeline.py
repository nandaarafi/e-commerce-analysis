import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

local_data_path = '/opt/airflow/raw_data'
def upload_to_gcs(data_folder,gcs_path,**kwargs):
    bucket_name = 'data-23539'  # Your GCS bucket name
    gcs_conn_id = 'google_cloud_main'

    # List all CSV files in the data folder
    # Note : you can filter the files extentions with file.endswith('.csv')
    # Examples : file.endswith('.csv')
    #            file.endswith('.json')
    #            file.endswith('.csv','json')

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
            gcp_conn_id=gcs_conn_id,
        )
        upload_task.execute(context=kwargs)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
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

upload_to_gcs= PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_args=[f'{local_data_path}', 'raw_data'],
    provide_context=True,
)
# upload_file = LocalFilesystemToGCSOperator(
#         task_id="upload_file",
#         src=f'{local_data_path}/*.csv',
#         dst='raw_data/',
#         bucket="data-23539",
#         gcp_conn_id='google_cloud_main',
#         dag=dag
# )

# Set the task sequence
download_kaggle_data >> upload_to_gcs