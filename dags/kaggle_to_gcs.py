from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from utils.config import LOCAL_DATA_PATH, KAGGLE_CONFIG_DIR
from utils.function import upload_to_gcs


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
    bash_command=f'mkdir -p {LOCAL_DATA_PATH} && /home/airflow/.local/bin/kaggle datasets download -d nadyinky/sephora-products-and-skincare-reviews -p {LOCAL_DATA_PATH} --unzip',
    env={'KAGGLE_CONFIG_DIR': KAGGLE_CONFIG_DIR},
    dag=dag,
)

upload_to_gcs= PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_args=[f'{LOCAL_DATA_PATH}', 'raw_data'],
    provide_context=True,
)

download_kaggle_data >> upload_to_gcs