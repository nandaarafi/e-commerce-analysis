import os

from airflow.utils.dates import days_ago
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/opt/airflow/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

DATAPROC_CLUSTER_NAME = os.getenv('DATAPROC_CLUSTER_NAME')
DATAPROC_REGION = os.getenv('DATAPROC_REGION')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
PYSPARK_JOB_LOCATION = os.getenv('PYSPARK_JOB_LOCATION')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
LOCAL_DATA_PATH = os.getenv('LOCAL_DATA_PATH')
KAGGLE_CONFIG_DIR = os.getenv('KAGGLE_CONFIG_DIR')
GCP_CONN_ID = 'google_cloud_main'
TRANSFORMED_RESOURCES_PATH = f'transformed_data/'


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    'gce_cluster_config': {
        'subnetwork_uri': 'default',
        'internal_ip_only': False,
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f'gs://data-23539/code/transform_data.py'},
}

default_args = {
    'owner': 'rafi',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}