from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/opt/airflow/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

DATAPROC_CLUSTER_NAME = os.getenv('DATAPROC_CLUSTER_NAME')
DATAPROC_REGION = os.getenv('DATAPROC_REGION')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
PYSPARK_JOB_LOCATION = os.getenv('PYSPARK_JOB_LOCATION')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
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
                'internal_ip_only': False,  # Set this to False
            },

}


PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f'{PYSPARK_JOB_LOCATION}/transform_data.py'},
}

default_args = {
    'owner': 'Name',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    "dataproc_airflow_gcp_to_gbq",
    schedule_interval=None,
    catchup=False,
    tags=["dataproc_airflow"],
    default_args=default_args

) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        gcp_conn_id=GCP_CONN_ID
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID
    )

    gsc_to_gbq_product = GCSToBigQueryOperator(
        task_id="transfer_product_data_to_bigquery",
        bucket=GCS_BUCKET_NAME,
        source_objects =[f"{TRANSFORMED_RESOURCES_PATH}product_info/*.csv"],
        destination_project_dataset_table =f"{GCP_PROJECT_ID}:e_commerce_dw.product", # bigquery table
        source_format = "csv",
        gcp_conn_id=GCP_CONN_ID,
        project_id=GCP_PROJECT_ID,
        write_disposition="write_append"
    )
    gsc_to_gbq_reviews = GCSToBigQueryOperator(
        task_id="transfer_review_data_to_bigquery",
        bucket=GCS_BUCKET_NAME,
        source_objects =[f"{TRANSFORMED_RESOURCES_PATH}reviews/*.csv"],
        destination_project_dataset_table ="e_commerce_dw.review", # bigquery table
        source_format = "csv",
        gcp_conn_id=GCP_CONN_ID,
        project_id=GCP_PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=DATAPROC_REGION,
        gcp_conn_id=GCP_CONN_ID
    )



    # create_cluster >> submit_job >> [delete_cluster,gsc_to_gbq_product, gsc_to_gbq_reviews]