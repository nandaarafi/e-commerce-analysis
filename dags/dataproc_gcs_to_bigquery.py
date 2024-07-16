from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from utils.config import (CLUSTER_CONFIG,
                          PYSPARK_JOB,
                          default_args,
                          GCS_BUCKET_NAME,
                          TRANSFORMED_RESOURCES_PATH,
                          GCP_PROJECT_ID,
                          DATAPROC_CLUSTER_NAME,
                          DATAPROC_REGION,
                          GCP_CONN_ID)

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
        source_objects =[f"{TRANSFORMED_RESOURCES_PATH}product_info/*.snappy.parquet"],
        destination_project_dataset_table =f"{GCP_PROJECT_ID}:e_commerce_dw.product", # bigquery table
        source_format = "parquet",
        gcp_conn_id=GCP_CONN_ID,
        project_id=GCP_PROJECT_ID,
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,

    )

    gsc_to_gbq_reviews = GCSToBigQueryOperator(
        task_id="transfer_review_data_to_bigquery",
        bucket=GCS_BUCKET_NAME,
        source_objects =[f"{TRANSFORMED_RESOURCES_PATH}reviews/*.snappy.parquet"],
        destination_project_dataset_table ="e_commerce_dw.review", # bigquery table
        source_format = "parquet",
        gcp_conn_id=GCP_CONN_ID,
        project_id=GCP_PROJECT_ID,
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=DATAPROC_REGION,
        gcp_conn_id=GCP_CONN_ID
    )

    create_cluster >> submit_job >> [gsc_to_gbq_product, gsc_to_gbq_reviews] >> delete_cluster
