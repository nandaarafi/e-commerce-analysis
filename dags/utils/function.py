import os

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from config import GCS_BUCKET_NAME, GCP_CONN_ID

def upload_to_gcs(data_folder, gcs_path,**kwargs):
    bucket_name = GCS_BUCKET_NAME
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
            gcp_conn_id=GCP_CONN_ID,
        )
        upload_task.execute(context=kwargs)