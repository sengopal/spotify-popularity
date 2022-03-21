import logging
import os
from datetime import datetime

import pyarrow.csv as pv
import pyarrow.parquet as pq
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

AIRFLOW_HOME = "/opt/airflow/"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
TABLE_NAME_TEMPLATE = 'taxi_zone_lookup'
OUTPUT_FILE_CSV = AIRFLOW_HOME + TABLE_NAME_TEMPLATE + ".csv"
OUTPUT_FILE_PARQUET = AIRFLOW_HOME + TABLE_NAME_TEMPLATE + ".parquet"

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return

    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def download_data(url, src_file):
    print(f"Performing {url} > {src_file} - started")

    r = requests.get(url)

    if r.status_code == 200:
        with open(src_file, 'wb') as f:
            f.write(r.content)
    else:
        raise ValueError(f'file_not_found: {url}')

    print(f"Performing {url} > {src_file} - completed")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
        dag_id="data_ingestion_zone_v01",
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=['dtc-de'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_data,
        op_kwargs={
            "url": f"{URL_TEMPLATE}",
            "src_file": f"{OUTPUT_FILE_CSV}"
        }
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{OUTPUT_FILE_CSV}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{TABLE_NAME_TEMPLATE}.parquet",
            "local_file": f"{OUTPUT_FILE_PARQUET}",
        },
    )

    cleanup_local_task = BashOperator(
        task_id="cleanup_local_task",
        bash_command=f"rm {OUTPUT_FILE_CSV};rm {OUTPUT_FILE_PARQUET}; "
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #         },
    #     },
    # )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> cleanup_local_task # >> bigquery_external_table_task
