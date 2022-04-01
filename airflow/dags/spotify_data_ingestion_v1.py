import os
import zipfile
from datetime import datetime

import pyarrow.csv as pv
import pyarrow.parquet as pq
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from google.cloud import storage

AIRFLOW_HOME = "/opt/airflow/"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'spotify_monthly_data')

# DOWNLOAD_URL = "https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks/download"
DOWNLOAD_URL = "https://www.dropbox.com/s/w6mrtd95tvhugq4/archive.zip?dl=0"
ZIP_FILE_NAME = AIRFLOW_HOME + "archive.zip"

ARTISTS_FILE_NAME = "artists"
TRACK_FILE_NAME = "tracks"
FILE_NAMES = [ARTISTS_FILE_NAME, TRACK_FILE_NAME]
CLUSTER_COL = 'popularity'


# def download_data(url, src_file):
#     print(f"Performing {url} > {src_file} - started")
#
#     r = requests.get(url)
#
#     if r.status_code == 200:
#         with open(src_file, 'wb') as f:
#             f.write(r.content)
#     else:
#         raise ValueError(f'file_not_found: {url}')
#
#     print(f"Performing {url} > {src_file} - completed")


def unzip_archive(src_file):
    print(f"Performing {src_file} unzip- started")

    with zipfile.ZipFile(src_file, 'r') as zip_ref:
        zip_ref.extractall(AIRFLOW_HOME)

    print(f"Performing {src_file} unzip- completed")


def format_to_parquet(location, src_file):
    table = pv.read_csv(location + src_file + ".csv")
    pq.write_table(table, (src_file + '.parquet'))


def upload_to_gcs(src_location, local_file, bucket, object_name):
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
    blob.upload_from_filename(src_location + local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 3, 1),
    "depends_on_past": False,
    "retries": 1,
    "schedule_interval": "@monthly",
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
        dag_id="spotify_data_ingestion_v1",
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=['dtc-de'],
) as dag:
    # download_archive_task = GCSToLocalFilesystemOperator(
    #     task_id="download_file_task",
    #     object_name=f"raw/archive.zip",
    #     bucket=BUCKET,
    #     filename=f"{ZIP_FILE_NAME}"
    # )


    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"wget {DOWNLOAD_URL} -O {ZIP_FILE_NAME}"
    )

    # download_dataset_task = PythonOperator(
    #     task_id="download_dataset_task",
    #     python_callable=download_data,
    #     op_kwargs={
    #         "url": f"{DOWNLOAD_URL}",
    #         "src_file": f"{ZIP_FILE_NAME}"
    #     }
    # )

    unzip_archive_task = PythonOperator(
        task_id="unzip_archive_task",
        python_callable=unzip_archive,
        op_kwargs={
            "src_file": f"{ZIP_FILE_NAME}"
        }
    )

    parquet_tasks = []
    local_to_gcs_tasks = []

    for file_name in FILE_NAMES:
        format_to_parquet_task = PythonOperator(
            task_id=f"{file_name}_format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "location": f"{AIRFLOW_HOME}",
                "src_file": f"{file_name}"
            },
        )

        parquet_tasks.append(format_to_parquet_task)

        local_to_gcs_task = PythonOperator(
            task_id=f"{file_name}_local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "src_location": f"{AIRFLOW_HOME}",
                "local_file": f"{file_name}.parquet",
                "bucket": BUCKET,
                "object_name": f"spotify/{file_name}.parquet",
            },
        )
        local_to_gcs_tasks.append(local_to_gcs_task)

    bigquery_tasks = []

    for file_name in FILE_NAMES:
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"{file_name}_bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{file_name}_external_table",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/spotify/{file_name}.parquet"],
                },
            },
        )

        bigquery_tasks.append(bigquery_external_table_task)

    table_tasks = []

    for file_name in FILE_NAMES:
        CREATE_BQ_TBL_QUERY = (f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{file_name}_data \
            CLUSTER BY {CLUSTER_COL} \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{file_name}_external_table;")

        # Create a partitioned table from external table
        bq_create_clustered_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{file_name}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        table_tasks.append(bq_create_clustered_table_job)

    cleanup_local_tasks = []
    for file_name in FILE_NAMES:
        cleanup_local_task = BashOperator(
            task_id=f"{file_name}_cleanup_local_task",
            bash_command=f"cd ${AIRFLOW_HOME}; rm -f {file_name}.csv; rm -f {file_name}.parquet; rm -f {ZIP_FILE_NAME};"
        )

        cleanup_local_tasks.append(cleanup_local_task)

    download_dataset_task >> unzip_archive_task >> parquet_tasks

    parquet_tasks[0] >> local_to_gcs_tasks[0] >> bigquery_tasks[0] >> table_tasks[0] >> cleanup_local_tasks[0]
    parquet_tasks[1] >> local_to_gcs_tasks[1] >> bigquery_tasks[1] >> table_tasks[1] >> cleanup_local_tasks[1]
