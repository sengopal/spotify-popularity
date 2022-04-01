import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

AIRFLOW_HOME = "/opt/airflow/"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'spotify_monthly_data')

DOWNLOAD_URL = "https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks/download"
ZIP_FILE_NAME = AIRFLOW_HOME + "archive.zip"

ARTISTS_FILE_NAME = "artists"
TRACK_FILE_NAME = "tracks"
FILE_NAMES = [ARTISTS_FILE_NAME, TRACK_FILE_NAME]
CLUSTER_COL = 'artist_popularity'

TRANSFORM_FILE_NAME = "tracks_artists_data"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 3, 1),
    "depends_on_past": False,
    "retries": 1,
    "schedule_interval": "@monthly",
}


def transform_data(output_file):
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[2] pyspark-shell"
    # conf = SparkConf().set("spark.jars", "./spark-bigquery-with-dependencies_2.12-0.23.2.jar")
    # spark = SparkSession.builder.master('local').appName('bq').config('spark.driver.extraClassPath', 'spark-bigquery-with-dependencies_2.12-0.23.2.jar').getOrCreate()
    spark = SparkSession.builder.master('local').appName('bq').config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2").getOrCreate()
    df_tracks = spark.read.format('bigquery').load('fluent-tea-338517.spotify_monthly_data.tracks_data')

    df_artists = spark.read.format('bigquery').load('fluent-tea-338517.spotify_monthly_data.artists_data')

    # tracks data preparation
    df_tracks = df_tracks.withColumnRenamed("id", "track_id").withColumnRenamed("name", "track_name").withColumnRenamed("popularity", "track_popularity")

    # artists data preparation
    df_artists = df_artists.withColumnRenamed("id", "artist_id").withColumnRenamed("name", "artist_name").withColumnRenamed("popularity", "artist_popularity")

    # Fix the tracks to explode for the artist_id
    df_tracks = df_tracks.withColumn('cleaned_artist_id', F.explode(F.split(F.regexp_replace('id_artists', "['\[\] ]", ""), ",")))

    # Selecting only the exploded columns and the other required columns
    df_tracks = df_tracks.select([c for c in df_tracks.columns if c not in {'artists', 'id_artists'}])

    # tracks_artists_data
    df_tracks = df_tracks.alias('df_tracks')
    df_artists = df_artists.alias('df_artists')

    df_tracks_artists_data = df_tracks.join(df_artists, df_tracks.cleaned_artist_id == df_artists.artist_id).select('df_tracks.*', 'df_artists.*')
    df_tracks_artists_data = df_tracks_artists_data.select([c for c in df_tracks_artists_data.columns if c not in {'cleaned_artist_id'}])
    df_tracks_artists_data = df_tracks_artists_data.fillna({'followers': 0})

    # df_tracks_artists_data.repartition(1).write.csv(f'{AIRFLOW_HOME}{output_file}.csv')
    df_tracks_artists_data.toPandas().to_csv(f'{AIRFLOW_HOME}{output_file}', index=False, header=False)

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


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
        dag_id="spark_transformation_v10",
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=['dtc-de'],
) as dag:
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        op_kwargs={
            "output_file": f"{TRANSFORM_FILE_NAME}.csv"
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id=f"{TRANSFORM_FILE_NAME}_local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "src_location": f"{AIRFLOW_HOME}",
            "local_file": f"{TRANSFORM_FILE_NAME}.csv",
            "bucket": BUCKET,
            "object_name": f"spotify/{TRANSFORM_FILE_NAME}.csv",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"{TRANSFORM_FILE_NAME}_bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{TRANSFORM_FILE_NAME}_external_table",
            },
            "schema": {
                "fields": [
                    {"name": "track_id", "type": "STRING"},
                    {"name": "track_name", "type": "STRING"},
                    {"name": "track_popularity", "type": "INTEGER"},
                    {"name": "duration_ms", "type": "INTEGER"},
                    {"name": "explicit", "type": "INTEGER"},
                    {"name": "release_date", "type": "STRING"},
                    {"name": "danceability", "type": "FLOAT"},
                    {"name": "energy", "type": "FLOAT"},
                    {"name": "key", "type": "INTEGER"},
                    {"name": "loudness", "type": "FLOAT"},
                    {"name": "mode", "type": "INTEGER"},
                    {"name": "speechiness", "type": "FLOAT"},
                    {"name": "acousticness", "type": "FLOAT"},
                    {"name": "instrumentalness", "type": "FLOAT"},
                    {"name": "liveness", "type": "FLOAT"},
                    {"name": "valence", "type": "FLOAT"},
                    {"name": "tempo", "type": "FLOAT"},
                    {"name": "time_signature", "type": "INTEGER"},
                    {"name": "artist_id", "type": "STRING"},
                    {"name": "followers", "type": "FLOAT"},
                    {"name": "genres", "type": "STRING"},
                    {"name": "artist_name", "type": "STRING"},
                    {"name": "artist_popularity", "type": "INTEGER"}
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{BUCKET}/spotify/{TRANSFORM_FILE_NAME}.csv"],
            },
        }
    )

    CREATE_BQ_TBL_QUERY = (f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{TRANSFORM_FILE_NAME} \
                CLUSTER BY {CLUSTER_COL} \
                AS \
                SELECT * FROM {BIGQUERY_DATASET}.{TRANSFORM_FILE_NAME}_external_table;")

    # Create a partitioned table from external table
    bq_create_clustered_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_create_{TRANSFORM_FILE_NAME}_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    cleanup_local_task = BashOperator(
        task_id="cleanup_local_task",
        bash_command=f"cd {AIRFLOW_HOME}; rm {TRANSFORM_FILE_NAME}.csv"
    )

    transform_task >> local_to_gcs_task >> bigquery_external_table_task >> bq_create_clustered_table_task >> cleanup_local_task
