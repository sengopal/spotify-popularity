### references
1. [Install anaconda, docker and docker-compose on your VM using this script](https://gist.github.com/ankurchavda/367de721b6673762fab48375020df22b)

### Dataset notebook
1. https://www.kaggle.com/sengopal/notebook1ca5137186/edit

### Dataset direct link
https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks/download

`dict_artists.json` - The artists recommended for fans of artists. Each recommendation is sorted in descending order (The first one is the most favorite one). The number of recommendations are limited to 20.


### Spark and Airflow
Spark Install - https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/macos.md
Pyspark install - https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/pyspark.md
https://medium.com/codex/executing-spark-jobs-with-apache-airflow-3596717bbbe3
https://github.com/cordon-thiago/airflow-spark

### References

#### Big Query
bq mkdef --autodetect --source_format=CSV "gs://dtc_data_lake_fluent-tea-338517/spotify/tracks_artists_data.csv" > myschema
bq mk --external_table_definition=myschema spotify_monthly_data.tracks_artists_data_external_table


