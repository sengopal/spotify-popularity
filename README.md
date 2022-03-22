# spotify-popularity
spotify-popularity - A Data Engineering project to demonstrate pipelines for Data Engineering Zoomcamp.

### Steps
1. Build the image (only first-time, or when there's any change in the `Dockerfile`):
Takes ~15 mins for the first-time
```shell
docker-compose build

2. Initialize the Airflow scheduler, DB, and other config
```shell
docker-compose up airflow-init
```

3. Kick up the all the services from the container:
```shell
docker-compose up
```

4. Login to Airflow web UI on `localhost:8080` with default creds: `airflow/airflow`

5. Run your DAG on the Web Console.

6. On finishing your run or to shut down the container/s:
```shell
docker-compose down
```