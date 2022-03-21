# Zoomcamp Project Proposal

## Problem description - Spotify Monitoring
(This is a hypothetical and synthetic requirement formulated for the zoomcamp project).

Spotify business wishes to review popular tracks on a period basis (monthly/yearly) to help create features such as "Currently trending", "2021 Top Hits", "Trending artists" etc., To solve this need, the data engineering / analytics need to create a pipeline to convert the raw data collected for popularity into actionable dashboards for the business team to analyze and curate to form the "Top Hits", "Trending" etc.,   

The pipeline refreshes data on a monthly basis with popularity information, pulling the raw data into data lake first for storage, extracting and transforming into trending data structure in cloud for easier dashboard construction from which business intelligence for trending and top hits can be easily obtained in a structured form. 

## Project high level design
This project produces a pipeline which:

1. Uses Terraform to manage the infrastructure
2. pull the raw data into GCP cloud
3. Transforms the raw data into standard tables
4. Joins the artists and tracks table to provide popularity metrics via dbt and write them back into BigQuery
5. Produce dashboard tiles in Google Data studio.
6. This allows the analytics to view the combined tracks and artists popularity information for quick review.

> Insert pipeline diagram here

## Dataset
[Spotify Dataset](https://www.kaggle.com/yamaerenay/spotify-dataset-19212020-600k-tracks?select=tracks.csv)

## References
1. [Rubric](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_7_project)

## Technology choices
1. Cloud: GCP
2. Datalake: GCP Bucket
3. Infrastructure as code (IaC): Terraform 
4. Workflow orchestration: Airflow 
5. Data Warehouse: BigQuery 
6. Transformations: dbt

## Proposal to address the requirements
1. **Data ingestion** - Using Airflow to download the dataset and place it in GCP Bucket
2. **Data warehouse** - BigQuery will be used to host the tables
3. **Transformations** - Use dbt to convert transform the data from GCP bucket and add to BigQuery (partitioned and clustered)
4. **Dashboard** - Use Google Data studio to build the dashboards 

## Dashboard Tiles
1. Total number of tracks
2. Total number of artists
3. Most popular song - by popularity
4. Most popular artist - by followers
5. Most Tracks - Sort by Artist