locals {
  data_lake_bucket = "dtc_data_lake" # Bucket name identifier - The actual full name of bucket is created in main.tf
}

variable "project" {
  description = "fluent-tea-338517" # Project ID
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "spotify_monthly_data" # BigQuery db for all raw and generated tables
}
