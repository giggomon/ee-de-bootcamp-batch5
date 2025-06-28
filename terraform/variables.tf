# Define variables to keep configuration flexible

variable "google_credentials" {
  description = "Path to the Google Cloud credentials JSON file"
  type        = string
  default     = "" # Empty by default for GitHub Action
}

variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
  default     = "ee-india-se-data"
}

variable "region" {
  description = "Default region for GCP resources"
  type        = string
  default     = "US"
}

variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
  default     = "se-data-landing-monica"
}

variable "bucket_location" {
  description = "Location for a GCS bucket"
  type        = string
  default     = "US"
}

variable "storage_class" {
  description = "The storage class of the bucket (e.g., STANDARD, NEARLINE, COLDLINE, ARCHIVE)"
  type        = string
  default     = "STANDARD"
}

variable "public_access_prevention" {
  description = "Enable public access prevention (e.g., 'enforced or 'unspecified')"
  type        = string
  default     = "enforced"
}

variable "uniform_bucket_level_access" {
  description = "Enable uniform bucket-level access (true/false)"
  type        = bool
  default     = true
}

variable "sample_file_name" {
  description = "Name of the file in the bucket to upload to GCS"
  type        = string
}

variable "snowflake_org" {
  description = "Name of the snowflake organisation"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name"
  type        = string
}

variable "snowflake_role" {
  description = "Snowflake Role"
  type        = string
}

variable "snowflake_wh" {
  description = "Snowflake warehouse"
  type        = string
}

variable "snowflake_database" {
  description = "Snowflake Database"
  type        = string
}

variable "snowflake_schema" {
  description = "Snowflake Schema"
  type        = string
}

variable "taxi_trip_raw_table" {
  description = "Taxi Trip Raw Table"
  type        = string
}

variable "snowflake_storage_integration" {
  description = "Name of the existing Snowflake Storage Integration to use"
  type        = string
}

variable "taxi_trip_staging_table" {
  description = "Taxi Trip Staging Table"
  type        = string
}

variable "snowflake_private_content" {
  description = "Private key for Snowflake authentication (contents of the PEM file)"
  type        = string
  sensitive   = true
}