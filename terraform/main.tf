# Specify Terraform version
terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.15.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87" # Try a different recent version
    }
  }
}


# Provider configuration
provider "google" {
  credentials = var.google_credentials != "" ? var.google_credentials : null
  project     = var.project_id
  region      = var.region
}


provider "snowflake" {
  organization_name = var.snowflake_org
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_pwd
  role              = var.snowflake_role
  warehouse         = var.snowflake_wh
}

# Resource: Google Cloud Storage bucket
# Storage bucket for landing zone of raw data files
# Used for initial data ingestion before processing
resource "google_storage_bucket" "landing_bucket" {
  name          = var.bucket_name
  location      = var.bucket_location
  storage_class = var.storage_class
  force_destroy = false # protect against accidental deletion
  # Security settings
  uniform_bucket_level_access = var.uniform_bucket_level_access # Bucket policy: Make it private by default
  public_access_prevention    = var.public_access_prevention    # No public access

  # Enable versioning
  versioning {
    enabled = true
  }

  # Protection against accidental deletion
  lifecycle {
    prevent_destroy = true
    ignore_changes = [
      location,
      labels["created_date"]
    ]
  }
}

resource "google_storage_bucket_object" "upload_file" {
  bucket       = google_storage_bucket.landing_bucket.name
  name         = var.sample_file_name
  source       = "${path.module}/../resources/sample_file.txt"
  content_type = "text/plain"

  depends_on = [google_storage_bucket.landing_bucket]
}

resource "snowflake_schema" "schema" {
  name     = var.snowflake_schema # Name of the schema
  database = var.snowflake_database
  comment  = "Create Schema for MONICA"
}

resource "snowflake_table" "taxi_trips_raw" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = var.taxi_trip_raw_table

  column {
    name     = "vendorid"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "tpep_pickup_datetime"
    type     = "TIMESTAMP_NTZ"
    nullable = false
  }

  column {
    name     = "tpep_dropoff_datetime"
    type     = "TIMESTAMP_NTZ"
    nullable = false
  }

  column {
    name     = "passenger_count"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "trip_distance"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "pickup_longitude"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "pickup_latitude"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "ratecodeID"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "store_and_fwd_flag"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "dropoff_longitude"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "dropoff_latitude"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "payment_type"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "fare_amount"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "extra"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "mta_tax"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "tip_amount"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "tolls_amount"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "improvement_surcharge"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "total_amount"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "trip_duration_minutes"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "trip_speed_mph"
    type     = "STRING"
    nullable = true
  }

  column {
    name = "created_timestamp"
    type = "TIMESTAMP_NTZ"
    #nullable = true

    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

  comment    = "Raw taxi trip data loaded from GCS"
  depends_on = [snowflake_schema.schema]
}

resource "snowflake_file_format" "csv_format" {
  name     = "CSV_FORMAT"
  database = var.snowflake_database
  schema   = var.snowflake_schema

  format_type         = "CSV"
  field_delimiter     = ","
  null_if             = ["\\N", "NULL", "null", ""]
  empty_field_as_null = true
  trim_space          = true
  parse_header        = true
}

resource "snowflake_stage" "gcs_taxi_stage" {
  name                = "GCS_TAXI_STAGE"
  database            = var.snowflake_database
  schema              = var.snowflake_schema
  url                 = "gcs://${var.bucket_name}/"
  storage_integration = var.snowflake_storage_integration

  comment    = "External stage to load taxi trip data from GCS bucket using pre-created integration"
  depends_on = [snowflake_schema.schema]
}

resource "snowflake_table" "taxi_trips_staging" {
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = var.taxi_trip_staging_table

  column {
    name     = "vendorid"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "tpep_pickup_datetime"
    type     = "TIMESTAMP_NTZ"
    nullable = false
  }

  column {
    name     = "tpep_dropoff_datetime"
    type     = "TIMESTAMP_NTZ"
    nullable = false
  }

  column {
    name     = "passenger_count"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "trip_distance"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "pickup_longitude"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "pickup_latitude"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "ratecodeid"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "store_and_fwd_flag"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "dropoff_longitude"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "dropoff_latitude"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "payment_type"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "fare_amount"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "extra"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "mta_tax"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "tip_amount"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "tolls_amount"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "improvement_surcharge"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "total_amount"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "trip_duration_minutes"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "trip_speed_mph"
    type     = "STRING"
    nullable = true
  }

  column {
    name = "created_timestamp"
    type = "TIMESTAMP_NTZ"
    #nullable = true

    default {
      expression = "CURRENT_TIMESTAMP()"
    }
}

  comment    = "Staging table for taxi trips"
  depends_on = [snowflake_schema.schema]
}