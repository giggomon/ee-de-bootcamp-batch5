# Specify Terraform version
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.15.0"
    }
    snowflake = {
      source = "Snowflake-Labs/snowflake"
      version = "~> 0.61"
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
  account_name = var.snowflake_account_name
  user = var.snowflake_user
  password = var.snowflake_pwd
  role = var.snowflake_role
  warehouse = var.snowflake_wh

}

# Resource: Google Cloud Storage bucket
resource "google_storage_bucket" "landing_bucket" {
  name          = var.bucket_name
  location      = var.bucket_location
  storage_class = var.storage_class

  uniform_bucket_level_access = var.uniform_bucket_level_access # Bucket policy: Make it private by default
  public_access_prevention    = var.public_access_prevention    # No public access

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [location]
  }
}

resource "google_storage_bucket_object" "upload_file" {
  bucket       = google_storage_bucket.landing_bucket.name
  name         = var.sample_file_name
  source       = "../resources/sample_file.txt"
  content_type = "text/plain"

  depends_on = [google_storage_bucket.landing_bucket]
}

resource "snowflake_schema" "schema" {
  name = var.snowflake_schema               # Name of the schema
  database = var.snowflake_database
  comment = "Create Schema for MONICA"
}

resource "snowflake_table" "taxi_trips_raw" {
  database = var.snowflake_database
  schema = var.snowflake_schema
  name = var.taxi_trip_raw_table

  column {
    name = "vendor_name"
    type = "STRING"
  }

   column {
    name = "tpep_pickup_datetime"
    type = "STRING"
  }

   column {
    name = "tpep_dropoff_datetime"
    type = "STRING"
  }

   column {
    name = "passenger_count"
    type = "STRING"
  }

  column {
    name = "trip_distance"
    type = "STRING"
  }

  column {
    name = "pickup_longitude"
    type = "STRING"
  }

  column {
    name = "pickup_latitude"
    type = "STRING"
  }

  column {
    name = "RatecodeID"
    type = "STRING"
  }

  column {
    name = "store_and_fwd_flag"
    type = "STRING"
  }

  column {
    name = "dropoff_longitude"
    type = "STRING"
  }

  column {
    name = "dropoff_latitude"
    type = "STRING"
  }

  column {
    name = "payment_type"
    type = "STRING"
  }

  column {
    name = "payment_type_name"
    type = "STRING"
  }

  column {
    name = "fare_amount"
    type = "STRING"
  }

  column {
    name = "extra"
    type = "STRING"
  }

  column {
    name = "mta_tax"
    type = "STRING"
  }

  column {
    name = "tip_amount"
    type = "STRING"
  }

  column {
    name = "tolls_amount"
    type = "STRING"
  }

  column {
    name = "improvement_surcharge"
    type = "STRING"
  }

  column {
    name = "total_amount"
    type = "STRING"
  }

  column {
    name = "trip_duration_minutes"
    type = "STRING"
  }

  column {
    name = "trip_speed_mph"
    type = "STRING"
  }

  column {
    name = "created_timestamp"
    type = "TIMESTAMP_NTZ"
    nullable = false

    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}