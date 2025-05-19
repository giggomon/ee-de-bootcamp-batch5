# Specify Terraform version
terraform {
  required_version = ">= 1.0.0"

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

  # Lifecycle management
  lifecycle_rule {
    condition {
      age = 120  # days move to cold storage after 120 days
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
   # Auto-delete old versions after 1 year
  lifecycle_rule {
    condition {
      age = 365
      with_state = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  # Protection against accidental deletion
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [
      location,
      labels["created_date"]
    ]
  }
}

resource "google_storage_bucket_object" "upload_file" {
  bucket       = google_storage_bucket.landing_bucket.name
  name         = var.sample_file_name
  source       = "../resources/sample_file.txt"
  content_type = "text/plain"

  # Add metadata
  metadata = {
    created_by = "terraform"
    purpose    = "sample_data"
  }
  lifecycle {
    ignore_changes = [
      source,
      content_type,
      detect_md5hash
    ]
  }
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

  # Clustering key for better query performance
  cluster_by = ["tpep_pickup_datetime"]

  # Data retention days
  data_retention_time_in_days = 90

  column {
    name      = "vendor_name"
    type      = "VARCHAR(50)"
    nullable  = true
  }

   column {
    name      = "tpep_pickup_datetime"
    type      = "TIMESTAMP_NTZ"
    nullable  = false
  }

   column {
    name = "tpep_dropoff_datetime"
    type      = "TIMESTAMP_NTZ"
    nullable  = false
  }

   column {
    name      = "passenger_count"
    type      = "NUMBER(3, 0)"
    nullable  = true
}

  column {
    name        = "trip_distance"
    type      = "FLOAT"
    nullable = true
  }

  column {
    name = "pickup_longitude"
    type = "FLOAT"
    nullable = true
  }

  column {
    name = "pickup_latitude"
    type = "FLOAT"
    nullable = true
  }

  column {
    name = "RatecodeID"
    type = "NUMBER(1, 0)"
    nullable = true
  }

  column {
    name = "store_and_fwd_flag"
    type = "CHAR(1)"
    nullable = true
  }

  column {
    name = "dropoff_longitude"
    type = "FLOAT"
    nullable = true
  }

  column {
    name = "dropoff_latitude"
    type = "FLOAT"
    nullable = true
  }

  column {
    name = "payment_type"
    type = "NUMBER(1, 0)"
    nullable = true
  }

  column {
    name = "payment_type_name"
    type = "VARCHAR(20)"
    nullable = true
  }

  column {
    name = "fare_amount"
    type = "NUMBER(10, 2)"
    nullable = true
  }

  column {
    name = "extra"
    type = "NUMBER(10, 2)"
    nullable = true
  }

  column {
    name = "mta_tax"
    type = "NUMBER(10,2)"
    nullable = true
  }

  column {
    name = "tip_amount"
    type = "NUMBER(10,2)"
    nullable = true
}

  column {
    name = "tolls_amount"
    type = "NUMBER(10,2)"
    nullable = true
}

  column {
    name = "improvement_surcharge"
    type = "NUMBER(10,2)"
    nullable = true
}

  column {
    name = "total_amount"
    type = "NUMBER(10,2)"
    nullable = true
}

  column {
    name = "trip_duration_minutes"
    type = "NUMBER(10,2)"
    nullable = true
}

  column {
    name = "trip_speed_mph"
    type = "FLOAT"
    nullable = true
  }

  column {
    name = "created_timestamp"
    type = "TIMESTAMP_NTZ"
    nullable = false

    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

  comment = "Raw taxi trip data loaded from GCS"

  lifecycle{
    prevent_destroy = true
  }

  depends_on = [snowflake_schema.schema]
}