terraform {
  backend "gcs" {
    # Don't use variables here
    # All values are passed via -backend-config
  }
}
