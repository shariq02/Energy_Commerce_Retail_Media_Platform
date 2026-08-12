variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "energy-commerce-retail-media"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west3"
}

variable "environment" {
  description = "Deployment environment (dev/prod)"
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "GCS bucket name for raw data"
  type        = string
  default     = "ecrmap-dev-raw"
}

variable "staged_bucket_name" {
  description = "GCS bucket name for staged data"
  type        = string
  default     = "ecrmap-dev-staged"
}

variable "bigquery_datasets" {
  description = "Map of logical dataset names to BigQuery dataset IDs"
  type        = map(string)
  default = {
    raw      = "ecrmap_raw"
    staging  = "ecrmap_staging"
    core     = "ecrmap_core"
    marts    = "ecrmap_marts"
    semantic = "ecrmap_semantic"
  }
}

variable "service_account_id" {
  description = "Account ID (local part) of the ECRMAP service account"
  type        = string
  default     = "ecrmap-admin"
}
