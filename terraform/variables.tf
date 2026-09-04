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
  description = <<-EOT
    Map of logical dataset names to BigQuery dataset IDs.

    raw/staging/core/marts/semantic are the first-wave (Energy ecosystem)
    datasets. They are RENAMED to energy_* only when the Energy serving
    build begins -- not now, zero migration cost today. Future ecosystems
    (mobility/healthcare/agriculture) get their own {ecosystem}_{layer} set,
    added here when that ecosystem's serving build begins -- not created
    speculatively.

    shared_conformed and cross_ecosystem_marts are the two platform-level
    namespaces: shared_conformed holds the one copy of
    dim_date/dim_time/dim_geography/dim_weather_context (+ the PLZ
    crosswalk); cross_ecosystem_marts holds marts that legitimately span
    ecosystems via conformed keys only, kept out of any single ecosystem's
    own _marts dataset.
  EOT
  type        = map(string)
  default = {
    raw      = "ecrmap_raw"
    staging  = "ecrmap_staging"
    core     = "ecrmap_core"
    marts    = "ecrmap_marts"
    semantic = "ecrmap_semantic"

    shared_conformed      = "shared_conformed"
    cross_ecosystem_marts = "cross_ecosystem_marts"
  }
}

variable "service_account_id" {
  description = "Account ID (local part) of the ECRMAP service account"
  type        = string
  default     = "ecrmap-admin"
}
