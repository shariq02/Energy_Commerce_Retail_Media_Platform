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

variable "bigquery_datasets" {
  description = <<-EOT
    Map of logical dataset names to BigQuery dataset IDs.

    BigQuery is the Google-source acquisition interface only: query output for
    Google-native sources (GA4 first) lands in one acquisition dataset, is
    exported to local storage, and loaded into Databricks Bronze. No
    analytical / warehouse / semantic datasets live in BigQuery.
  EOT
  type        = map(string)
  default = {
    acquisition = "ecrmap_acquisition"
  }
}

variable "service_account_id" {
  description = "Account ID (local part) of the ECRMAP service account"
  type        = string
  default     = "ecrmap-admin"
}
