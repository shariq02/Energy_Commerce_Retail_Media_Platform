variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "service_account_id" {
  description = "Account ID (local part) of the existing ECRMAP service account"
  type        = string
}

variable "bigquery_dataset_ids" {
  description = "Map of logical dataset names to BigQuery dataset IDs to grant access to"
  type        = map(string)
}
