variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "BigQuery dataset location"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev/prod)"
  type        = string
  default     = "dev"
}

variable "datasets" {
  description = "Map of logical dataset names to BigQuery dataset IDs"
  type        = map(string)
}
