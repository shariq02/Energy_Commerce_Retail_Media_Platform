variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for the buckets"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev/prod)"
  type        = string
}

variable "raw_bucket_name" {
  description = "Name of the raw data bucket"
  type        = string
}

variable "staged_bucket_name" {
  description = "Name of the staged data bucket"
  type        = string
}

variable "raw_retention_days" {
  description = "Days to retain objects in the raw bucket before deletion"
  type        = number
  default     = 90
}

variable "staged_retention_days" {
  description = "Days to retain objects in the staged bucket before deletion"
  type        = number
  default     = 30
}
