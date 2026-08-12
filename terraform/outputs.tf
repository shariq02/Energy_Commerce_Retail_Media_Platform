output "raw_bucket_name" {
  description = "Name of the GCS raw data bucket"
  value       = module.gcs.raw_bucket_name
}

output "staged_bucket_name" {
  description = "Name of the GCS staged data bucket"
  value       = module.gcs.staged_bucket_name
}

output "bigquery_dataset_ids" {
  description = "Map of logical dataset names to BigQuery dataset IDs"
  value       = module.bigquery.dataset_ids
}

output "service_account_email" {
  description = "Email of the ECRMAP service account"
  value       = module.iam.service_account_email
}
