output "bigquery_dataset_ids" {
  description = "Map of logical dataset names to BigQuery dataset IDs"
  value       = module.bigquery.dataset_ids
}

output "service_account_email" {
  description = "Email of the ECRMAP service account"
  value       = module.iam.service_account_email
}
