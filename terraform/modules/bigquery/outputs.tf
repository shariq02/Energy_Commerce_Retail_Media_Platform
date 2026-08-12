output "dataset_ids" {
  description = "Map of logical dataset names to their BigQuery dataset IDs"
  value       = { for key, ds in google_bigquery_dataset.this : key => ds.dataset_id }
}

output "dataset_self_links" {
  description = "Map of logical dataset names to their self links"
  value       = { for key, ds in google_bigquery_dataset.this : key => ds.self_link }
}
