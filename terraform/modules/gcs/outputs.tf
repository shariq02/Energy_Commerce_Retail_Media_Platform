output "raw_bucket_name" {
  description = "Name of the raw data bucket"
  value       = google_storage_bucket.raw.name
}

output "staged_bucket_name" {
  description = "Name of the staged data bucket"
  value       = google_storage_bucket.staged.name
}

output "raw_bucket_url" {
  description = "gs:// URL of the raw data bucket"
  value       = google_storage_bucket.raw.url
}

output "staged_bucket_url" {
  description = "gs:// URL of the staged data bucket"
  value       = google_storage_bucket.staged.url
}
