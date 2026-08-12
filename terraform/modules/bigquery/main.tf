resource "google_bigquery_dataset" "this" {
  for_each = var.datasets

  project       = var.project_id
  dataset_id    = each.value
  friendly_name = each.value
  description   = "ECRMAP ${each.key} layer dataset"
  location      = var.region

  delete_contents_on_destroy = var.environment != "prod"
}
