data "google_service_account" "ecrmap_admin" {
  project    = var.project_id
  account_id = var.service_account_id
}

resource "google_storage_bucket_iam_member" "raw_object_admin" {
  bucket = var.raw_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${data.google_service_account.ecrmap_admin.email}"
}

resource "google_storage_bucket_iam_member" "staged_object_admin" {
  bucket = var.staged_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${data.google_service_account.ecrmap_admin.email}"
}

resource "google_bigquery_dataset_iam_member" "dataset_editor" {
  for_each = var.bigquery_dataset_ids

  project    = var.project_id
  dataset_id = each.value
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${data.google_service_account.ecrmap_admin.email}"
}

# roles/bigquery.jobUser is intentionally NOT managed here. Terraform runs as
# ecrmap-admin itself, and a project-level google_project_iam_member requires
# setIamPolicy on the project — a permission the Editor role does not grant,
# even for the identity's own bindings. Granting it here would make Terraform
# try to modify the project IAM policy with an identity that isn't allowed to,
# which fails on apply regardless of the binding's content. This role was
# granted manually via the GCP Console instead and is deliberately left
# outside Terraform's management.
