resource "google_storage_bucket" "raw" {
  name                        = var.raw_bucket_name
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = var.environment != "prod"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.raw_retention_days
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "staged" {
  name                        = var.staged_bucket_name
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = var.environment != "prod"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.staged_retention_days
    }
    action {
      type = "Delete"
    }
  }
}
