module "gcs" {
  source = "./modules/gcs"

  project_id         = var.project_id
  region             = var.region
  environment        = var.environment
  raw_bucket_name    = var.raw_bucket_name
  staged_bucket_name = var.staged_bucket_name
}

module "bigquery" {
  source = "./modules/bigquery"

  project_id = var.project_id
  region     = var.region
  datasets   = var.bigquery_datasets
}

module "iam" {
  source = "./modules/iam"

  project_id           = var.project_id
  service_account_id   = var.service_account_id
  raw_bucket_name      = module.gcs.raw_bucket_name
  staged_bucket_name   = module.gcs.staged_bucket_name
  bigquery_dataset_ids = module.bigquery.dataset_ids
}
