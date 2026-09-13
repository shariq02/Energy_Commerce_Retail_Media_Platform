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
  bigquery_dataset_ids = module.bigquery.dataset_ids
}
