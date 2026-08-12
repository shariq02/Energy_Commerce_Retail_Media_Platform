terraform {
  backend "gcs" {
    bucket = "ecrmap-terraform-state"
    prefix = "terraform/state"
  }
}
