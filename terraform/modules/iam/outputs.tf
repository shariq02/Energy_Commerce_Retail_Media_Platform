output "service_account_email" {
  description = "Email of the ECRMAP service account"
  value       = data.google_service_account.ecrmap_admin.email
}
