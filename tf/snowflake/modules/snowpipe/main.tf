
provider "snowflake" {
    alias = "account_admin"
    role  = "ACCOUNTADMIN"
}

resource "snowflake_storage_integration" "gcs_storage_int" {
  name    = "GCS_STORAGE_INT"
  comment = "A storage integration for Google Cloud Storage."
  type    = "EXTERNAL_STAGE"
  enabled = true
  storage_allowed_locations = ["gcs://${var.project_id_hyphens}/street", "gcs://${var.project_id_hyphens}/outcomes", "gcs://${var.project_id_hyphens}/stop-and-search"]
  storage_provider = "GCS"
  storage_gcp_service_account = "k4g200000@gcpuscentral1-1dfa.iam.gserviceaccount.com"
}


resource "snowflake_integration_grant" "grant" {
  integration_name = gcs_storage_int
  privilege = "USAGE"
  roles     = ["ACCOUNTADMIN"]
  with_grant_option = false
}


