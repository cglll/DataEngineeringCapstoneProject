/*module "service_accounts" {
  source        = "terraform-google-modules/service-accounts/google"
  version       = "~> 3.0"
  project_id    = var.project_id
  prefix        = "test-sa"
  names         = [var.name]
  project_roles = ["roles/cloudfunctions.developer",
                   "roles/bigquery.dataEditor",
                   "roles/dataflow.admin",
                   "roles/dataproc.admin",
                   "roles/storage.admin"
                  "project-foo=>roles/viewer",
              "project-spam=>roles/storage.objectViewer",
  ]
}*/
resource "google_service_account" "airflow_user" {
  account_id   = var.name
  display_name = var.name
  project = var.project_id
}

resource "google_service_account_iam_binding" "airflow_user" {
  service_account_id="${google_service_account.airflow_user.name}"
 
  role    = "roles/owner"
  members = [
    "user:${var.user}"
  ]
}
