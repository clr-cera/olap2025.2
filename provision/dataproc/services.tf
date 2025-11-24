locals {
  required_services = [
    "dataproc.googleapis.com",
    "workflows.googleapis.com",
    "workflowexecutions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "storage.googleapis.com",
    "compute.googleapis.com"
  ]
}

resource "google_project_service" "required" {
  for_each           = toset(local.required_services)
  project            = var.gcp_project_id
  service            = each.key
  disable_on_destroy = false
}
