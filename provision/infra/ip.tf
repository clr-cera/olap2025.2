resource "google_compute_address" "superset_ip" {
  project = var.gcp_project_id
  name    = "superset-static-ip"
  region  = var.gcp_region
}

resource "google_compute_address" "postgres_ip" {
  project = var.gcp_project_id
  name    = "postgres-static-ip"
  region  = var.gcp_region
}
