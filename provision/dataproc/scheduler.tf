resource "google_project_iam_member" "scheduler_invoker" {
  project = var.gcp_project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.scheduler_invoker.email}"
}

resource "google_cloud_scheduler_job" "etl" {
  name        = "wildfire-etl-daily"
  description = "Triggers the Wildfire ETL workflow"
  project     = var.gcp_project_id
  region      = var.gcp_region
  schedule    = var.scheduler_cron_expression
  time_zone   = var.scheduler_timezone

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.etl_bq_postgres.id}/executions"
    body        = base64encode("{}")

    oauth_token {
      service_account_email = google_service_account.scheduler_invoker.email
    }
  }
}
