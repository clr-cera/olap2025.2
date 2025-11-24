output "etl_bucket_name" {
  description = "Central bucket that stores ETL code, configs and parquet outputs"
  value       = google_storage_bucket.etl_artifacts.name
}

output "dataproc_workflow_name" {
  description = "Workflows resource that submits Dataproc batches"
  value       = google_workflows_workflow.etl.name
}

output "cloud_scheduler_job" {
  description = "Identifier of the Cloud Scheduler job that drives executions"
  value       = google_cloud_scheduler_job.etl.name
}

output "dataproc_config_uri" {
  description = "GCS URI of the config JSON consumed by the PySpark job"
  value       = local.dataproc_config_uri
}
