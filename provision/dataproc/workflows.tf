locals {
  workflow_source = templatefile("${path.module}/templates/workflow_dataproc.yaml.tmpl", {
    project_id            = var.gcp_project_id,
    region                = var.gcp_region,
    batch_prefix          = var.dataproc_batch_prefix,
    main_python_uri       = local.dataproc_main_python_uri,
    py_file_uris          = local.dataproc_py_file_uris,
    jar_file_uris         = local.dataproc_jar_file_uris,
    args                  = local.dataproc_args,
    runtime_version       = var.dataproc_runtime_version,
    dataproc_service_acct = google_service_account.dataproc_runner.email,
    subnetwork_uri        = data.google_compute_subnetwork.default.self_link,
    staging_bucket        = google_storage_bucket.etl_artifacts.name,
    poll_interval_seconds = 60
  })

  workflow_source_gcs = templatefile("${path.module}/templates/workflow_dataproc.yaml.tmpl", {
    project_id            = var.gcp_project_id,
    region                = var.gcp_region,
    batch_prefix          = "wildfire-etl-gcs",
    main_python_uri       = local.dataproc_main_python_uri,
    py_file_uris          = local.dataproc_py_file_uris,
    jar_file_uris         = local.dataproc_jar_file_uris,
    args                  = ["--config", "gs://${local.etl_bucket_name}/configs/dataproc_config_gcs.json"],
    runtime_version       = var.dataproc_runtime_version,
    dataproc_service_acct = google_service_account.dataproc_runner.email,
    subnetwork_uri        = data.google_compute_subnetwork.default.self_link,
    staging_bucket        = google_storage_bucket.etl_artifacts.name,
    poll_interval_seconds = 60
  })

  workflow_source_bq_gcs = templatefile("${path.module}/templates/workflow_dataproc.yaml.tmpl", {
    project_id            = var.gcp_project_id,
    region                = var.gcp_region,
    batch_prefix          = "wildfire-etl-bq-gcs",
    main_python_uri       = local.dataproc_main_python_uri,
    py_file_uris          = local.dataproc_py_file_uris,
    jar_file_uris         = local.dataproc_jar_file_uris,
    args                  = ["--config", "gs://${local.etl_bucket_name}/configs/dataproc_config_bq_gcs.json"],
    runtime_version       = var.dataproc_runtime_version,
    dataproc_service_acct = google_service_account.dataproc_runner.email,
    subnetwork_uri        = data.google_compute_subnetwork.default.self_link,
    staging_bucket        = google_storage_bucket.etl_artifacts.name,
    poll_interval_seconds = 60
  })

  workflow_source_bq_postgres = templatefile("${path.module}/templates/workflow_dataproc.yaml.tmpl", {
    project_id            = var.gcp_project_id,
    region                = var.gcp_region,
    batch_prefix          = "wildfire-etl-bq-postgres",
    main_python_uri       = local.dataproc_main_python_uri,
    py_file_uris          = local.dataproc_py_file_uris,
    jar_file_uris         = local.dataproc_jar_file_uris,
    args                  = ["--config", "gs://${local.etl_bucket_name}/configs/dataproc_config_bq_postgres.json"],
    runtime_version       = var.dataproc_runtime_version,
    dataproc_service_acct = google_service_account.dataproc_runner.email,
    subnetwork_uri        = data.google_compute_subnetwork.default.self_link,
    staging_bucket        = google_storage_bucket.etl_artifacts.name,
    poll_interval_seconds = 60
  })
}

resource "google_workflows_workflow" "etl" {
  name                = "wildfire-etl-gcs-postgres"
  region              = var.gcp_region
  description         = "Submits the Wildfire PySpark job to Dataproc Serverless (GCS -> Postgres)"
  service_account     = google_service_account.workflow_orchestrator.email
  source_contents     = local.workflow_source
  deletion_protection = false
}

resource "google_workflows_workflow" "etl_gcs" {
  name                = "wildfire-etl-gcs"
  region              = var.gcp_region
  description         = "Submits the Wildfire PySpark job to Dataproc Serverless (GCS -> GCS)"
  service_account     = google_service_account.workflow_orchestrator.email
  source_contents     = local.workflow_source_gcs
  deletion_protection = false
}

resource "google_workflows_workflow" "etl_bq_gcs" {
  name                = "wildfire-etl-bq-gcs"
  region              = var.gcp_region
  description         = "Submits the Wildfire PySpark job to Dataproc Serverless (BQ -> GCS)"
  service_account     = google_service_account.workflow_orchestrator.email
  source_contents     = local.workflow_source_bq_gcs
  deletion_protection = false
}

resource "google_workflows_workflow" "etl_bq_postgres" {
  name                = "wildfire-etl-bq-postgres"
  region              = var.gcp_region
  description         = "Submits the Wildfire PySpark job to Dataproc Serverless (BQ -> Postgres)"
  service_account     = google_service_account.workflow_orchestrator.email
  source_contents     = local.workflow_source_bq_postgres
  deletion_protection = false
}
