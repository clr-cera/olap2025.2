resource "random_id" "etl_bucket" {
  byte_length = 4
}

locals {
  etl_bucket_name          = "${var.gcp_project_id}-wildfire-${random_id.etl_bucket.hex}"
  dataproc_main_python_uri = "gs://${local.etl_bucket_name}/jobs/main.py"
  dataproc_py_file_uris    = ["gs://${local.etl_bucket_name}/jobs/etl.zip"]
  dataproc_jar_file_uris   = ["gs://${local.etl_bucket_name}/libs/postgresql-42.7.3.jar"]
  dataproc_config_uri      = "gs://${local.etl_bucket_name}/configs/dataproc_config.json"
  dataproc_args            = ["--config", local.dataproc_config_uri]
  columnar_migration_enabled = true


  # postgres_host = "10.45.128.3" # Cloud SQL internal IP
  # postgres_host = "10.158.15.204" # Alloy DB internal IP
  postgres_host = "10.158.15.228" # Hyper DB internal IP

  dataproc_config_content = templatefile("${path.module}/templates/dataproc_config.json.tmpl", {
    bucket_name                = local.etl_bucket_name,
    raw_prefix                 = var.etl_raw_prefix,
    curated_prefix             = var.etl_curated_prefix,
    postgres_host              = local.postgres_host,
    postgres_port              = var.db_port,
    postgres_db                = var.db_name,
    postgres_user              = var.db_user,
    postgres_pass              = var.db_password,
    columnar_migration_enabled = tostring(local.columnar_migration_enabled)
  })

  dataproc_config_gcs_content = templatefile("${path.module}/templates/dataproc_config_gcs.json.tmpl", {
    bucket_name                = local.etl_bucket_name,
    raw_prefix                 = var.etl_raw_prefix,
    curated_prefix             = var.etl_curated_prefix,
    columnar_migration_enabled = tostring(local.columnar_migration_enabled)
  })

  dataproc_config_bq_gcs_content = templatefile("${path.module}/templates/dataproc_config_bq_gcs.json.tmpl", {
    bucket_name                = local.etl_bucket_name,
    raw_prefix                 = var.etl_raw_prefix,
    curated_prefix             = var.etl_curated_prefix,
    project_id                 = var.gcp_project_id,
    columnar_migration_enabled = tostring(local.columnar_migration_enabled)
  })

  dataproc_config_bq_postgres_content = templatefile("${path.module}/templates/dataproc_config_bq_postgres.json.tmpl", {
    bucket_name                = local.etl_bucket_name,
    raw_prefix                 = var.etl_raw_prefix,
    project_id                 = var.gcp_project_id,
    postgres_host              = local.postgres_host,
    postgres_port              = var.db_port,
    postgres_db                = var.db_name,
    postgres_user              = var.db_user,
    postgres_pass              = var.db_password,
    columnar_migration_enabled = tostring(local.columnar_migration_enabled)
  })
}

resource "google_storage_bucket" "etl_artifacts" {
  name          = local.etl_bucket_name
  project       = var.gcp_project_id
  location      = var.gcp_region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    application = "wildfire-etl"
    environment = "prod"
  }
}

resource "google_storage_bucket_object" "etl_main" {
  bucket       = google_storage_bucket.etl_artifacts.name
  name         = "jobs/main.py"
  source       = "${path.module}/../../main.py"
  content_type = "text/x-python"
}

resource "null_resource" "etl_package" {
  triggers = {
    etl_files_md5 = md5(join("", [for f in fileset("${path.module}/../../etl", "**/*.py") : filemd5("${path.module}/../../etl/${f}")]))
  }

  depends_on = [google_storage_bucket.etl_artifacts]

  provisioner "local-exec" {
    command = <<EOT
      mkdir -p ${abspath(path.module)}/tmp
      cd ${abspath(path.module)}/../../
      zip -r ${abspath(path.module)}/tmp/etl.zip etl -x '**/__pycache__/*' -x '**/*.pyc'
      gcloud storage cp ${abspath(path.module)}/tmp/etl.zip gs://${google_storage_bucket.etl_artifacts.name}/jobs/etl.zip
    EOT
  }
}

# Removed google_storage_bucket_object.etl_zip to avoid two-apply issue


resource "google_storage_bucket_object" "etl_config" {
  bucket       = google_storage_bucket.etl_artifacts.name
  name         = "configs/dataproc_config.json"
  content      = local.dataproc_config_content
  content_type = "application/json"
}

resource "google_storage_bucket_object" "etl_config_gcs" {
  bucket       = google_storage_bucket.etl_artifacts.name
  name         = "configs/dataproc_config_gcs.json"
  content      = local.dataproc_config_gcs_content
  content_type = "application/json"
}

resource "google_storage_bucket_object" "etl_config_bq_gcs" {
  bucket       = google_storage_bucket.etl_artifacts.name
  name         = "configs/dataproc_config_bq_gcs.json"
  content      = local.dataproc_config_bq_gcs_content
  content_type = "application/json"
}

resource "google_storage_bucket_object" "etl_config_bq_postgres" {
  bucket       = google_storage_bucket.etl_artifacts.name
  name         = "configs/dataproc_config_bq_postgres.json"
  content      = local.dataproc_config_bq_postgres_content
  content_type = "application/json"
}

resource "google_service_account" "dataproc_runner" {
  account_id   = "dataproc-etl-runner"
  display_name = "Dataproc Serverless runtime for Wildfire ETL"
}

resource "google_service_account" "workflow_orchestrator" {
  account_id   = "dataproc-etl-workflow"
  display_name = "Workflow orchestrator for Wildfire ETL"
}

resource "google_service_account" "scheduler_invoker" {
  account_id   = "dataproc-etl-scheduler"
  display_name = "Scheduler invoker for Wildfire ETL"
}

resource "google_project_iam_member" "dataproc_runner_worker" {
  project = var.gcp_project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

resource "google_project_iam_member" "dataproc_runner_cloudsql" {
  project = var.gcp_project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

resource "google_project_iam_member" "dataproc_runner_logging" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

resource "google_storage_bucket_iam_member" "dataproc_runner_bucket_admin" {
  bucket = google_storage_bucket.etl_artifacts.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

resource "google_project_iam_member" "workflow_dataproc_editor" {
  project = var.gcp_project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.workflow_orchestrator.email}"
}

resource "google_service_account_iam_member" "workflow_impersonates_runner" {
  service_account_id = google_service_account.dataproc_runner.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.workflow_orchestrator.email}"
}

resource "google_project_iam_member" "dataproc_runner_bigquery" {
  project = var.gcp_project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

resource "null_resource" "download_postgres_driver" {
  triggers = {
    driver_md5 = "c3d7f5513c17426a7139def45a52672c" # md5sum of postgresql-42.7.3.jar
  }

  depends_on = [google_storage_bucket.etl_artifacts]

  provisioner "local-exec" {
    command = <<EOT
      mkdir -p ${abspath(path.module)}/tmp
      FILE=${abspath(path.module)}/tmp/postgresql-42.7.3.jar
      if [ ! -f "$FILE" ] || [ "$(md5 -q $FILE)" != "${self.triggers.driver_md5}" ]; then
          curl -o $FILE https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
      fi
      gcloud storage cp $FILE gs://${google_storage_bucket.etl_artifacts.name}/libs/postgresql-42.7.3.jar
    EOT
  }
}

# Removed google_storage_bucket_object.postgres_driver to avoid two-apply issue

