# OLAP - Provisioning

Terraform scripts to provision the required infrastructure for this project using GCP, Cloudflare and Google Cloud Dataproc Serverless.

## Dataproc Serverless workflow

The Terraform in this folder now delivers everything that is necessary to execute the PySpark ETL in Dataproc Serverless and keep it on a daily cadence:

1. **Artifact bucket** – stores the `main.py` driver, the packaged `etl/` module and the runtime configuration JSON consumed by Spark.
2. **Service accounts & IAM** – isolates runtime permissions between the Dataproc cluster, the Workflow that submits jobs, and the Cloud Scheduler trigger.
3. **Cloud Workflow** – submits a Dataproc Serverless batch, waits for it to finish, and propagates failures.
4. **Cloud Scheduler** – invokes the workflow every day (defaults to 03:00 America/Sao_Paulo, configurable via variables).

### One-time setup

```fish
cd provision
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

> **Tip:** the Dataproc assets are optimised for Serverless runtime `2.2`. Use `-var "dataproc_runtime_version=<version>"` during plan/apply if you need a different runtime.

### Ship the ETL package

The apply step uploads `main.py`, a freshly zipped copy of the `etl/` module, and a GCS-friendly configuration JSON automatically via the `archive_file` data source. There is no manual packaging step required for Dataproc (though `make package` is still handy when running locally).

Terraform outputs the name of the artifact bucket. Use it to push the raw datasets that live under `data/` locally:

```fish
set BUCKET (terraform output -raw etl_bucket_name)
gsutil -m cp data/municipios.csv gs://$BUCKET/raw/
gsutil -m cp data/uf.csv gs://$BUCKET/raw/
gsutil -m cp data/queimadas-full.pqt.zstd gs://$BUCKET/raw/
gsutil -m cp data/sisam-full.pqt.zstd gs://$BUCKET/raw/
```

### Manual execution & monitoring

```fish
set WORKFLOW (terraform output -raw dataproc_workflow_name)
gcloud workflows run --location $GCP_REGION $WORKFLOW
```

Use the Cloud Console (Workflows > Executions) or `gcloud workflows executions describe` to monitor runs. The Cloud Scheduler job `wildfire-etl-daily` keeps the workflow on a daily schedule once raw files are present.

### Customising behaviour

Key knobs live in `vars.tf` and can be overridden during `terraform plan`/`apply`:

- `db_*` – connection string and credentials for the Cloud SQL Postgres target.
- `etl_raw_prefix` / `etl_curated_prefix` – top-level folders inside the artifact bucket for inputs and parquet outputs.
- `dataproc_batch_prefix` – prefix used when generating Dataproc batch IDs.
- `scheduler_cron_expression` / `scheduler_timezone` – scheduling window for Cloud Scheduler.

All other values (project, region, zone, Cloud SQL instance) continue to be provided through `terraform.tfvars` as before.

## TL;DR

```shell
cd provision
terraform init
terraform plan -out=tfplan
terraform apply tfplan

set BUCKET (terraform output -raw etl_bucket_name)
gsutil -m cp data/municipios.csv gs://$BUCKET/raw/
gsutil -m cp data/uf.csv gs://$BUCKET/raw/
gsutil -m cp data/queimadas-full.pqt.zstd gs://$BUCKET/raw/
gsutil -m cp data/sisam-full.pqt.zstd gs://$BUCKET/raw/

set WORKFLOW (terraform output -raw dataproc_workflow_name)
gcloud workflows run --location $GCP_REGION $WORKFLOW
```
