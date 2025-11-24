variable "cloudflare_account_id" {
  description = "Cloudflare Account ID"
  type        = string
}

variable "cloudflare_api_token" {
  description = "Cloudflare API Token"
  type        = string
}

variable "gcp_project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "gcp_region" {
  description = "Google Cloud Region"
  type        = string
  default     = "southamerica-east1"
}

variable "gcp_zone" {
  description = "Google Cloud Zone"
  type        = string
  default     = "southamerica-east1-a"
}

variable "domain_name" {
  description = "Domain name"
  type        = string
  default     = "capivara.cafe"
}

variable "db_name" {
  description = "Target PostgreSQL database name"
  type        = string
  default     = "wildfire"
}

variable "db_user" {
  description = "Target PostgreSQL username"
  type        = string
  default     = "wildfire"
}

variable "db_password" {
  description = "Target PostgreSQL password"
  type        = string
  default     = "wildfire"
}

variable "db_port" {
  description = "Target PostgreSQL port"
  type        = number
  default     = 5432
}

variable "etl_raw_prefix" {
  description = "Top-level folder inside the ETL bucket for raw inputs"
  type        = string
  default     = "raw"
}

variable "etl_curated_prefix" {
  description = "Top-level folder inside the ETL bucket for processed outputs"
  type        = string
  default     = "curated"
}

variable "dataproc_batch_prefix" {
  description = "Prefix that will be used when generating Dataproc batch identifiers"
  type        = string
  default     = "wildfire-etl"
}

variable "dataproc_runtime_version" {
  description = "Dataproc Serverless runtime version for PySpark"
  type        = string
  default     = "3.0"
}

variable "scheduler_cron_expression" {
  description = "CRON expression that drives the Cloud Scheduler trigger"
  type        = string
  default     = "0 3 * * *"
}

variable "scheduler_timezone" {
  description = "IANA timezone for the Cloud Scheduler job"
  type        = string
  default     = "America/Sao_Paulo"
}
