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
  description = "Application PostgreSQL username"
  type        = string
  default     = "wildfire"
}

variable "db_password" {
  description = "Application PostgreSQL user's password"
  type        = string
  default     = "wildfire"
}