data "cloudflare_zone" "main" {
  name = var.domain_name
}