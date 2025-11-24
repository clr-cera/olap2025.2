data "cloudflare_zone" "main" {
  name = var.domain_name
}

resource "cloudflare_record" "db" {
  zone_id = data.cloudflare_zone.main.id
  name    = "db.usp-olap"
  content = data.google_sql_database_instance.main.public_ip_address
  type    = "A"
  proxied = false
}
