data "cloudflare_zone" "main" {
  name = var.domain_name
}

resource "cloudflare_record" "db" {
  zone_id = data.cloudflare_zone.main.id
  name    = "db.usp-olap"
  content = data.google_alloydb_instance.primary.public_ip_address
  type    = "A"
  proxied = false
}

resource "cloudflare_record" "superset" {
  zone_id = data.cloudflare_zone.main.id
  name    = "superset.usp-olap"
  content = google_compute_instance.superset.network_interface[0].access_config[0].nat_ip
  type    = "A"
  proxied = false
}

resource "cloudflare_record" "postgres" {
  zone_id = data.cloudflare_zone.main.id
  name    = "db-hyper.usp-olap"
  content = google_compute_instance.postgres.network_interface[0].access_config[0].nat_ip
  type    = "A"
  proxied = false
}