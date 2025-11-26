data "google_compute_network" "default" {
  name = "default"
}

data "google_compute_subnetwork" "default" {
  name   = "default"
  region = var.gcp_region
}

resource "google_compute_firewall" "superset_http_https" {
  name    = "superset-allow-http-https"
  network = data.google_compute_network.default.self_link

  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }

  allow {
    protocol = "udp"
    ports    = ["443"]
  }

  source_ranges = ["0.0.0.0/0"]

  target_tags = ["superset"]
}

resource "google_compute_firewall" "postgres" {
  name    = "postgres-allow"
  network = data.google_compute_network.default.self_link

  allow {
    protocol = "tcp"
    ports    = ["5432"]
  }

  source_ranges = ["0.0.0.0/0"]

  target_tags = ["postgres"]
}
