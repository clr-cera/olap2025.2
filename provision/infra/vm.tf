resource "google_compute_instance" "superset" {
  project      = var.gcp_project_id
  name         = "superset-vm"
  machine_type = "e2-standard-2"
  zone         = var.gcp_zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 128
      type  = "pd-ssd"
    }
  }

  network_interface {
    network    = data.google_compute_network.default.name
    subnetwork = data.google_compute_subnetwork.default.name
    access_config {
      nat_ip = google_compute_address.superset_ip.address
    }
  }

  tags = ["superset"]

  scheduling {
    automatic_restart           = false
    preemptible                 = true
    provisioning_model          = "SPOT"
    on_host_maintenance         = "TERMINATE"
    instance_termination_action = "STOP"
  }

  metadata = {
    ssh-keys       = "lelis:${local.ssh_pub_key}"
    enable-oslogin = "FALSE"
  }

  allow_stopping_for_update = true
}

resource "google_compute_instance" "postgres" {
  provider = google-beta
  project      = var.gcp_project_id
  name         = "postgres-vm"
  machine_type = "c3-standard-22"
  zone         = var.gcp_zone

  boot_disk {
    initialize_params {
      # Ubuntu 22.04 LTS (Jammy / Noble equivalent image family for GCE)
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 256
      type  = "hyperdisk-balanced"
      provisioned_iops       = 15000
      provisioned_throughput = 1000
    }
  }

  network_interface {
    network    = data.google_compute_network.default.name
    subnetwork = data.google_compute_subnetwork.default.name
    access_config {
      nat_ip = google_compute_address.postgres_ip.address
    }
  }

  tags = ["postgres"]

  scheduling {
    automatic_restart           = false
    preemptible                 = true
    provisioning_model          = "SPOT"
    on_host_maintenance         = "TERMINATE"
    instance_termination_action = "STOP"

    # graceful_shutdown {
    #   enabled = true
    #   max_duration {
    #     seconds = 600
    #   }
    # }
  }

  metadata = {
    ssh-keys       = "lelis:${local.ssh_pub_key}"
    enable-oslogin = "FALSE"
    user-data      = templatefile("./config/postgres-init.yml.tmpl", {
      wildfire_password = var.db_password
      postgres_password = var.db_postgres_password
    })
  }

  allow_stopping_for_update = true
}
