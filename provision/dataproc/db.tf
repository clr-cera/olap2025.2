data "google_sql_database_instance" "main" {
  name = "usp-olap-wildfire"
}

data "google_alloydb_cluster" "alloy" {
  cluster_id = "usp-olap-wildfire-alloy"
}

data "google_alloydb_instance" "primary" {
  cluster_id = data.google_alloydb_cluster.alloy.cluster_id
  instance_id = "primary"
}