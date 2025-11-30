data "google_sql_database_instance" "main" {
  name = "usp-olap-wildfire"
}

resource "google_sql_database" "wildfire" {
  name            = var.db_name
  instance        = data.google_sql_database_instance.main.name
  charset         = "UTF8"
  collation       = "en_US.UTF8"
  deletion_policy = "ABANDON"
}
