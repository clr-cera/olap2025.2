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

resource "google_sql_user" "wildfire" {
  instance = data.google_sql_database_instance.main.name
  name     = var.db_user
  password = var.db_password
  type     = "BUILT_IN"

  depends_on = [google_sql_database.wildfire]
}
