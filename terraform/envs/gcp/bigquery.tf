# BigQuery analytics on delivery attempts (DDIA ch. 10: batch processing over
# a derived copy of the log).
#
#   courier --JSON--> Pub/Sub topic attempts-bq --BigQuery subscription--> martinet.attempts
#
# No exporter service: Pub/Sub writes straight into the table. Set
# BQ_ATTEMPTS_TOPIC=attempts-bq on the courier to turn it on. Example queries
# are in docs/bigquery.sql.

resource "google_bigquery_dataset" "martinet" {
  dataset_id = "martinet"
  location   = "US"
  depends_on = [google_project_service.apis]
}

resource "google_bigquery_table" "attempts" {
  dataset_id          = google_bigquery_dataset.martinet.dataset_id
  table_id            = "attempts"
  deletion_protection = false

  # Pub/Sub writes `publish_time` and `data` (the JSON attempt) per row.
  time_partitioning {
    type  = "DAY"
    field = "publish_time"
  }

  schema = jsonencode([
    { name = "subscription_name", type = "STRING" },
    { name = "message_id", type = "STRING" },
    { name = "publish_time", type = "TIMESTAMP" },
    { name = "attributes", type = "JSON" },
    { name = "data", type = "JSON" },
  ])
}

resource "google_pubsub_topic" "attempts_bq" {
  name       = "attempts-bq"
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "attempts_to_bq" {
  name  = "attempts-to-bigquery"
  topic = google_pubsub_topic.attempts_bq.id

  bigquery_config {
    table          = "${var.project_id}.${google_bigquery_dataset.martinet.dataset_id}.${google_bigquery_table.attempts.table_id}"
    write_metadata = true
  }

  depends_on = [google_project_iam_member.pubsub_bq_writer]
}

resource "google_project_iam_member" "pubsub_bq_writer" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = local.pubsub_agent
}

resource "google_pubsub_topic_iam_member" "courier_bq_publish" {
  topic  = google_pubsub_topic.attempts_bq.id
  role   = "roles/pubsub.publisher"
  member = google_service_account.workload.member
}
