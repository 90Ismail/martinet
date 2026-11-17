# Cloud Pub/Sub carries the `events` stream on GCP (EVENT_LOG=pubsub).
#
#   ingest --publish(ordering_key = tenant)--> topic events
#   topic events --> subscription fanout (ordered, exactly-once delivery off)
#   fanout nacks N times --> dead_letter_policy --> topic events-dlq
#                                                     --> subscription events-dlq-inspect

resource "google_pubsub_topic" "events" {
  name                       = "events"
  message_retention_duration = "604800s" # 7 days: replay with Seek
  depends_on                 = [google_project_service.apis]
}

resource "google_pubsub_topic" "events_dlq" {
  name                       = "events.dlq"
  message_retention_duration = "2678400s"
}

resource "google_pubsub_subscription" "fanout" {
  name  = "fanout"
  topic = google_pubsub_topic.events.id

  enable_message_ordering    = true # per tenant FIFO, like a Kafka partition key
  ack_deadline_seconds       = 60
  retain_acked_messages      = true # allows Seek-to-timestamp replays
  message_retention_duration = "604800s"

  retry_policy {
    minimum_backoff = "5s"
    maximum_backoff = "300s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.events_dlq.id
    max_delivery_attempts = 10
  }

  expiration_policy {
    ttl = "" # never expire
  }
}

resource "google_pubsub_subscription" "events_dlq_inspect" {
  name                       = "events-dlq-inspect"
  topic                      = google_pubsub_topic.events_dlq.id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
}

# The Pub/Sub service agent must be able to forward to the DLQ topic and ack
# the original message, or dead-lettering silently does nothing.
data "google_project" "this" {}

locals {
  pubsub_agent = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  topic  = google_pubsub_topic.events_dlq.id
  role   = "roles/pubsub.publisher"
  member = local.pubsub_agent
}

resource "google_pubsub_subscription_iam_member" "fanout_subscriber_agent" {
  subscription = google_pubsub_subscription.fanout.id
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
}
