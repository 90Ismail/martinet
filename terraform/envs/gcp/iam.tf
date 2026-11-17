# Workload Identity: pods running as k8s SA martinet/martinet act as this
# Google service account. No exported JSON keys anywhere.

resource "google_service_account" "workload" {
  account_id   = "martinet-workload"
  display_name = "Martinet workloads (GKE Workload Identity)"
}

resource "google_service_account_iam_member" "wi_binding" {
  service_account_id = google_service_account.workload.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[martinet/martinet]"
  depends_on         = [google_container_cluster.gke]
}

resource "google_pubsub_topic_iam_member" "ingest_publish" {
  for_each = toset([google_pubsub_topic.events.id, google_pubsub_topic.events_dlq.id])
  topic    = each.value
  role     = "roles/pubsub.publisher"
  member   = google_service_account.workload.member
}

resource "google_pubsub_subscription_iam_member" "fanout_consume" {
  subscription = google_pubsub_subscription.fanout.id
  role         = "roles/pubsub.subscriber"
  member       = google_service_account.workload.member
}

# GKE nodes pull images from Artifact Registry.
resource "google_artifact_registry_repository_iam_member" "node_pull" {
  repository = google_artifact_registry_repository.images.name
  location   = var.region
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${data.google_project.this.number}-compute@developer.gserviceaccount.com"
}

# ---------------------------------------------------------------- add-ons

data "google_client_config" "current" {}

provider "kubernetes" {
  host                   = "https://${google_container_cluster.gke.endpoint}"
  token                  = data.google_client_config.current.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.gke.master_auth[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes = {
    host                   = "https://${google_container_cluster.gke.endpoint}"
    token                  = data.google_client_config.current.access_token
    cluster_ca_certificate = base64decode(google_container_cluster.gke.master_auth[0].cluster_ca_certificate)
  }
}

module "platform" {
  source     = "../../modules/platform"
  depends_on = [google_container_node_pool.default]
}
