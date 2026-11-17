variable "project_id" {
  description = "GCP project to deploy into."
  type        = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "max_nodes_per_zone" {
  type    = number
  default = 3
}

variable "deletion_protection" {
  description = "Protect the GKE cluster from terraform destroy."
  type        = bool
  default     = true
}

output "cluster_name" {
  value = google_container_cluster.gke.name
}

output "get_credentials" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.gke.name} --region ${var.region} --project ${var.project_id}"
}

output "image_repo" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "workload_service_account" {
  value = google_service_account.workload.email
}
