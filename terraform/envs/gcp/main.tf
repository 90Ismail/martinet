# Google Cloud environment.
#
#   VPC (private nodes, Cloud NAT for egress to webhook endpoints)
#   GKE Standard, regional, Workload Identity, autoscaling node pool
#   Artifact Registry for the two images
#   Pub/Sub: events topic + fanout subscription with ordering, exponential
#            retry and a dead-letter topic (Pub/Sub's native DLQ)
#   BigQuery: attempts analytics (bigquery.tf)
#   IAM: one Google service account bound to the k8s SA via Workload Identity
#
#   terraform init && terraform apply -var project_id=my-project

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.35"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  name = "martinet"
  apis = [
    "container.googleapis.com", "artifactregistry.googleapis.com", "pubsub.googleapis.com",
    "storage.googleapis.com", "iam.googleapis.com", "cloudbuild.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

# ---------------------------------------------------------------- network

resource "google_compute_network" "vpc" {
  name                    = "${local.name}-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "nodes" {
  name                     = "${local.name}-nodes"
  network                  = google_compute_network.vpc.id
  region                   = var.region
  ip_cidr_range            = "10.10.0.0/20"
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/14"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

resource "google_compute_router" "router" {
  name    = "${local.name}-router"
  network = google_compute_network.vpc.id
  region  = var.region
}

# Couriers call arbitrary public URLs; private nodes need NAT to do so.
resource "google_compute_router_nat" "nat" {
  name                               = "${local.name}-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# ---------------------------------------------------------------- GKE

resource "google_container_cluster" "gke" {
  name     = local.name
  location = var.region # regional: control plane + nodes across 3 zones

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.nodes.id

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = var.deletion_protection

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  release_channel {
    channel = "REGULAR"
  }
}

resource "google_container_node_pool" "default" {
  name     = "default"
  cluster  = google_container_cluster.gke.id
  location = var.region

  autoscaling {
    min_node_count = 1
    max_node_count = var.max_nodes_per_zone
  }

  node_config {
    machine_type = var.machine_type
    disk_size_gb = 50
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    labels = { app = local.name }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# ---------------------------------------------------------------- registry

resource "google_artifact_registry_repository" "images" {
  repository_id = local.name
  location      = var.region
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]

  cleanup_policies {
    id     = "keep-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = 20
    }
  }
}

