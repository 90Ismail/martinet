# Local environment: a kind cluster on Docker plus the platform add-ons
# (Strimzi for Kafka, Prometheus/Grafana).
#
#   cd terraform/envs/local
#   terraform init && terraform apply
#   make k8s-load k8s-apply      # from the repo root

terraform {
  required_version = ">= 1.6"
  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.7"
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

variable "cluster_name" {
  type    = string
  default = "martinet"
}

resource "kind_cluster" "this" {
  name           = var.cluster_name
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"
      # Expose ingest (HTTP + gRPC) on the host through NodePorts.
      extra_port_mappings {
        container_port = 30080
        host_port      = 8080
      }
      extra_port_mappings {
        container_port = 30090
        host_port      = 9090
      }
    }
    node {
      role = "worker"
    }
    node {
      role = "worker"
    }
  }
}

provider "kubernetes" {
  host                   = kind_cluster.this.endpoint
  cluster_ca_certificate = kind_cluster.this.cluster_ca_certificate
  client_certificate     = kind_cluster.this.client_certificate
  client_key             = kind_cluster.this.client_key
}

provider "helm" {
  kubernetes = {
    host                   = kind_cluster.this.endpoint
    cluster_ca_certificate = kind_cluster.this.cluster_ca_certificate
    client_certificate     = kind_cluster.this.client_certificate
    client_key             = kind_cluster.this.client_key
  }
}

module "platform" {
  source            = "../../modules/platform"
  enable_monitoring = true
}

output "kubeconfig_context" {
  value = "kind-${var.cluster_name}"
}
