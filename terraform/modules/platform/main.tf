# Cluster add-ons every environment needs, installed with Helm:
#   - Strimzi            (Kafka operator, KRaft mode)
#   - kube-prometheus    (Prometheus + Grafana, scrapes the /metrics ports)
# The Martinet workloads themselves are applied with kustomize afterwards.

terraform {
  required_providers {
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

resource "kubernetes_namespace" "martinet" {
  metadata {
    name = var.namespace
  }
}

resource "helm_release" "strimzi" {
  name             = "strimzi"
  repository       = "oci://quay.io/strimzi-helm"
  chart            = "strimzi-kafka-operator"
  version          = var.strimzi_version
  namespace        = "strimzi"
  create_namespace = true
  set = [{
    name  = "watchNamespaces[0]"
    value = var.namespace
  }]
  depends_on = [kubernetes_namespace.martinet]
}

resource "helm_release" "monitoring" {
  count            = var.enable_monitoring ? 1 : 0
  name             = "monitoring"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = var.prometheus_stack_version
  namespace        = "monitoring"
  create_namespace = true
  values = [yamlencode({
    grafana = { adminPassword = var.grafana_admin_password }
    prometheus = {
      prometheusSpec = {
        # Pick up the prometheus.io/scrape annotations on Martinet pods.
        additionalScrapeConfigs = [{
          job_name              = "martinet-pods"
          kubernetes_sd_configs = [{ role = "pod", namespaces = { names = [var.namespace] } }]
          relabel_configs = [
            { source_labels = ["__meta_kubernetes_pod_annotation_prometheus_io_scrape"], action = "keep", regex = "true" },
            {
              source_labels = ["__address__", "__meta_kubernetes_pod_annotation_prometheus_io_port"]
              regex         = "([^:]+)(?::\\d+)?;(\\d+)"
              replacement   = "$1:$2"
              target_label  = "__address__"
            },
          ]
        }]
      }
    }
  })]
}
