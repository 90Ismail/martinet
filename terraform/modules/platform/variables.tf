variable "namespace" {
  description = "Namespace the Martinet workloads run in."
  type        = string
  default     = "martinet"
}

variable "strimzi_version" {
  type    = string
  default = "0.45.0"
}

variable "prometheus_stack_version" {
  type    = string
  default = "67.4.0"
}

variable "enable_monitoring" {
  type    = bool
  default = true
}

variable "grafana_admin_password" {
  description = "Grafana admin password. Set via TF_VAR_grafana_admin_password; never commit it."
  type        = string
  sensitive   = true
  default     = "change-me"
}
