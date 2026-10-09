variable "resource_group_name" {
  type    = string
  default = "RetailPulse"
}

variable "location" {
  type    = string
  default = "UK West"
}

variable "storage_account_name" {
  type    = string
  default = "stretailpulsedev21"
}

variable "databricks_workspace_name" {
  type    = string
  default = "dbw-retailpulse-dev"
}

variable "databricks_access_connector_name" {
  type    = string
  default = "ac-retailpulse-dev"
}