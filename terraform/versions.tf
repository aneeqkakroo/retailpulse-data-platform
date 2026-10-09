terraform {
  required_version = ">= 1.7.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "4.81.0"
    }

    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.134"
    }
  }
}