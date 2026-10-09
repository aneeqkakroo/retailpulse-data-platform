provider "azurerm" {
  features {}
}

provider "databricks" {
  host = "https://${data.azurerm_databricks_workspace.retailpulse.workspace_url}"
}