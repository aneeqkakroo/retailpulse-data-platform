output "storage_account_id" {
  value = azurerm_storage_account.retailpulse.id
}

output "databricks_workspace_url" {
  value = data.azurerm_databricks_workspace.retailpulse.workspace_url
}

output "databricks_workspace_id" {
  value = data.azurerm_databricks_workspace.retailpulse.id
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.retailpulse.id
}

output "access_connector_principal_id" {
  value = azurerm_databricks_access_connector.retailpulse.identity[0].principal_id
}

