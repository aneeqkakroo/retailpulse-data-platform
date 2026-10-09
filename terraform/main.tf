resource "azurerm_resource_group" "retailpulse" {
  name     = var.resource_group_name
  location = var.location

  tags = {}
}

resource "azurerm_storage_account" "retailpulse" {
  name                = var.storage_account_name
  resource_group_name = azurerm_resource_group.retailpulse.name
  location            = azurerm_resource_group.retailpulse.location

  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  access_tier = "Hot"

  is_hns_enabled = true

  public_network_access_enabled   = true
  allow_nested_items_to_be_public = false

  tags = {}
}

data "azurerm_databricks_workspace" "retailpulse" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.retailpulse.name
}


resource "azurerm_databricks_access_connector" "retailpulse" {
  name                = var.databricks_access_connector_name
  resource_group_name = azurerm_resource_group.retailpulse.name
  location            = azurerm_resource_group.retailpulse.location

  identity {
    type = "SystemAssigned"
  }

  tags = {}
}