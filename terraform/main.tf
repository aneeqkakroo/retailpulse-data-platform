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


# ==========================================================
# DATABRICKS UNITY CATALOG
# ==========================================================

resource "databricks_storage_credential" "retailpulse" {
  name = "retailpulse_storage_credential"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.retailpulse.id
  }

  comment = "RetailPulse ADLS managed identity credential"

  force_update = true
}


resource "databricks_external_location" "retailpulse" {
  name = "retailpulse_adls"

  url = "abfss://retailpulse@${azurerm_storage_account.retailpulse.name}.dfs.core.windows.net/"

  credential_name = databricks_storage_credential.retailpulse.name

  comment = "RetailPulse ADLS Gen2 external location"

  enable_file_events = true

  lifecycle {
    ignore_changes = [
      file_event_queue
    ]
  }
}


resource "databricks_catalog" "retailpulse" {
  name = "retailpulse"

  comment = "RetailPulse governed data platform catalog"

  storage_root = "abfss://unitycatalog@prdqostzuly2w0bgijcj2ivq.dfs.core.windows.net/uc/3de0d047-0605-45c1-8834-dfe5bd0dff95/e0de20b5-33e4-4f4a-81ba-22f9ef358b52"

  properties = {
    collation = "UTF8_BINARY"
  }
}


resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.retailpulse.name
  name         = "bronze"

  comment = "Raw ingested RetailPulse data"

  lifecycle {
    ignore_changes = [
      properties
    ]
  }
}


resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.retailpulse.name
  name         = "silver"

  comment = "Cleaned and validated RetailPulse data"

  lifecycle {
    ignore_changes = [
      properties
    ]
  }
}


resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.retailpulse.name
  name         = "gold"

  comment = "Business-ready dimensional RetailPulse data"

  lifecycle {
    ignore_changes = [
      properties
    ]
  }
}


resource "databricks_schema" "quarantine" {
  catalog_name = databricks_catalog.retailpulse.name
  name         = "quarantine"

  comment = "Rejected RetailPulse records"

  lifecycle {
    ignore_changes = [
      properties
    ]
  }
}


# ==========================================================
# DATABRICKS JOB
# ==========================================================

resource "databricks_job" "retailpulse_cloud_pipeline" {
  name        = "RetailPulse Cloud Pipeline"
  description = "RetailPulse Silver to Gold cloud data pipeline"

  performance_target = "PERFORMANCE_OPTIMIZED"
  task {
    task_key = "Gold_Dimensional"

    depends_on {
      task_key = "Silver_Incremental"
    }

    run_if      = "ALL_SUCCESS"
    max_retries = 2

    notebook_task {
      notebook_path = "/Workspace/Users/aaneeq21@hotmail.com/05_build_gold_dimensional"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "Silver_Incremental"

    max_retries = 2

    notebook_task {
      notebook_path = "/Workspace/Users/aaneeq21@hotmail.com/04_build_silver_incremental"
      source        = "WORKSPACE"
    }
  }
}