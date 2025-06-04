provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

data "azurerm_client_config" "current" {}

# Resource group for SQL Server, database, and Key Vault
resource "azurerm_resource_group" "rg_datasource" {
  name     = var.rg_datasource_name
  location = var.location_sql
}

# Resource group for Databricks
resource "azurerm_resource_group" "rg_dataplatform" {
  name     = var.rg_dataplatform_name
  location = var.location
}

# SQL Server
resource "azurerm_mssql_server" "sql_server" {
  name                         = var.sql_server_name
  resource_group_name          = azurerm_resource_group.rg_datasource.name
  location                     = azurerm_resource_group.rg_datasource.location
  version                      = "12.0"
  administrator_login          = var.sql_admin
  administrator_login_password = var.sql_password

  identity {
    type = "SystemAssigned"
  }

  azuread_administrator {
    login_username = var.aad_admin_login
    object_id      = var.aad_admin_object_id
  }
}

# SQL Database with configuration compatible with student subscriptions
resource "azurerm_mssql_database" "sql_db" {
  name                                = var.sql_database_name
  server_id                           = azurerm_mssql_server.sql_server.id
  sku_name                            = "Basic"
  max_size_gb                         = 2
  sample_name                         = "AdventureWorksLT"
  storage_account_type                = "Local"
  collation                           = "SQL_Latin1_General_CP1_CI_AS"
  transparent_data_encryption_enabled = true

  tags = {
    environment = "dev"
  }
}

# Azure Key Vault
resource "azurerm_key_vault" "kv" {
  name                        = var.key_vault_name
  location                    = azurerm_resource_group.rg_datasource.location
  resource_group_name         = azurerm_resource_group.rg_datasource.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "Set", "List"]
  }
}

# Secrets in the Key Vault
resource "azurerm_key_vault_secret" "sql_username" {
  name         = "sql-username"
  value        = var.sql_admin
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "sql_password" {
  name         = "sql-password"
  value        = var.sql_password
  key_vault_id = azurerm_key_vault.kv.id
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "databricks" {
  name                          = var.databricks_workspace_name
  location                      = azurerm_resource_group.rg_dataplatform.location
  resource_group_name           = azurerm_resource_group.rg_dataplatform.name
  managed_resource_group_name   = var.databricks_managed_rg_name
  sku                           = "premium"

  tags = {
    environment = "dev"
  }
}
