output "databricks_workspace_url" {
  description = "Access URL of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.workspace_url
}

output "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  value       = azurerm_databricks_workspace.databricks.name
}

output "key_vault_name" {
  description = "Name of the Key Vault"
  value       = azurerm_key_vault.kv.name
}

output "key_vault_uri" {
  description = "URI of the Key Vault"
  value       = azurerm_key_vault.kv.vault_uri
}

output "key_vault_id" {
  description = "Resource ID of the Key Vault"
  value       = azurerm_key_vault.kv.id
}

output "sql_server_name" {
  description = "Name of the SQL Server"
  value       = azurerm_mssql_server.sql_server.name
}

output "sql_database_name" {
  description = "Name of the SQL database"
  value       = azurerm_mssql_database.sql_db.name
}

output "rg_datasource_name" {
  description = "Resource group name for SQL/Key Vault components"
  value       = azurerm_resource_group.rg_datasource.name
}

output "rg_dataplatform_name" {
  description = "Resource group name for Databricks"
  value       = azurerm_resource_group.rg_dataplatform.name
}
