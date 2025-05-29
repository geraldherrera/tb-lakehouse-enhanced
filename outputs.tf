output "databricks_workspace_url" {
  description = "URL d’accès au workspace Databricks"
  value       = azurerm_databricks_workspace.databricks.workspace_url
}

output "databricks_workspace_name" {
  description = "Nom du workspace Databricks"
  value       = azurerm_databricks_workspace.databricks.name
}

output "key_vault_name" {
  description = "Nom du Key Vault"
  value       = azurerm_key_vault.kv.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}

output "key_vault_id" {
  value = azurerm_key_vault.kv.id
}

output "sql_server_name" {
  description = "Nom du serveur SQL"
  value       = azurerm_mssql_server.sql_server.name
}

output "sql_database_name" {
  description = "Nom de la base de données SQL"
  value       = azurerm_mssql_database.sql_db.name
}

output "rg_datasource_name" {
  description = "Nom du groupe de ressources pour les composants SQL/Key Vault"
  value       = azurerm_resource_group.rg_datasource.name
}

output "rg_dataplatform_name" {
  description = "Nom du groupe de ressources pour Databricks"
  value       = azurerm_resource_group.rg_dataplatform.name
}
