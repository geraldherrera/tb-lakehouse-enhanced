# Azure Subscription ID
variable "subscription_id" {
  description = "Azure Subscription ID used for deployment"
  type        = string
}

# Default region for Databricks and general resource groups
variable "location" {
  description = "Azure region for Databricks and general resources"
  type        = string
  default     = "westeurope"
}

# Dedicated region for SQL Server (e.g., Switzerland North)
variable "location_sql" {
  description = "Azure region for SQL Server deployment"
  type        = string
  default     = "Switzerland North"
}

# SQL credentials
variable "sql_admin" {
  description = "Administrator username for the SQL Server"
  type        = string
  sensitive   = true
}

variable "sql_password" {
  description = "Administrator password for the SQL Server"
  type        = string
  sensitive   = true
}

# Entra ID admin credentials
variable "aad_admin_login" {
  description = "UPN login of the Entra ID administrator"
  type        = string
}

variable "aad_admin_object_id" {
  description = "Azure AD Object ID of the administrator user"
  type        = string
}

# Resource names
variable "rg_datasource_name" {
  description = "Resource group name for SQL Server and Key Vault"
  type        = string
}

variable "rg_dataplatform_name" {
  description = "Resource group name for Databricks"
  type        = string
}

variable "sql_server_name" {
  description = "Name of the SQL Server"
  type        = string
}

variable "sql_database_name" {
  description = "Name of the SQL database"
  type        = string
}

variable "key_vault_name" {
  description = "Name of the Key Vault used for secrets"
  type        = string
}

variable "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  type        = string
}

variable "databricks_managed_rg_name" {
  description = "Name of the resource group automatically managed by Databricks"
  type        = string
}
