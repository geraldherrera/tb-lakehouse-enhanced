# Identifiant de l'abonnement Azure
variable "subscription_id" {
  description = "Azure Subscription ID utilisé pour le déploiement"
  type        = string
}

# Région par défaut pour Databricks et les groupes de ressources
variable "location" {
  description = "Région Azure pour Databricks et autres ressources générales"
  type        = string
  default     = "westeurope"
}

# Région dédiée au SQL Server (ex: Switzerland North)
variable "location_sql" {
  description = "Région Azure pour le déploiement du serveur SQL"
  type        = string
  default     = "Switzerland North"
}

# Identifiants SQL
variable "sql_admin" {
  description = "Nom d'utilisateur administrateur pour le serveur SQL"
  type        = string
  sensitive   = true
}

variable "sql_password" {
  description = "Mot de passe administrateur pour le serveur SQL"
  type        = string
  sensitive   = true
}

# 
variable "aad_admin_login" {
  description = "Login UPN de l'administrateur Entra ID"
  type        = string
}

variable "aad_admin_object_id" {
  description = "Object ID Azure AD de l'utilisateur administrateur"
  type        = string
}

# Noms de ressources
variable "rg_datasource_name" {
  description = "Nom du groupe de ressources pour le serveur SQL et Key Vault"
  type        = string
}

variable "rg_dataplatform_name" {
  description = "Nom du groupe de ressources pour Databricks"
  type        = string
}

variable "sql_server_name" {
  description = "Nom du serveur SQL"
  type        = string
}

variable "sql_database_name" {
  description = "Nom de la base de données SQL"
  type        = string
}

variable "key_vault_name" {
  description = "Nom du Key Vault utilisé pour les secrets"
  type        = string
}

variable "databricks_workspace_name" {
  description = "Nom du workspace Databricks"
  type        = string
}

variable "databricks_managed_rg_name" {
  description = "Nom du groupe de ressources managé automatiquement par Databricks"
  type        = string
}
