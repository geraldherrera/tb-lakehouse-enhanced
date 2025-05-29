# 📦 Lakehouse Infrastructure – Deployment with Terraform

This repository contains the full automation of the infrastructure for a Lakehouse-type project on Azure. Deployment is based on Terraform, with a Python post-deployment script to finalize the Databricks workspace setup and import notebooks.

## Features

- Creation of dedicated resource groups for SQL and Databricks
- Deployment of a SQL Server with AdventureWorksLT database (student format)
- Creation of an Azure Key Vault containing SQL credentials
- Provisioning of an Azure Databricks Workspace with a managed resource group
- Generation of reusable Terraform outputs for scripts
- Versioned storage of `.py` notebooks in the `/notebooks/` folder:
  - `1.0_Initialize.py`
  - `2.0_ETL_Bronze_Layer.py`
  - `2.5_Test_Bronze_Layer.py`
  - `3.0_ETL_Silver_Layer.py`
  - `3.5_Test_Silver_Layer.py`
  - `4.0_ETL_Gold_Layer.py`
  - `5.0_Spaceship_Titanic.py`
  - `5.1_Sample_Bakehouse.py`

## Deployment Steps

### 1. Clone the Repository

```bash
git clone https://github.com/geraldherrera/tb-lakehouse-enhanced.git
cd infra-azure-lakehouse
```

### 2. Prepare Configuration Files

> ⚠️ The Databricks workspace name **must always start with `dbw-`**, otherwise there will be issues with notebook paths in Databricks.

Update the following files:

- `secrets.auto.tfvars`
```hcl
subscription_id     = "<your-subscription-id>"
sql_admin           = "<sql-username>"
sql_password        = "<sql-password>"
aad_admin_login     = "<your-email@domain.com>"
aad_admin_object_id = "<your-user-object-id>"
```

- `terraform.tfvars`
```hcl
location                    = "westeurope"
location_sql                = "switzerlandnorth"
rg_datasource_name          = "rg-datasource-dev-ghe"
rg_dataplatform_name        = "rg-dataplatform-dev-ghe"
sql_server_name             = "sql-datasource-dev-ghe"
sql_database_name           = "sqldb-adventureworks-dev-ghe"
key_vault_name              = "kv-jdbc-secrets-dev-ghe"
databricks_workspace_name   = "dbw-dataplatform-dev-ghe"
databricks_managed_rg_name  = "mg-dataplatform-dev-ghe"
```

### 3. Initialize and Deploy Infrastructure

```bash
terraform init
terraform apply
```
Press `yes` when prompted.

### 4. Configure Azure SQL Access

Go to Azure Portal:
- Open the SQL Server
- Go to **Security** tab
- Add your IP address to the firewall rules
- Allow Azure services to access the SQL Server

### 5. Modify Notebooks with Deployment Outputs

After infrastructure deployment, retrieve required values (hostname, database name, workspace name) from Terraform outputs or the Azure portal.

Update the following notebooks:

- `2.0_ETL_Bronze_Layer.py` → `jdbc_hostname`, `jdbc_database`
- `2.5_Test_Bronze_Layer.py` → `jdbc_hostname`, `jdbc_database`
- `5.0_Spaceship_Titanic.py` → `user_workspace_name`

### 6. Prepare Finalization Script

Edit the `finalize_databricks_deployment.py` script to set:

- `DATABRICKS_NOTEBOOK_FOLDER`
- `CLUSTER_NAME`
- `POLICY_NAME`
- `DATABRICKS_WORKFLOW_NAME`
- `JOB_TASK_BRONZE_NAME`
- `JOB_TASK_SILVER_NAME`
- `JOB_TASK_GOLD_NAME`

### 7. Generate Databricks Token

In the Databricks UI:
- Go to **User Settings > Developer**
- Create a **Personal Access Token**

### 8. Run Finalization Script

```bash
python finalize_databricks_deployment.py
```

> ⚠️ You will be prompted to enter your Databricks personal access token.

### 9. Wait for Initialization to Complete

The script will:
- Import notebooks
- Start the cluster
- Execute the initialization notebook

> ⏳ This may take several minutes due to cluster startup time.

### 10. Monitor Pipeline Execution

The script launches the ETL pipeline. You can follow execution in the Databricks interface under **Jobs > <your job name>**.


## ETL Orchestration in Databricks (Workflow)

Once infrastructure is deployed and notebooks are imported, a Databricks workflow is automatically created by the `post_deploy.py` script.

By default this workflow is called **`lakehouse_etl_pipeline-ghe`** and runs daily at **2:00 AM**. It follows a three-layer architecture with the following tasks:

```
| Task             | Notebook Path                                                    | Dependency          |
|------------------|------------------------------------------------------------------|---------------------|
| `task_bronze_ghe`| `/Workspace/Users/gerald.herrera@he-arc.ch/2.0_ETL_Bronze_Layer` | None                |
| `task_silver_ghe`| `/Workspace/Users/gerald.herrera@he-arc.ch/3.0_ETL_Silver_Layer` | `task_bronze_ghe`   |
| `task_gold_ghe`  | `/Workspace/Users/gerald.herrera@he-arc.ch/4.0_ETL_Gold_Layer`   | `task_silver_ghe`   |
```

Each task runs on the personal cluster preconfigured by the script.

> This workflow ensures consistent execution of the data pipeline from bronze to gold layers.

---

## Power BI Access via Serverless Warehouse

The `finalize_databricks_deployment.py` script also creates a Serverless SQL Warehouse named "Serverless SQL". It is automatically configured with the following settings:

```
Type : Serverless

Size : 2X-Small (XXS)

Auto Stop : 10 minutes

Min/Max Clusters : 1

Channel : Current
```

This warehouse can be used immediately from Power BI as a direct data source with no further setup.

To connect, use the Databricks connector in Power BI and select the "Serverless SQL" warehouse.

## Power BI Dashboard

The `business_sales_dashboard.pbix` file is an interactive Power BI dashboard providing a comprehensive overview of sales, products, customers, and business operations.

It is based on a star schema built from Gold layer data in Databricks, and covers the following topics:

```
Business Overview: Key KPIs, monthly trends, sales by region.

Products and Catalog: Top-selling products, unsold items, performance by category.

Customer Analysis: Geographic distribution, orders per customer, most profitable clients.
```

This file is designed to connect directly to the automatically deployed "Serverless SQL" warehouse. Just add it as a data source.

## Repository Structure

```
infra-azure-lakehouse/
├── main.tf                               # Azure infrastructure deployment
├── variables.tf                          # Terraform variables file
├── outputs.tf                            # Extracted outputs for Python script
├── terraform.tfvars                      # Resource name values
├── notebooks/                            # Databricks notebooks (.ipynb)
│   ├── 1.0_Initialize.py
│   ├── 2.0_ETL_Bronze_Layer.py
│   ├── 2.5_Test_Bronze_Layer.py
│   ├── 3.0_ETL_Silver_Layer.py
│   ├── 3.5_Test_Silver_Layer.py
│   ├── 4.0_ETL_Gold_Layer.py
│   ├── 5.0_Spaceship_Titanic.py
│   └── 5.1_Sample_Bakehouse.py
├── finalize_databricks_deployment.py     # Python post-deployment script
├── gold_ddl_diagram.md                   # Mermaid DDL diagram for Gold layer
├── gold_ddl_diagram.svg                  # SVG export of Gold DDL diagram
├── business_sales_dashboard.pbix         # Power BI dashboard (sales, products, clients)
└── README.md                             # This file
```
## Notes

- If at least the `secrets.auto.tfvars` file is properly filled and the `user_workspace_name` variable in `5.0_Spaceship_Titanic.py` is correctly updated, the entire process should work **without any additional manual modifications**.

---

🛠️ This project was designed to minimize manual actions and ensure reproducible deployment on Azure + Databricks. It can serve as a base for any Lakehouse architecture in academic or professional environments.