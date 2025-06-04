import os
import json
import requests
import subprocess
import time
from base64 import b64encode

# ========================== #
# USER CONFIG                #
# ========================== #

# Replace these with your actual paths and names
DATABRICKS_NOTEBOOK_FOLDER = "/Workspace/Users/gerald.herrera@he-arc.ch"
CLUSTER_NAME = "Personal Compute - Gerald Herrera"
POLICY_NAME = "Personal Policy - GHE"
DATABRICKS_WORKFLOW_NAME = "lakehouse_etl_pipeline-ghe"
JOB_TASK_BRONZE_NAME = "task_bronze_ghe"
JOB_TASK_SILVER_NAME = "task_silver_ghe"
JOB_TASK_GOLD_DIM_NAME = "task_gold_dim_ghe"
JOB_TASK_GOLD_FACT_NAME = "task_gold_fact_ghe"

NOTEBOOKS_DIR = "./notebooks"
NOTEBOOK_TO_INIT = "01_Init"
NOTEBOOK_BRONZE = "12_ETL_Bronze_PySpark"
NOTEBOOK_SILVER = "22_ETL_Silver_PySpark"
NOTEBOOK_GOLD_DIM = "33_ETL_Gold_Dim_PySpark"
NOTEBOOK_GOLD_FACT = "34_ETL_Gold_Fact_PySpark"
# ======================= #
# MAIN FONCTIONS          #
# ======================= #

def load_terraform_outputs():
    """
    Load all Terraform outputs and build an enriched dictionary,
    including 'host' (alias for the Databricks URL).
    """
    print("Reading Terraform outputs...")
    raw = subprocess.check_output(["terraform", "output", "-json"])
    data = json.loads(raw)

    url = data["databricks_workspace_url"]["value"].strip()
    if not url.startswith("https://"):
        url = f"https://{url}"
    url = url.rstrip("/")

    data["host"] = url

    return data

def create_databricks_client(host, token):
    """
    Create an API client to interact with Databricks.

    Args:
        host (str): Databricks workspace URL.
        token (str): Databricks authentication token.

    Returns:
        function: Function to perform API calls to Databricks.
    """
    def call(method, endpoint, **kwargs):
        url = f"{host}/api/2.0{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}"
        }
        response = requests.request(method, url, headers=headers, **kwargs)
        if not response.ok:
            print(f"Databricks error {response.status_code} - {response.text}")
            response.raise_for_status()
        return response.json() if response.text else {}
    return call

def write_databrickscfg(workspace_host, aad_token):
    """
    Generate the ~/.databrickscfg file for Databricks CLI using Azure AD token.

    Args:
        workspace_host (str): Databricks workspace URL (https://...).
        aad_token (str): Azure AD token obtained via `az account get-access-token`.
    """
    config_file = os.path.expanduser("~/.databrickscfg")
    os.makedirs(os.path.dirname(config_file), exist_ok=True)

    content = (
        "[DEFAULT]\n"
        f"host = {workspace_host}\n"
        f"token = {aad_token}\n"
    )

    with open(config_file, "w", encoding="utf-8") as f:
        f.write(content)
    os.chmod(config_file, 0o600)

    print(f"Databricks CLI config file written: : {config_file}")

def ensure_secret_scope(outputs):
    """
    Create a Databricks secret scope with Azure Key Vault backend.

    Args:
        outputs (dict): Terraform outputs, including:
                        - key_vault_uri
                        - key_vault_id
                        - databricks_workspace_url

    Effects:
        - Creates the scope if it doesn't exist
        - Ignores error if already exists
        - Stops on unexpected error
    """
    scope_name = "kv-jdbc"
    vault_uri = outputs["key_vault_uri"]["value"]
    vault_resource_id = outputs["key_vault_id"]["value"]
    workspace_host = f"https://{outputs['databricks_workspace_url']['value']}"

    print(f"Creating (or checking) Secret Scope: {scope_name}...")

    # 1 : Get Azure AD token for Databricks CLI
    token_result = subprocess.run([
        "az", "account", "get-access-token",
        "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
        "--query", "accessToken",
        "-o", "tsv"
    ], capture_output=True, text=True, check=True)
    aad_token = token_result.stdout.strip()

    # 2 : Generate the ~/.databrickscfg file from the Azure AD token
    write_databrickscfg(workspace_host, aad_token)

    # 3 : Make the payload for the secret scope creation
    scope_payload = {
        "scope": scope_name,
        "scope_backend_type": "AZURE_KEYVAULT",
        "backend_azure_keyvault": {
            "resource_id": vault_resource_id,
            "dns_name": vault_uri
        }
    }

    #  4 : Run the Databricks CLI command to create the secret scope
    result = subprocess.run([
        "databricks", "secrets", "create-scope",
        "--json", json.dumps(scope_payload)
    ], capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Secret Scope '{scope_name}' created successfully.")
    elif "RESOURCE_ALREADY_EXISTS" in result.stderr:
        print(f"Secret Scope '{scope_name}' already exists.")
    else:
        print("Error while creating the scope :")
        print(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)


def ensure_cluster_policy(client):
    """
    Check existence of a cluster policy and create it if needed.

    Args:
        client (function): Databricks API client.

    Returns:
        str: Cluster policy ID.
    """
    print(f"Creating cluster policy : {POLICY_NAME}")
    policies = client("get", "/policies/clusters/list").get("policies", [])
    for p in policies:
        if p["name"] == POLICY_NAME:
            print("Policy already exists.")
            return p["policy_id"]

    definition = {
        "node_type_id": {
            "type": "allowlist",
            "values": ["Standard_F4"],
            "defaultValue": "Standard_F4"
        },
        "spark_version": {
            "type": "unlimited",
            "defaultValue": "auto:latest-ml"
        },
        "runtime_engine": {
            "type": "fixed",
            "value": "STANDARD",
            "hidden": True
        },
        "num_workers": {
            "type": "fixed",
            "value": 0,
            "hidden": True
        },
        "data_security_mode": {
            "type": "allowlist",
            "values": [
                "SINGLE_USER",
                "LEGACY_SINGLE_USER",
                "LEGACY_SINGLE_USER_STANDARD"
            ],
            "defaultValue": "SINGLE_USER",
            "hidden": True
        },
        "driver_instance_pool_id": {
            "type": "forbidden",
            "hidden": True
        },
        "cluster_type": {
            "type": "fixed",
            "value": "all-purpose"
        },
        "instance_pool_id": {
            "type": "forbidden",
            "hidden": True
        },
        "azure_attributes.availability": {
            "type": "fixed",
            "value": "ON_DEMAND_AZURE",
            "hidden": True
        },
        "spark_conf.spark.databricks.cluster.profile": {
            "type": "fixed",
            "value": "singleNode",
            "hidden": True
        },
        "autotermination_minutes": {
            "type": "fixed",
            "value": 10,
            "hidden": True
        }
    }

    policy = client("post", "/policies/clusters/create", json={
        "name": POLICY_NAME,
        "definition": json.dumps(definition)
    })
    return policy["policy_id"]

def ensure_cluster(client, policy_id):
    """
    Check existence of a personal cluster and create it if needed.

    Args:
        client (function): Databricks API client.
        policy_id (str): Cluster policy ID to apply.

    Returns:
        str: Cluster ID.
    """
    print(f"Checking or creating cluster : {CLUSTER_NAME}")
    clusters = client("get", "/clusters/list").get("clusters", [])
    for c in clusters:
        if c["cluster_name"] == CLUSTER_NAME:
            print("Cluster already exists.")
            return c["cluster_id"]

    cluster = client("post", "/clusters/create", json={
        "cluster_name": CLUSTER_NAME,
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "Standard_F4",
        "policy_id": policy_id,
        "autotermination_minutes": 10,
        "num_workers": 1,
        "data_security_mode": "SINGLE_USER"
    })
    return cluster["cluster_id"]

def import_py_files(client, folder=DATABRICKS_NOTEBOOK_FOLDER):
    """
    Import .py (Databricks Source Files) files into Databricks workspace.

    Args:
        client (function): Databricks API client.
        folder (str): Target workspace folder.
    """
    print(f"Importing py files from {NOTEBOOKS_DIR} to {folder}")
    for file in os.listdir(NOTEBOOKS_DIR):
        if file.endswith(".py"):
            filepath = os.path.join(NOTEBOOKS_DIR, file)
            print(f"Importing {file}")
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()

        encoded = b64encode(content.encode("utf-8")).decode("utf-8")

        client("post", "/workspace/import", json={
            "path": f"{folder}/{file.replace('.py', '')}",
            "format": "SOURCE",
            "language": "PYTHON",
            "content": encoded
        })


def run_notebook_and_wait(client, notebook_path, cluster_id, retry_on_quota_error=True):
    """
    Run a Databricks notebook and wait for it to finish.

    Args:
        client (function): Databricks API client.
        notebook_path (str): Path of the notebook in the workspace.
        cluster_id (str): Cluster ID to run the notebook.
        retry_on_quota_error (bool): Retry on quota error or not.
    """
    print(f"Running notebook : {notebook_path}")
    payload = {
        "run_name": "Automatic Initialization",
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path
        }
    }
    response = client("post", "/jobs/runs/submit", json=payload)
    run_id = response.get("run_id")
    print(f"Job submitted with ID : {run_id}")

    while True:
        time.sleep(10)
        status = client("get", f"/jobs/runs/get?run_id={run_id}")
        life_cycle = status["state"]["life_cycle_state"]
        result_state = status["state"].get("result_state")
        print(f"Statut : {life_cycle} / Result : {result_state}")
        if life_cycle in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break

    if result_state == "SUCCESS":
        print("Notebook executed successfully.")
    else:
        print("Notebook failed. Details:")
        print(status.get("state", {}))

def ensure_sql_warehouse(client):
    """
    Check existence of a Serverless SQL Warehouse and create it if needed.

    Args:
        client (function): Databricks API client.

    Returns:
        str: SQL Warehouse ID.
    """
    print("Checking or creating Serverless SQL Warehouse...")
    warehouses = client("get", "/sql/warehouses").get("warehouses", [])
    for w in warehouses:
        if w["name"] == "Serverless SQL":
            print("Warehouse already exists.")
            return w["id"]

    payload = {
        "name": "Serverless SQL",
        "cluster_size": "2X-Small",
        "enable_serverless_compute": True,
        "auto_stop_mins": 10,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "channel": {
            "name": "CHANNEL_NAME_CURRENT"
        }
    }

    response = client("post", "/sql/warehouses", json=payload)
    print(f"SQL Warehouse created with ID : {response['id']}")
    return response["id"]

def ensure_etl_workflow(client, cluster_id):
    """
    Create a Databricks job daily at 2:00 AM
    and trigger it immediately after creation.

    Args:
        client (function): Databricks API client.
        cluster_id (str): Existing cluster ID used for tasks.

    Returns:
        str: Job ID.
    """
    print("Checking or creating scheduled ETL job...")

    jobs = client("get", "/jobs/list").get("jobs", [])
    for job in jobs:
        if job["settings"]["name"] == DATABRICKS_WORKFLOW_NAME:
            print("Job already exists.")
            return job["job_id"]

    bronze_task = {
        "task_key": JOB_TASK_BRONZE_NAME,
        "notebook_task": {
            "notebook_path": f"{DATABRICKS_NOTEBOOK_FOLDER}/{NOTEBOOK_BRONZE}"
        },
        "existing_cluster_id": cluster_id
    }

    silver_task = {
        "task_key": JOB_TASK_SILVER_NAME,
        "depends_on": [{"task_key": JOB_TASK_BRONZE_NAME}],
        "notebook_task": {
            "notebook_path": f"{DATABRICKS_NOTEBOOK_FOLDER}/{NOTEBOOK_SILVER}"
        },
        "existing_cluster_id": cluster_id
    }

    gold_dim_task = {
        "task_key": JOB_TASK_GOLD_DIM_NAME,
        "depends_on": [{"task_key": JOB_TASK_SILVER_NAME}],
        "notebook_task": {
            "notebook_path": f"{DATABRICKS_NOTEBOOK_FOLDER}/{NOTEBOOK_GOLD_DIM}"
        },
        "existing_cluster_id": cluster_id
    }

    gold_fact_task = {
        "task_key": JOB_TASK_GOLD_FACT_NAME,
        "depends_on": [{"task_key": JOB_TASK_GOLD_DIM_NAME}],
        "notebook_task": {
            "notebook_path": f"{DATABRICKS_NOTEBOOK_FOLDER}/{NOTEBOOK_GOLD_FACT}"
        },
        "existing_cluster_id": cluster_id
    }

    payload = {
        "name": DATABRICKS_WORKFLOW_NAME,
        "tasks": [bronze_task, silver_task, gold_dim_task, gold_fact_task],
        "format": "MULTI_TASK"
    }

    response = client("post", "/jobs/create", json=payload)
    job_id = response["job_id"]
    print(f"Job created with ID : {job_id}")

    print("Triggering job immediately for first run...")
    client("post", "/jobs/run-now", json={"job_id": job_id})

    return job_id

# ========================== #
# SCRIPT ENTRY POINT         #
# ========================== #

if __name__ == "__main__":
    outputs = load_terraform_outputs()
    ensure_secret_scope(outputs)
    token = input("Enter your Databricks token : ").strip()
    client = create_databricks_client(outputs["host"], token)
    policy_id = ensure_cluster_policy(client)
    cluster_id = ensure_cluster(client, policy_id)
    import_py_files(client)
    run_notebook_and_wait(client, f"{DATABRICKS_NOTEBOOK_FOLDER}/{NOTEBOOK_TO_INIT}", cluster_id)
    warehouse_id = ensure_sql_warehouse(client)
    workflow_id = ensure_etl_workflow(client, cluster_id)
