# Databricks notebook source
# MAGIC %md
# MAGIC # Spaceship Titanic - Kaggle competition
# MAGIC
# MAGIC https://www.kaggle.com/competitions/spaceship-titanic
# MAGIC
# MAGIC This notebook serves as an introduction for the class about supervised learning.
# MAGIC Students are introduced to predictive modeling. They build a Scikit-Learn pipeline and fit a simple DecisionTreeClassifier on the Spaceship Titanic dataset. The model is evaluated with a kaggle submission.

# COMMAND ----------

# MAGIC %md
# MAGIC ## You must manually create a folder ml_sandbox in your workspace before starting ! 

# COMMAND ----------

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
unity_catalogs = [c for c in catalogs if c != "hive_metastore"]

if len(unity_catalogs) == 1:
    default_catalog = unity_catalogs[0]
else:
    default_catalog = next((c for c in unity_catalogs if c.startswith("dbw_")), "hive_metastore")

dbutils.widgets.text("my_catalog", default_catalog, "Detected Catalog")
catalog = dbutils.widgets.get("my_catalog")

schema = "ml_sandbox"
volume = "ml_volume"
user_workspace_name = "gerald.herrera@he-arc.ch"

# COMMAND ----------

# COMMAND ----------

import mlflow
import mlflow.sklearn

# Set the experiment name to organize runs in MLflow
mlflow.set_experiment(f"/Users/{user_workspace_name}/ml_sandbox/titanic_demo")

volume_path = f"{catalog}.{schema}.{volume}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")

print(f"Volume path : /Volumes/{catalog}/{schema}/{volume}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## You must upload train.csv and test.csv in the volume we just created !

# COMMAND ----------

# MAGIC %md
# MAGIC We load the train data. The PassengerId column is used as the index of the dataframe

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

spark.sql(f"""
    CREATE OR REPLACE TABLE titanic_train
    USING DELTA
    AS
    SELECT *
    FROM read_files(
      '/Volumes/{catalog}/{schema}/{volume}/train.csv',
      format => 'csv',
      header => true
    )
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE titanic_test
    USING DELTA
    AS
    SELECT *
    FROM read_files(
      '/Volumes/{catalog}/{schema}/{volume}/test.csv',
      format => 'csv',
      header => true
    )
""")


# COMMAND ----------

import pandas as pd
import numpy as np

train_spark = spark.table(f"{catalog}.{schema}.titanic_train")

train = train_spark.toPandas()
train.set_index("PassengerId", inplace=True)

train

# COMMAND ----------

train.info()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessing Pipeline
# MAGIC
# MAGIC We identified null values in all columns. We will clean these by type.

# COMMAND ----------

train.isna().sum()

# COMMAND ----------

# pip install scikit-learn

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder

train_spark = spark.table(f"{catalog}.{schema}.titanic_train")

train = train_spark.toPandas()
train.set_index("PassengerId", inplace=True)

# Step 1: Define transformers for different column types
numerical_cols = ['Age', 'RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
numeric_transformer = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="mean"))]
)

categorical_cols = ['HomePlanet', 'Destination', 'VIP', 'CryoSleep']
categorical_transformer = Pipeline(
    steps=[
        ('encoder', OneHotEncoder())
])

# Step 2: Create a ColumnTransformer that applies the transformations to the columns
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numerical_cols),
        ('cat', categorical_transformer, categorical_cols)
    ],
    remainder='drop' 
)

# Step 3: Assemble the preprocessing pipeline
preprocessing_pipeline = Pipeline([
    ('preprocessor', preprocessor)
])

# Fit and transform the DataFrame
X_train = preprocessing_pipeline.fit_transform(train)

preprocessing_pipeline

# COMMAND ----------

# Converting back to Pandas DataFrame
onehot_encoder_feature_names = list(preprocessing_pipeline.named_steps['preprocessor'].named_transformers_['cat'].named_steps['encoder'].get_feature_names_out())
column_order =  numerical_cols + onehot_encoder_feature_names

# Show the cleaned DataFrame
pd.DataFrame(X_train, columns=column_order, index=train.index)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision Tree Classifier 
# MAGIC
# MAGIC We extend the pipeline with a decision tree classifier to predict the Transported variable.

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier
from mlflow.tracking import MlflowClient
from sklearn.metrics import accuracy_score

X = train.drop('Transported', axis=1)
y = train['Transported']

# Define the hyperparameters for the DecisionTreeClassifier
hyperparams = {
    'criterion': 'entropy',     # Function to measure the quality of a split
    'max_depth': 3,             # Limits the depth of the tree to prevent overfitting
    'min_samples_split': 20,    # The minimum number of samples required to split an internal node
    'min_samples_leaf': 10,     # The minimum number of samples required to be at a leaf node
    'random_state': 42          # Ensures reproducibility of the results
}

# Update the model pipeline with the new DecisionTreeClassifier parameters
model_pipeline = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', DecisionTreeClassifier(**hyperparams))
])

# Force Unity Catalog registry
mlflow.set_registry_uri("databricks-uc")

model_name = f"{catalog}.{schema}.Titanic_DT"

mlflow.sklearn.autolog(
    log_models=True,
    registered_model_name=model_name
)

with mlflow.start_run(run_name="dt_baseline") as run:
    model_pipeline.fit(X, y)
    train_acc = accuracy_score(y, model_pipeline.predict(X))
    mlflow.log_metric("train_accuracy", train_acc)

    client = MlflowClient()

    # Check model version
    all_versions = client.search_model_versions(f"name = '{model_name}'")

    # Check for the current model version
    run_id = run.info.run_id
    version = next(
        mv.version for mv in all_versions if mv.run_id == run_id
    )

    # Apply "champion" alias
    client.set_registered_model_alias(model_name, "champion", version)

    print(f"'Champion' alias applied to version {version}")

model_pipeline

# COMMAND ----------

# pip install matplotlib

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tree visualization
# MAGIC
# MAGIC We use matplotlib library to plot the tree we just fitted

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.tree import plot_tree

# Extract the decision tree model
decision_tree_model = model_pipeline.named_steps['classifier']

# Plot the decision tree
plt.figure(figsize=(20,10))
plot_tree(decision_tree_model, 
          filled=True, 
          rounded=True,
          class_names=['Not Transported', 'Transported'],
          feature_names=column_order)  # Ensure 'column_order' matches the order of features in the trained model
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Evaluation on Kaggle

# COMMAND ----------

#test_spark = spark.table(f"{catalog}.{schema}.titanic_test")
#test = test_spark.toPandas()
#test.set_index("PassengerId", inplace=True)

#test

# COMMAND ----------

#X_test = test

#y_pred = model_pipeline.predict(X_test)

#kaggle_submission = pd.DataFrame(y_pred, columns=['Transported'], index=X_test.index)
#kaggle_submission

# COMMAND ----------

# Writing the submission DataFrame to a CSV file
#kaggle_submission.to_csv("Data/simple_decision_tree.csv", index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Test du model

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd

# 1. Force Unity Catalog Registry
mlflow.set_registry_uri("databricks-uc")

# 2. Load model with "champion" alias
model_uri = f"models:/{model_name}@champion"
model = mlflow.sklearn.load_model(model_uri)

# 3. Load data from Delta table
test = (
    spark.table(f"{catalog}.{schema}.titanic_test")
         .toPandas()
         .set_index("PassengerId")
)

# 4. Select the columns to use as features
feature_cols = [c for c in test.columns if c != "Transported"]
X_test = test[feature_cols]

# 5. Prediction
y_pred = model.predict(X_test)

# 6. Show result
result = pd.DataFrame({
    "PassengerId": X_test.index,
    "predicted_Transported": y_pred
}).set_index("PassengerId")

print(result.head(10))