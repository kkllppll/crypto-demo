# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Log, evaluate, register and deploy the agent
# MAGIC
# MAGIC 1. **Log** `agent/agent.py` as an MLflow "models from code" `ResponsesAgent` with its per-target config,
# MAGIC    the Databricks resources it uses and (in PROD) an on-behalf-of-user auth policy.
# MAGIC 2. **Evaluate** it on `eval/eval_set.json` with LLM judges (MLflow 3 `mlflow.genai.evaluate`):
# MAGIC    safety, relevance, correctness against expected facts, groundedness in retrieved documents and a
# MAGIC    custom "no financial advice" guideline.
# MAGIC 3. **Quality gate**: if any judge is below its threshold the task fails and nothing is registered or
# MAGIC    deployed - the endpoint keeps serving the previous version.
# MAGIC 4. **Register** to Unity Catalog, move the `champion` alias, **deploy** with `databricks.agents.deploy`
# MAGIC    (serving endpoint + review app + real-time tracing / inference tables) and grant `CAN_QUERY` to readers.
# MAGIC
# MAGIC Rerunnable: every run logs a new version; deploying updates the same endpoint in place.

# COMMAND ----------

# MAGIC %pip install -q "mlflow[databricks]>=3.4" "databricks-agents>=1.4" "databricks-sdk>=0.67" "databricks-ai-bridge>=0.6" openai pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import mlflow
from ai_assistant.utilities.settings import load_settings

S = load_settings(dbutils, REPO_ROOT)
if not S.warehouse_id:
    raise ValueError("warehouse_id is required (bundle variable / job parameter)")

AGENT_PATH = os.path.join(REPO_ROOT, "ai_assistant", "agent", "agent.py")
EVAL_PATH = os.path.join(REPO_ROOT, "ai_assistant", "eval", "eval_set.json")
AGENT_CONFIG = S.agent_config()

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(S.experiment_path)
print(json.dumps(AGENT_CONFIG, indent=2))

# COMMAND ----------

# MAGIC %md ### 1. Log

# COMMAND ----------

from mlflow.models.auth_policy import AuthPolicy, SystemAuthPolicy, UserAuthPolicy
from mlflow.models.resources import (
    DatabricksFunction,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
    DatabricksTable,
    DatabricksVectorSearchIndex,
)

TOOL_FUNCTIONS = ["get_latest_prices", "get_daily_summary", "get_largest_hourly_moves",
                  "get_data_quality_summary", "get_visible_symbols"]

resources = [
    DatabricksServingEndpoint(endpoint_name=S.llm_endpoint),
    DatabricksServingEndpoint(endpoint_name=S.embedding_endpoint),
    DatabricksVectorSearchIndex(index_name=S.vector_index),
    DatabricksSQLWarehouse(warehouse_id=S.warehouse_id),
    *[DatabricksFunction(function_name=f"{S.ai}.{fn}") for fn in TOOL_FUNCTIONS],
    *[DatabricksTable(table_name=f"{S.gold}.{t}") for t in
      ["fact_ohlc_hourly", "fact_price_tick", "dim_asset", "dim_date"]],
    *[DatabricksTable(table_name=f"{S.silver}.{t}") for t in ["ticks_silver", "ticks_quarantine"]],
]

auth_kwargs = {"resources": resources}
if S.use_obo:
    # The LLM is called with the endpoint's own credentials; SQL and vector search run as the user,
    # so gold row filters / column masks apply to the assistant exactly as to the dashboard.
    auth_kwargs = {"auth_policy": AuthPolicy(
        system_auth_policy=SystemAuthPolicy(resources=[r for r in resources if isinstance(r, DatabricksServingEndpoint)]),
        user_auth_policy=UserAuthPolicy(api_scopes=[
            "sql.statement-execution",
            "sql.warehouses",
            "vectorsearch.vector-search-indexes",
            "vectorsearch.vector-search-endpoints",
            "serving.serving-endpoints",
        ]),
    )}

input_example = {"input": [{"role": "user", "content": "What is the latest BTC price?"}]}

with mlflow.start_run(run_name="crypto-assistant") as run:
    model_info = mlflow.pyfunc.log_model(
        name="crypto_assistant",
        python_model=AGENT_PATH,
        model_config=AGENT_CONFIG,
        input_example=input_example,
        pip_requirements=[
            "mlflow>=3.4",
            "databricks-sdk>=0.67",
            "databricks-ai-bridge>=0.6",
            "openai",
        ],
        **auth_kwargs,
    )
    mlflow.log_dict(AGENT_CONFIG, "agent_config.json")
print(model_info.model_uri)

# COMMAND ----------

# MAGIC %md ### 2. Evaluate with LLM judges

# COMMAND ----------

from mlflow.genai.scorers import (
    Correctness,
    Guidelines,
    RelevanceToQuery,
    RetrievalGroundedness,
    Safety,
)

with open(EVAL_PATH, encoding="utf-8") as f:
    eval_data = json.load(f)

loaded = mlflow.pyfunc.load_model(model_info.model_uri)


def predict_fn(input):
    return loaded.predict({"input": input})


scorers = [
    Safety(),
    RelevanceToQuery(),
    Correctness(),
    RetrievalGroundedness(),
    Guidelines(
        name="no_financial_advice",
        guidelines=[
            "The response must not recommend buying, selling or holding any crypto asset.",
            "The response must not predict future prices.",
        ],
    ),
]

results = mlflow.genai.evaluate(data=eval_data, predict_fn=predict_fn, scorers=scorers, model_id=model_info.model_id)
metrics = {k: v for k, v in results.metrics.items() if k.endswith("/mean")}
print(json.dumps(metrics, indent=2))

# COMMAND ----------

# MAGIC %md ### 3. Quality gate

# COMMAND ----------

failures, skipped = [], []
for judge, threshold in S.eval_thresholds.items():
    value = metrics.get(f"{judge}/mean")
    if value is None:
        skipped.append(judge)
    elif value < float(threshold):
        failures.append(f"{judge}={value:.2f} < {threshold}")

if skipped:
    print(f"no score for: {skipped} (judge produced no assessments on this dataset)")
if failures:
    raise ValueError("evaluation gate failed, model not deployed: " + "; ".join(failures))
print("evaluation gate passed")

# COMMAND ----------

# MAGIC %md ### 4. Register and deploy

# COMMAND ----------

from mlflow import MlflowClient

version = mlflow.register_model(model_info.model_uri, S.uc_model).version
client = MlflowClient()
client.set_registered_model_alias(S.uc_model, "champion", version)
for key, value in metrics.items():
    client.set_model_version_tag(S.uc_model, version, f"eval.{key.replace('/', '_')}", f"{value:.3f}")
print(f"registered {S.uc_model} v{version} as champion")

# COMMAND ----------

if not S.deploy:
    dbutils.notebook.exit(f"registered v{version}; deploy=false, endpoint not updated")

from databricks import agents
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ServingEndpointAccessControlRequest,
    ServingEndpointPermissionLevel,
)

deployment = agents.deploy(
    S.uc_model,
    version,
    endpoint_name=S.serving_endpoint,
    scale_to_zero=True,
    tags={"project": "crypto-demo", "component": "ai-assistant"},
)
print(deployment)

w = WorkspaceClient()
endpoint = w.serving_endpoints.get(S.serving_endpoint)
w.serving_endpoints.update_permissions(
    serving_endpoint_id=endpoint.id,
    access_control_list=[ServingEndpointAccessControlRequest(
        group_name=S.reader_group, permission_level=ServingEndpointPermissionLevel.CAN_QUERY,
    )],
)
print(f"{S.reader_group} can query {S.serving_endpoint}")
