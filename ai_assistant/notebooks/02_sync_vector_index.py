# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Vector Search index
# MAGIC
# MAGIC Creates (once) a Vector Search endpoint and a **Delta Sync** index over `kb_documents` with
# MAGIC managed embeddings (`databricks-gte-large-en`), then triggers a sync and waits until it is ready.
# MAGIC
# MAGIC - `TRIGGERED` pipeline: the index is refreshed by this job, not continuously - cheaper, and the
# MAGIC   knowledge base only changes when gold is reloaded.
# MAGIC - Delta Sync reads the Change Data Feed, so only changed documents are re-embedded.
# MAGIC - Rerunnable: existing endpoint / index are reused; a sync with no changes is a no-op.

# COMMAND ----------

import os
import sys
import time
from datetime import timedelta

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceConflict, BadRequest
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

from ai_assistant.utilities.settings import load_settings

S = load_settings(dbutils, REPO_ROOT)
w = WorkspaceClient()
print(f"endpoint={S.vector_search_endpoint} index={S.vector_index} source={S.kb_table}")

# COMMAND ----------

# MAGIC %md ### Endpoint

# COMMAND ----------

try:
    ep = w.vector_search_endpoints.get_endpoint(S.vector_search_endpoint)
    print(f"endpoint exists: {ep.endpoint_status.state if ep.endpoint_status else 'unknown'}")
except NotFound:
    print("creating endpoint (takes several minutes the first time)")
    w.vector_search_endpoints.create_endpoint_and_wait(
        name=S.vector_search_endpoint,
        endpoint_type=EndpointType.STANDARD,
        timeout=timedelta(minutes=40),
    )

# COMMAND ----------

# MAGIC %md ### Index

# COMMAND ----------

try:
    w.vector_search_indexes.get_index(S.vector_index)
    try:
        w.vector_search_indexes.sync_index(S.vector_index)
        print("sync triggered")
    except (ResourceConflict, BadRequest) as exc:  # a sync is already running / index still provisioning
        print(f"sync not triggered: {exc}")
except NotFound:
    print("creating index")
    w.vector_search_indexes.create_index(
        name=S.vector_index,
        endpoint_name=S.vector_search_endpoint,
        primary_key="doc_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=S.kb_table,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(name="content", embedding_model_endpoint_name=S.embedding_endpoint)
            ],
            columns_to_sync=["doc_id", "source_type", "symbol", "title", "content", "source_ref"],
        ),
    )

# COMMAND ----------

# MAGIC %md ### Wait until ready

# COMMAND ----------

deadline = time.time() + 40 * 60
while True:
    status = w.vector_search_indexes.get_index(S.vector_index).status
    print(f"ready={status.ready} indexed_rows={status.indexed_row_count} {status.message or ''}")
    if status.ready:
        break
    if time.time() > deadline:
        raise TimeoutError(f"index {S.vector_index} not ready after 40 minutes")
    time.sleep(30)

source_rows = spark.table(S.kb_table).count()
print(f"source rows: {source_rows}, indexed rows: {status.indexed_row_count}")

# Readers query the index (as themselves, through the agent); the source table stays private.
spark.sql(f"GRANT SELECT ON TABLE {S.vector_index} TO `{S.reader_group}`")

# COMMAND ----------

# MAGIC %md ### Smoke query

# COMMAND ----------

resp = w.vector_search_indexes.query_index(
    index_name=S.vector_index,
    columns=["title", "source_type", "symbol"],
    query_text="How are invalid price ticks handled?",
    num_results=3,
)
for row in resp.result.data_array or []:
    print(row)
