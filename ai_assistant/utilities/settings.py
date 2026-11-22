"""Settings for the AI assistant notebooks.

Precedence: notebook widget (= job parameter from the bundle) > config/config.yml.
The bundle passes per-target values (catalog, warehouse, endpoint names), so
the same code runs unchanged in DEV and PROD.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# widget name -> (config section, key)
WIDGETS = {
    "catalog": (None, "catalog"),
    "gold_schema": ("gold", "schema"),
    "silver_schema": ("silver", "schema"),
    "ai_schema": ("ai", "schema"),
    "reader_group": ("gold", "reader_group"),
    "vector_search_endpoint": ("ai", "vector_search_endpoint"),
    "embedding_endpoint": ("ai", "embedding_endpoint"),
    "llm_endpoint": ("ai", "llm_endpoint"),
    "model_name": ("ai", "model_name"),
    "serving_endpoint": ("ai", "serving_endpoint"),
    "warehouse_id": ("ai", "warehouse_id"),
    "use_obo": ("ai", "use_obo"),
    "experiment_path": ("ai", "experiment_path"),
    "eval_thresholds": ("ai", "eval_thresholds"),
    "deploy": (None, None),  # job-only switch, default "true"
}


@dataclass
class Settings:
    catalog: str
    gold_schema: str
    silver_schema: str
    ai_schema: str
    reader_group: str
    vector_search_endpoint: str
    embedding_endpoint: str
    llm_endpoint: str
    model_name: str
    serving_endpoint: str
    warehouse_id: str
    use_obo: bool
    experiment_path: str
    eval_thresholds: dict = field(default_factory=dict)
    deploy: bool = True
    kb_table_name: str = "kb_documents"
    vector_index_name: str = "kb_documents_index"

    @property
    def gold(self) -> str:
        return f"{self.catalog}.{self.gold_schema}"

    @property
    def silver(self) -> str:
        return f"{self.catalog}.{self.silver_schema}"

    @property
    def ai(self) -> str:
        return f"{self.catalog}.{self.ai_schema}"

    @property
    def kb_table(self) -> str:
        return f"{self.ai}.{self.kb_table_name}"

    @property
    def vector_index(self) -> str:
        return f"{self.ai}.{self.vector_index_name}"

    @property
    def uc_model(self) -> str:
        return f"{self.ai}.{self.model_name}"

    def agent_config(self) -> dict:
        """model_config logged with the agent (read by agent.py at serving time)."""
        return {
            "llm_endpoint": self.llm_endpoint,
            "catalog": self.catalog,
            "ai_schema": self.ai_schema,
            "vector_index": self.vector_index,
            "warehouse_id": self.warehouse_id,
            "use_obo": self.use_obo,
            "num_results": 5,
            "max_iterations": 6,
            "max_rows": 200,
            "temperature": 0.0,
            "max_tokens": 1500,
        }


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def _as_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    return json.loads(value)


def resolve(config: dict, overrides: dict) -> Settings:
    """config.yml dict + non-empty overrides (widget values) -> Settings."""
    values = {}
    for name, (section, key) in WIDGETS.items():
        default = None
        if key is not None:
            default = config.get(key) if section is None else config.get(section, {}).get(key)
        value = overrides.get(name)
        values[name] = default if value in (None, "") else value

    ai = config.get("ai", {})
    return Settings(
        catalog=values["catalog"],
        gold_schema=values["gold_schema"],
        silver_schema=values["silver_schema"],
        ai_schema=values["ai_schema"],
        reader_group=values["reader_group"],
        vector_search_endpoint=values["vector_search_endpoint"],
        embedding_endpoint=values["embedding_endpoint"],
        llm_endpoint=values["llm_endpoint"],
        model_name=values["model_name"],
        serving_endpoint=values["serving_endpoint"],
        warehouse_id=values["warehouse_id"] or "",
        use_obo=_as_bool(values["use_obo"]),
        experiment_path=values["experiment_path"],
        eval_thresholds=_as_dict(values["eval_thresholds"]),
        deploy=True if values["deploy"] in (None, "") else _as_bool(values["deploy"]),
        kb_table_name=ai.get("kb_table", "kb_documents"),
        vector_index_name=ai.get("vector_index", "kb_documents_index"),
    )


def load_settings(dbutils, repo_root) -> Settings:
    """Creates a text widget per setting (empty = use config.yml) and resolves."""
    config = yaml.safe_load((Path(repo_root) / "config" / "config.yml").read_text(encoding="utf-8"))
    overrides = {}
    for name in WIDGETS:
        dbutils.widgets.text(name, "")
        overrides[name] = dbutils.widgets.get(name)
    return resolve(config, overrides)
