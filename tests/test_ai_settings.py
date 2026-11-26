from pathlib import Path
import yaml
from ai_assistant.utilities.settings import WIDGETS, resolve

CONFIG = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "config.yml").read_text(encoding="utf-8"))

def test_defaults_come_from_config_yml():
    s = resolve(CONFIG, {})
    assert s.catalog == dbutils.widgets.get("catalog")
    assert s.gold == f"{dbutils.widgets.get("catalog")}.{CONFIG['gold']['schema']}"
    assert s.kb_table == f"{dbutils.widgets.get("catalog")}.{CONFIG['ai']['schema']}.kb_documents"
    assert s.vector_index.endswith(".kb_documents_index")
    assert s.use_obo is True
    assert s.deploy is True
    assert s.eval_thresholds["safety"] == 1.0

def test_job_parameters_override_config_and_empty_means_default():
    s = resolve(CONFIG, {"catalog": "prod_cat", "ai_schema": "dev_me_ai", "llm_endpoint": "",
                         "use_obo": "false", "deploy": "false",
                         "eval_thresholds": '{"safety": 0.5}'})
    assert s.catalog == "prod_cat"
    assert s.ai == "prod_cat.dev_me_ai"
    assert s.llm_endpoint == CONFIG["ai"]["llm_endpoint"]
    assert s.use_obo is False
    assert s.deploy is False
    assert s.eval_thresholds == {"safety": 0.5}

def test_agent_config_points_at_target_objects():
    cfg = resolve(CONFIG, {"catalog": "c", "ai_schema": "a", "warehouse_id": "w1"}).agent_config()
    assert cfg["vector_index"] == "c.a.kb_documents_index"
    assert cfg["warehouse_id"] == "w1"
    assert cfg["catalog"] == "c" and cfg["ai_schema"] == "a"

def test_every_job_parameter_in_bundle_is_a_known_widget():
    bundle = yaml.safe_load((Path(__file__).resolve().parents[1] / "resources" / "ai_assistant.yml").read_text(encoding="utf-8"))
    params = [p["name"] for p in bundle["resources"]["jobs"]["crypto_ai_assistant"]["parameters"]]
    unknown = set(params) - set(WIDGETS)
    assert not unknown, f"job parameters without a widget: {unknown}"