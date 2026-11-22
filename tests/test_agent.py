"""Agent tests with a fake LLM and a fake workspace client: no network."""

import json
import os
from types import SimpleNamespace

import pytest

pytest.importorskip("mlflow")
os.environ.setdefault("CRYPTO_AGENT_SKIP_AUTOLOG", "1")

from mlflow.types.responses import ResponsesAgentRequest  # noqa: E402

from ai_assistant.agent.agent import (  # noqa: E402
    SQL_TOOLS,
    TOOL_SPECS,
    CryptoAssistant,
    ToolError,
    build_sql_call,
    normalize_symbol,
    responses_to_chat,
    truncate,
)

CONFIG = {
    "llm_endpoint": "fake-llm",
    "catalog": "cat",
    "ai_schema": "ai",
    "vector_index": "cat.ai.kb_documents_index",
    "warehouse_id": "wh",
    "num_results": 3,
    "max_iterations": 4,
}


# ------------------------------------------------------------ fakes


def _col(name):
    return SimpleNamespace(name=name)


class FakeStatements:
    """Answers SQL by function name; records every call."""

    def __init__(self, tables):
        self.tables = tables
        self.calls = []

    def execute_statement(self, statement, warehouse_id, parameters, **_):
        self.calls.append((statement, [(p.name, p.value, p.type) for p in parameters]))
        fn = statement.split(".")[-1].split("(")[0]
        columns, rows = self.tables[fn]
        from databricks.sdk.service.sql import StatementState

        return SimpleNamespace(
            status=SimpleNamespace(state=StatementState.SUCCEEDED, error=None),
            manifest=SimpleNamespace(schema=SimpleNamespace(columns=[_col(c) for c in columns])),
            result=SimpleNamespace(data_array=rows),
        )


class FakeIndexes:
    def __init__(self):
        self.calls = []

    def query_index(self, index_name, columns, query_text, num_results, filters_json):
        self.calls.append(json.loads(filters_json))
        cols = columns + ["score"]
        rows = [["d1", "project_doc", "*", "README: Quarantine", "Invalid ticks go to quarantine", "README.md", 0.9]]
        return SimpleNamespace(manifest=SimpleNamespace(columns=[_col(c) for c in cols]),
                               result=SimpleNamespace(data_array=rows))


def fake_client(visible=("BTC",)):
    tables = {
        "get_visible_symbols": (["symbol"], [[s] for s in visible]),
        "get_latest_prices": (["symbol", "price_usd"], [["BTC", "67000.5"]]),
        "get_daily_summary": (["full_date", "day_close"], []),
    }
    return SimpleNamespace(statement_execution=FakeStatements(tables), vector_search_indexes=FakeIndexes())


def _tool_call(call_id, name, args):
    return SimpleNamespace(id=call_id, function=SimpleNamespace(name=name, arguments=json.dumps(args)))


class FakeLLM:
    """Replays scripted assistant messages and records what it was sent."""

    def __init__(self, script):
        self.script = list(script)
        self.requests = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(json.loads(json.dumps(kwargs["messages"], default=str)))
        content, calls = self.script.pop(0)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content, tool_calls=calls))])


def make_agent(script, client=None):
    client = client or fake_client()
    return CryptoAssistant(config=CONFIG, system_client=client, user_client_factory=lambda: client, llm=FakeLLM(script)), client


def ask(agent, text="hi"):
    return agent.predict(ResponsesAgentRequest(input=[{"role": "user", "content": text}]))


# ------------------------------------------------------- pure helpers


def test_every_sql_tool_has_a_spec():
    spec_names = {t["function"]["name"] for t in TOOL_SPECS}
    assert set(SQL_TOOLS) | {"search_knowledge_base"} == spec_names


@pytest.mark.parametrize("raw, expected", [("btc", "BTC"), (" Eth ", "ETH"), ("SOL", "SOL")])
def test_normalize_symbol(raw, expected):
    assert normalize_symbol(raw) == expected


@pytest.mark.parametrize("raw", ["", None, "BTC; DROP TABLE x", "bitcoin-usd", "A" * 11])
def test_normalize_symbol_rejects_injection_and_garbage(raw):
    with pytest.raises(ToolError):
        normalize_symbol(raw)


def test_build_sql_call_binds_parameters_and_clamps():
    stmt, params = build_sql_call("cat", "ai", "get_daily_summary", {"symbol": "eth", "days_back": 1000})
    assert stmt == "SELECT * FROM cat.ai.get_daily_summary(:symbol, :days_back)"
    assert params == [
        {"name": "symbol", "value": "ETH", "type": "STRING"},
        {"name": "days_back", "value": "90", "type": "INT"},
    ]


def test_build_sql_call_uses_defaults():
    _, params = build_sql_call("c", "s", "get_largest_hourly_moves", {"symbol": "BTC"})
    assert params[1]["value"] == "5"
    stmt, params = build_sql_call("c", "s", "get_latest_prices", {})
    assert stmt.endswith("get_latest_prices()") and params == []


@pytest.mark.parametrize("args", [{}, {"symbol": "BTC", "days_back": "abc"}])
def test_build_sql_call_reports_bad_arguments(args):
    with pytest.raises(ToolError):
        build_sql_call("c", "s", "get_daily_summary", args)


def test_truncate():
    assert truncate("abc", 5) == "abc"
    assert truncate("x" * 10, 4).startswith("xxxx... [truncated, 10 chars]")


def test_responses_to_chat_rebuilds_tool_history():
    items = [
        {"role": "user", "content": "price?"},
        {"type": "function_call", "call_id": "c1", "name": "get_latest_prices", "arguments": "{}"},
        {"type": "function_call", "call_id": "c2", "name": "search_knowledge_base", "arguments": "{\"query\": \"x\"}"},
        {"type": "function_call_output", "call_id": "c1", "output": "[]"},
        {"type": "function_call_output", "call_id": "c2", "output": "[]"},
        {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "BTC is 1"}]},
        {"role": "developer", "content": "be brief"},
    ]
    msgs = responses_to_chat(items)
    assert [m["role"] for m in msgs] == ["user", "assistant", "tool", "tool", "assistant", "system"]
    assert [c["id"] for c in msgs[1]["tool_calls"]] == ["c1", "c2"]
    assert msgs[4]["content"] == "BTC is 1"


# ---------------------------------------------------------- the loop


def test_plain_answer_without_tools():
    agent, _ = make_agent([("Hello!", None)])
    out = ask(agent).output
    assert len(out) == 1
    assert out[0].content[0]["text"] == "Hello!"
    assert agent.llm.requests[0][0]["role"] == "system"


def test_tool_call_then_answer():
    agent, client = make_agent([
        (None, [_tool_call("c1", "get_latest_prices", {})]),
        ("BTC is 67,000.50 USD", None),
    ])
    out = ask(agent, "latest BTC price?").output
    assert [o.type for o in out] == ["function_call", "function_call_output", "message"]
    assert "67000.5" in out[1].output
    # the tool result was sent back to the LLM
    assert agent.llm.requests[1][-1] == {"role": "tool", "tool_call_id": "c1", "content": out[1].output}
    assert client.statement_execution.calls[0][0] == "SELECT * FROM cat.ai.get_latest_prices()"


def test_empty_result_says_no_data():
    agent, _ = make_agent([
        (None, [_tool_call("c1", "get_daily_summary", {"symbol": "ETH"})]),
        ("No data.", None),
    ])
    out = ask(agent).output
    assert json.loads(out[1].output)["rows"] == []


def test_bad_tool_arguments_go_back_to_llm_not_crash():
    agent, client = make_agent([
        (None, [_tool_call("c1", "get_daily_summary", {"symbol": "BTC'; --"})]),
        ("Sorry.", None),
    ])
    out = ask(agent).output
    assert "invalid symbol" in json.loads(out[1].output)["error"]
    assert client.statement_execution.calls == []  # nothing reached the warehouse


def test_unknown_tool_is_rejected():
    agent, _ = make_agent([(None, [_tool_call("c1", "drop_tables", {})]), ("ok", None)])
    assert "unknown tool" in json.loads(ask(agent).output[1].output)["error"]


def test_retrieval_is_filtered_to_visible_symbols():
    client = fake_client(visible=("BTC",))
    agent, _ = make_agent([
        (None, [_tool_call("c1", "search_knowledge_base", {"query": "ETH yesterday", "symbol": "ETH"})]),
        (None, [_tool_call("c2", "search_knowledge_base", {"query": "what is quarantine"})]),
        ("done", None),
    ], client)
    out = ask(agent).output
    # ETH is not visible to this user: only project-wide documents may come back
    assert client.vector_search_indexes.calls == [{"symbol": ["*"]}, {"symbol": ["BTC", "*"]}]
    docs = json.loads(out[1].output)
    assert docs[0]["page_content"] == "Invalid ticks go to quarantine"
    assert docs[0]["metadata"]["doc_uri"] == "README.md"


def test_loop_stops_after_max_iterations():
    looping = [(None, [_tool_call(f"c{i}", "get_latest_prices", {})]) for i in range(CONFIG["max_iterations"])]
    agent, _ = make_agent(looping)
    out = ask(agent).output
    assert out[-1].type == "message"
    assert "could not finish" in out[-1].content[0]["text"]
