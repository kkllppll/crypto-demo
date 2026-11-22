"""Crypto Assistant - Databricks App (Streamlit chat UI for the agent endpoint).

With user authorization enabled, Databricks Apps forwards the signed-in user's
token in `x-forwarded-access-token`. The app calls the agent endpoint with that
token, so the agent (on-behalf-of-user auth) queries gold as this user and the
same row filters / column masks apply as on the dashboard. Without the header
(local run, scope not granted) the app falls back to its own service principal.
"""

import base64
import json
import os
import sys

import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

ENDPOINT = os.environ.get("SERVING_ENDPOINT", "crypto-assistant-dev")

EXAMPLES = [
    "What is the latest price of every asset?",
    "How did ETH do over the last 3 days?",
    "How many ticks were quarantined recently and why?",
    "How does the pipeline deduplicate streaming ticks?",
]

TOOL_LABELS = {
    "search_knowledge_base": "Knowledge base search",
    "get_latest_prices": "Latest prices (gold)",
    "get_daily_summary": "Daily summary (gold)",
    "get_largest_hourly_moves": "Largest hourly moves (gold)",
    "get_data_quality_summary": "Data quality (silver)",
}


def workspace_client() -> tuple:
    token = st.context.headers.get("x-forwarded-access-token")
    if token:
        cfg = Config()
        return WorkspaceClient(host=cfg.host, token=token, auth_type="pat"), "your identity"
    return WorkspaceClient(), "the app service principal"


def token_scopes() -> str:
    """Scopes of the forwarded user token (JWT claim only, never the token), for error logs."""
    token = st.context.headers.get("x-forwarded-access-token") or ""
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return str(claims.get("scope") or claims.get("scp"))
    except (IndexError, ValueError):
        return "n/a"


def ask_agent(client: WorkspaceClient, history: list) -> dict:
    return client.api_client.do(
        "POST",
        f"/serving-endpoints/{ENDPOINT}/invocations",
        body={"input": history},
    )


def parse_output(response: dict) -> tuple:
    """-> (answer text, [(tool name, arguments, output)])"""
    answer, calls, outputs = [], {}, {}
    for item in response.get("output", []):
        kind = item.get("type")
        if kind == "message":
            answer += [c.get("text", "") for c in item.get("content", []) if c.get("type") == "output_text"]
        elif kind == "function_call":
            calls[item["call_id"]] = (item["name"], item.get("arguments", "{}"))
        elif kind == "function_call_output":
            outputs[item["call_id"]] = item.get("output", "")
    steps = [(name, args, outputs.get(cid, "")) for cid, (name, args) in calls.items()]
    return "\n\n".join(a for a in answer if a), steps


def render_steps(steps: list) -> None:
    if not steps:
        return
    with st.expander(f"How I got this ({len(steps)} tool call{'s' if len(steps) > 1 else ''})"):
        for name, args, output in steps:
            st.markdown(f"**{TOOL_LABELS.get(name, name)}** `{args}`")
            try:
                parsed = json.loads(output)
            except (TypeError, ValueError):
                parsed = output
            if isinstance(parsed, list) and parsed and "page_content" in parsed[0]:
                for doc in parsed:
                    meta = doc.get("metadata", {})
                    st.caption(f"{meta.get('title')} - {meta.get('doc_uri')}")
            else:
                st.json(parsed, expanded=False)


st.set_page_config(page_title="Crypto Assistant", page_icon=":bar_chart:", layout="centered")
st.title("Crypto Assistant")
st.caption(
    "Answers from the gold layer and project documentation. Numbers come from governed Unity Catalog "
    "functions; you only see the assets your access rules allow. Not financial advice."
)

client, identity = workspace_client()

if "history" not in st.session_state:
    st.session_state.history = []  # Responses API input items sent to the agent
    st.session_state.chat = []     # (role, text, steps) shown in the UI

with st.sidebar:
    st.subheader("Try asking")
    for example in EXAMPLES:
        if st.button(example, use_container_width=True):
            st.session_state.pending = example
    st.divider()
    st.caption(f"Endpoint: `{ENDPOINT}`")
    st.caption(f"Queries run as {identity}.")
    if st.button("New conversation", use_container_width=True):
        st.session_state.history, st.session_state.chat = [], []
        st.rerun()

for role, text, steps in st.session_state.chat:
    with st.chat_message(role):
        st.markdown(text)
        render_steps(steps)

prompt = st.chat_input("Ask about prices, data quality or how the platform works")
prompt = prompt or st.session_state.pop("pending", None)

if prompt:
    st.session_state.chat.append(("user", prompt, []))
    with st.chat_message("user"):
        st.markdown(prompt)

    st.session_state.history.append({"role": "user", "content": prompt})
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                response = ask_agent(client, st.session_state.history)
                answer, steps = parse_output(response)
                # keep tool calls in history so follow-up questions have context
                st.session_state.history.extend(response.get("output", []))
            except Exception as exc:  # shown to the user instead of a stack trace
                print(f"agent call failed: {type(exc).__name__}: {exc}; token scopes: {token_scopes()}",
                      file=sys.stderr, flush=True)
                answer, steps = f"Sorry, the assistant is unavailable right now ({type(exc).__name__}).", []
                st.session_state.history.pop()
        st.markdown(answer or "_No answer._")
        render_steps(steps)
    st.session_state.chat.append(("assistant", answer, steps))
