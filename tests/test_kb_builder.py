import datetime as dt

import pytest
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from ai_assistant.utilities.kb_builder import (
    ALL_SYMBOLS,
    KB_COLUMNS,
    chunk_text,
    daily_market_stats,
    data_dictionary_documents,
    doc_id,
    market_daily_documents,
    markdown_documents,
    split_markdown_sections,
)


@pytest.fixture(autouse=True)
def utc_timezone(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")


# ------------------------------------------------------------- doc ids


def test_doc_id_is_stable_and_distinct():
    assert doc_id("a", 1) == doc_id("a", 1)
    assert doc_id("a", 1) != doc_id("a", 2)
    assert len(doc_id("x")) == 32


# ------------------------------------------------------------ markdown


def test_sections_keep_heading_path_and_skip_code_headings():
    md = "# Top\nintro\n## Child\nbody\n```\n# not a heading\n```\n# Other\ntext"
    sections = split_markdown_sections(md)
    assert sections[0] == ("Top", "intro")
    assert sections[1][0] == "Top > Child"
    assert "# not a heading" in sections[1][1]
    assert sections[2] == ("Other", "text")


def test_sibling_heading_replaces_previous_level():
    sections = split_markdown_sections("# A\n## B\nb\n## C\nc")
    assert [s[0] for s in sections] == ["A > B", "A > C"]


def test_empty_sections_are_dropped():
    assert split_markdown_sections("# A\n\n# B\ntext") == [("B", "text")]


def test_text_before_first_heading_is_introduction():
    assert split_markdown_sections("hello\n# A\nx")[0] == ("Introduction", "hello")


def test_short_text_is_one_chunk():
    assert chunk_text("hello") == ["hello"]
    assert chunk_text("   ") == []


def test_paragraphs_are_packed_up_to_limit():
    text = "\n\n".join(["a" * 40] * 5)
    chunks = chunk_text(text, max_chars=100, overlap=10)
    assert all(len(c) <= 100 for c in chunks)
    assert len(chunks) == 3  # 2 + 2 + 1 paragraphs


def test_long_paragraph_is_windowed_with_overlap():
    text = "".join(str(i % 10) for i in range(250))
    chunks = chunk_text(text, max_chars=100, overlap=20)
    assert all(len(c) <= 100 for c in chunks)
    assert chunks[0][-20:] == chunks[1][:20]
    assert "".join(c if i == 0 else c[20:] for i, c in enumerate(chunks)) == text


@pytest.mark.parametrize("max_chars, overlap", [(0, 0), (10, 10), (10, -1)])
def test_chunk_text_rejects_bad_settings(max_chars, overlap):
    with pytest.raises(ValueError):
        chunk_text("x" * 50, max_chars=max_chars, overlap=overlap)


def test_markdown_documents_have_kb_shape():
    rows = markdown_documents("# Gold\nstar schema\n## RLS\nrow filter", "README.md", "README")
    assert [r["title"] for r in rows] == ["README: Gold", "README: Gold > RLS"]
    for r in rows:
        assert set(r) == set(KB_COLUMNS)
        assert r["symbol"] == ALL_SYMBOLS
        assert r["content"].startswith(r["title"])
    assert len({r["doc_id"] for r in rows}) == 2


# ------------------------------------------------------ data dictionary


def test_data_dictionary_orders_columns_and_includes_comments():
    tables = [{"table_schema": "gold", "table_name": "dim_asset", "comment": "Assets"}]
    columns = [
        {"table_schema": "gold", "table_name": "dim_asset", "column_name": "symbol",
         "data_type": "STRING", "comment": "BTC, ETH", "ordinal_position": 1},
        {"table_schema": "gold", "table_name": "dim_asset", "column_name": "asset_key",
         "data_type": "BIGINT", "comment": None, "ordinal_position": 0},
    ]
    [row] = data_dictionary_documents(tables, columns)
    assert row["title"] == "Data dictionary: gold.dim_asset"
    lines = row["content"].splitlines()
    assert lines[:3] == ["Table gold.dim_asset", "Description: Assets", "Columns:"]
    assert lines[3] == "- asset_key (BIGINT)"
    assert lines[4] == "- symbol (STRING) - BTC, ETH"


def test_table_without_columns_still_documented():
    [row] = data_dictionary_documents([{"table_schema": "s", "table_name": "t", "comment": None}], [])
    assert row["content"] == "Table s.t"


# ---------------------------------------------------- market narratives

FACT_SCHEMA = StructType([
    StructField("asset_key", LongType()),
    StructField("event_time", TimestampType()),
    StructField("hour_of_day", IntegerType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("price_change_pct", DoubleType()),
])

DIM_SCHEMA = StructType([
    StructField("asset_key", LongType()),
    StructField("symbol", StringType()),
    StructField("asset_name", StringType()),
])


def _fact(spark):
    d = dt.datetime(2026, 10, 27)
    return spark.createDataFrame(
        [
            (1, d.replace(hour=0), 0, 100.0, 110.0, 95.0, 105.0, 5.0),
            (1, d.replace(hour=1), 1, 105.0, 108.0, 90.0, 92.0, -12.38),
            (1, d.replace(hour=2), 2, 92.0, 111.0, 91.0, 110.0, 19.57),
            (1, d + dt.timedelta(days=1), 0, 110.0, 112.0, 109.0, 111.0, 0.91),
        ],
        FACT_SCHEMA,
    )


def _dim(spark):
    return spark.createDataFrame([(1, "BTC", "Bitcoin")], DIM_SCHEMA)


def test_daily_stats_open_close_range_and_biggest_move(spark):
    stats = {r["trade_date"]: r for r in daily_market_stats(_fact(spark), _dim(spark)).collect()}
    day = stats[dt.date(2026, 10, 27)]
    assert day["day_open"] == 100.0
    assert day["day_close"] == 110.0
    assert day["day_low"] == 90.0
    assert day["day_high"] == 111.0
    assert day["daily_change_pct"] == pytest.approx(10.0)
    assert day["max_move_pct"] == 19.57
    assert day["max_move_hour"] == 2
    assert day["hours"] == 3
    assert stats[dt.date(2026, 10, 28)]["hours"] == 1


def test_market_documents_text_and_ids(spark):
    docs = market_daily_documents(daily_market_stats(_fact(spark), _dim(spark)))
    assert docs.columns == KB_COLUMNS
    rows = {r["title"]: r for r in docs.collect()}
    row = rows["BTC market summary 2026-10-27"]
    assert row["symbol"] == "BTC"
    assert row["source_type"] == "market_daily"
    assert row["content"].startswith("BTC (Bitcoin) on 2026-10-27, Tuesday: opened at 100.00 USD")
    assert "(+10.00%)" in row["content"]
    assert "Largest hourly move +19.57% at 02:00 UTC" in row["content"]
    assert "volume" not in row["content"].lower()
    assert row["doc_id"] == doc_id("market_daily", "BTC", "2026-10-27")
    # single-candle day has no stddev -> n/a instead of a broken sentence
    assert "volatility (stddev of hourly change) n/a" in rows["BTC market summary 2026-10-28"]["content"]


def test_market_documents_are_deterministic(spark):
    stats = daily_market_stats(_fact(spark), _dim(spark))
    first = sorted(market_daily_documents(stats).collect())
    second = sorted(market_daily_documents(stats).collect())
    assert first == second
