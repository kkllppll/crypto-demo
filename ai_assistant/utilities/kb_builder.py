"""Knowledge base for the crypto assistant: documents for the vector index.

Three kinds of documents end up in `<ai_schema>.kb_documents`:

- `market_daily`  - one short narrative per asset per day, built from gold
                    `fact_ohlc_hourly` (open / close / range / largest hourly move)
- `project_doc`   - README sections, split by markdown headings
- `data_dictionary` - one document per gold / silver table with column comments

Everything here is a pure function (Python or PySpark DataFrame in, DataFrame
or list out) so it can be unit tested without a workspace.

Every document carries a `symbol` column. Market narratives have a real symbol,
other documents have `ALL_SYMBOLS`. The agent filters retrieval by the symbols
the current user can see in gold (row filter), so a user restricted to BTC never
gets ETH narratives back from the index.
"""

import hashlib
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ALL_SYMBOLS = "*"

KB_COLUMNS = ["doc_id", "source_type", "symbol", "title", "content", "source_ref"]

MAX_CHUNK_CHARS = 1500
CHUNK_OVERLAP_CHARS = 200


def doc_id(*parts) -> str:
    """Stable id from the parts that identify a document (not its content).

    The same README section or the same asset/day always gets the same id, so
    MERGE updates it in place instead of adding a duplicate.
    """
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------- chunking


_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")


def split_markdown_sections(text: str) -> list:
    """Splits markdown into (heading_path, body) by headings.

    `heading_path` joins parent headings with " > " so a chunk keeps its
    context ("crypto-demo > Gold layer > Gold verification"). Headings inside
    fenced code blocks (``` ... ```) are ignored. Empty sections are dropped.
    """
    sections = []
    stack = []  # [(level, title)]
    body = []
    in_code = False

    def flush():
        content = "\n".join(body).strip()
        if content:
            path = " > ".join(t for _, t in stack) or "Introduction"
            sections.append((path, content))

    for line in text.splitlines():
        if line.strip().startswith("```"):
            in_code = not in_code
            body.append(line)
            continue
        match = None if in_code else _HEADING.match(line)
        if match:
            flush()
            body = []
            level = len(match.group(1))
            while stack and stack[-1][0] >= level:
                stack.pop()
            stack.append((level, match.group(2)))
        else:
            body.append(line)
    flush()
    return sections


def chunk_text(text: str, max_chars: int = MAX_CHUNK_CHARS, overlap: int = CHUNK_OVERLAP_CHARS) -> list:
    """Splits text into chunks of at most `max_chars`, preferring paragraph breaks.

    Paragraphs are packed greedily; a paragraph longer than `max_chars` is cut
    into windows with `overlap` characters repeated between neighbours.
    """
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")
    if not 0 <= overlap < max_chars:
        raise ValueError("overlap must be in [0, max_chars)")

    text = text.strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    chunks = []
    current = ""
    for para in re.split(r"\n\s*\n", text):
        para = para.strip()
        if not para:
            continue
        candidate = f"{current}\n\n{para}" if current else para
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(para) <= max_chars:
            current = para
            continue
        step = max_chars - overlap
        for start in range(0, len(para), step):
            piece = para[start:start + max_chars]
            chunks.append(piece)
            if start + max_chars >= len(para):
                break
    if current:
        chunks.append(current)
    return chunks


def markdown_documents(text: str, source_ref: str, doc_title: str) -> list:
    """README / docs markdown -> list of KB rows (dicts with KB_COLUMNS)."""
    rows = []
    for path, body in split_markdown_sections(text):
        pieces = chunk_text(body)
        for i, piece in enumerate(pieces):
            suffix = f" (part {i + 1}/{len(pieces)})" if len(pieces) > 1 else ""
            title = f"{doc_title}: {path}{suffix}"
            rows.append({
                "doc_id": doc_id("project_doc", source_ref, path, i),
                "source_type": "project_doc",
                "symbol": ALL_SYMBOLS,
                "title": title,
                "content": f"{title}\n\n{piece}",
                "source_ref": source_ref,
            })
    return rows


# --------------------------------------------------------- data dictionary


def data_dictionary_documents(tables: list, columns: list) -> list:
    """information_schema rows -> one KB row per table.

    `tables`: dicts with table_schema, table_name, comment.
    `columns`: dicts with table_schema, table_name, column_name, data_type,
    comment, ordinal_position.
    """
    by_table = {}
    for col in columns:
        by_table.setdefault((col["table_schema"], col["table_name"]), []).append(col)

    rows = []
    for table in sorted(tables, key=lambda t: (t["table_schema"], t["table_name"])):
        key = (table["table_schema"], table["table_name"])
        cols = sorted(by_table.get(key, []), key=lambda c: c.get("ordinal_position") or 0)
        full_name = f"{key[0]}.{key[1]}"
        lines = [f"Table {full_name}"]
        if table.get("comment"):
            lines.append(f"Description: {table['comment']}")
        if cols:
            lines.append("Columns:")
            for c in cols:
                comment = f" - {c['comment']}" if c.get("comment") else ""
                lines.append(f"- {c['column_name']} ({c['data_type']}){comment}")
        rows.append({
            "doc_id": doc_id("data_dictionary", full_name),
            "source_type": "data_dictionary",
            "symbol": ALL_SYMBOLS,
            "title": f"Data dictionary: {full_name}",
            "content": "\n".join(lines),
            "source_ref": full_name,
        })
    return rows


# --------------------------------------------------------- market narratives


def daily_market_stats(fact_ohlc: DataFrame, dim_asset: DataFrame) -> DataFrame:
    """fact_ohlc_hourly + dim_asset -> one row per symbol per day.

    Volume is deliberately left out: it is masked by CLS for some users and
    text in the index cannot be masked per user.
    """
    hourly = (
        fact_ohlc.join(dim_asset.select("asset_key", "symbol", "asset_name"), "asset_key")
        .withColumn("trade_date", F.to_date("event_time"))
    )

    # Largest absolute hourly move of the day, with its hour (earliest on ties).
    by_move = Window.partitionBy("symbol", "trade_date").orderBy(
        F.abs(F.col("price_change_pct")).desc_nulls_last(), F.col("event_time")
    )
    biggest = (
        hourly.withColumn("_rn", F.row_number().over(by_move))
        .where("_rn = 1")
        .select(
            "symbol",
            "trade_date",
            F.col("price_change_pct").alias("max_move_pct"),
            F.col("hour_of_day").alias("max_move_hour"),
        )
    )

    stats = (
        hourly.groupBy("symbol", "asset_name", "trade_date")
        .agg(
            F.min_by("open", "event_time").alias("day_open"),
            F.max_by("close", "event_time").alias("day_close"),
            F.min("low").alias("day_low"),
            F.max("high").alias("day_high"),
            F.stddev("price_change_pct").alias("volatility_pct"),
            F.count("*").alias("hours"),
        )
        .withColumn(
            "daily_change_pct",
            F.when(F.col("day_open") > 0, (F.col("day_close") - F.col("day_open")) / F.col("day_open") * 100),
        )
    )
    return stats.join(biggest, ["symbol", "trade_date"], "left")


def _fmt_price(col):
    return F.format_number(col, 2)


def _fmt_pct(col):
    return F.concat(F.when(col >= 0, F.lit("+")).otherwise(F.lit("")), F.format_number(col, 2), F.lit("%"))


def market_daily_documents(stats: DataFrame) -> DataFrame:
    """daily_market_stats -> KB rows (DataFrame with KB_COLUMNS).

    Example content:
    "BTC (Bitcoin) on 2026-10-27, Tuesday: opened at 67,100.00 USD and closed
    at 68,020.50 USD (+1.37%). Intraday range 66,800.00 - 68,400.00 USD.
    Hourly volatility (stddev of hourly change) 0.42%. Largest hourly move
    +0.95% at 14:00 UTC. Based on 24 hourly candles from Kraken."
    """
    date_str = F.date_format("trade_date", "yyyy-MM-dd")
    content = F.concat(
        F.col("symbol"), F.lit(" ("), F.coalesce(F.col("asset_name"), F.lit("unknown")), F.lit(") on "),
        date_str, F.lit(", "), F.date_format("trade_date", "EEEE"), F.lit(": opened at "),
        _fmt_price(F.col("day_open")), F.lit(" USD and closed at "), _fmt_price(F.col("day_close")),
        F.lit(" USD ("), F.coalesce(_fmt_pct(F.col("daily_change_pct")), F.lit("n/a")), F.lit("). "),
        F.lit("Intraday range "), _fmt_price(F.col("day_low")), F.lit(" - "), _fmt_price(F.col("day_high")),
        F.lit(" USD. Hourly volatility (stddev of hourly change) "),
        F.coalesce(F.concat(F.format_number("volatility_pct", 2), F.lit("%")), F.lit("n/a")),
        F.lit(". Largest hourly move "),
        F.coalesce(_fmt_pct(F.col("max_move_pct")), F.lit("n/a")), F.lit(" at "),
        F.coalesce(F.format_string("%02d:00", F.col("max_move_hour")), F.lit("n/a")),
        F.lit(" UTC. Based on "), F.col("hours").cast("string"), F.lit(" hourly candles from Kraken."),
    )
    id_col = F.substring(F.sha2(F.concat_ws("|", F.lit("market_daily"), F.col("symbol"), date_str), 256), 1, 32)
    return stats.select(
        id_col.alias("doc_id"),
        F.lit("market_daily").alias("source_type"),
        F.col("symbol"),
        F.concat(F.col("symbol"), F.lit(" market summary "), date_str).alias("title"),
        content.alias("content"),
        F.concat(F.lit("fact_ohlc_hourly/"), F.col("symbol"), F.lit("/"), date_str).alias("source_ref"),
    )
