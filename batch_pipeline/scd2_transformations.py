
from pyspark.sql import functions as F


def classify_scd2_records(source_df, target_df, business_key="entry_id"):
    """
    Classify incoming records against current Silver records.

    Both DataFrames must contain:
    - business key
    - _hash of the business attributes
    """

    current_target = target_df.filter(F.col("is_current") == True)

    comparison = source_df.alias("s").join(
        current_target.alias("t"),
        F.col(f"s.{business_key}") == F.col(f"t.{business_key}"),
        "left"
    )

    result = comparison.select("s.*",
        F.when(
            F.col(f"t.{business_key}").isNull(),
            F.lit("NEW")
        ).when(
            ~F.col("s._hash").eqNullSafe(F.col("t._hash")),
            F.lit("CHANGED")
        ).otherwise(
            F.lit("UNCHANGED")
        ).alias("scd2_action")
    )

    return result
