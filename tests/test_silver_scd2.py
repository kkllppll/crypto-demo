
from batch_pipeline.scd2_transformations import classify_scd2_records


def test_scd2_classifies_new_changed_and_unchanged(spark):
    source_df = spark.createDataFrame(
        [
            (1, "hash_new"),
            (2, "hash_updated"),
            (3, "hash_same"),
        ],
        ["entry_id", "_hash"]
    )

    target_df = spark.createDataFrame(
        [
            (2, "hash_old", True),
            (3, "hash_same", True),
        ],
        ["entry_id", "_hash", "is_current"]
    )

    result = classify_scd2_records(source_df, target_df)

    actual = {row["entry_id"]: row["scd2_action"] for row in result.collect()
    }

    assert actual == {
        1: "NEW",
        2: "CHANGED",
        3: "UNCHANGED"
    }
