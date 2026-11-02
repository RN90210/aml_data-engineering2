"""Opt-in Spark regression: cached lookup sides must not be sorted per join."""

import os

import pytest

from streaming.ingestion.spark_streaming_pipeline import enrich_transaction_entities, load_entity_lookup


@pytest.mark.skipif(os.environ.get("MULEDETECTOR_SPARK_PLAN_TEST") != "1",
                    reason="opt-in Spark/Java integration check")
def test_cached_lookup_reuses_partitioning_and_ordering(tmp_path):
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master("local[2]").appName("LookupPlanRegression")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.adaptive.enabled", "true").getOrCreate())
    lookup = None
    try:
        path = tmp_path / "lookup.parquet"
        spark.createDataFrame([("a", "entity-a"), ("b", "entity-b")],
                              "account_id string, entity_id string").write.parquet(str(path))
        lookup = load_entity_lookup(spark, path)
        # Streaming disables AQE. The already-materialized cache must still
        # advertise the exact partition count and ordering to those joins.
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        transactions = spark.createDataFrame([("a", "b")],
                                             "sender_account_id string, receiver_account_id string")
        enriched = enrich_transaction_entities(transactions, lookup)

        def nodes(plan):
            yield plan
            for index in range(plan.children().size()):
                yield from nodes(plan.children().apply(index))

        plan_nodes = list(nodes(enriched._jdf.queryExecution().executedPlan()))
        assert sum(node.nodeName() == "SortMergeJoin" for node in plan_nodes) == 2
        scans = [node for node in plan_nodes if node.nodeName() == "InMemoryTableScan"]
        assert len(scans) == 2
        for scan in scans:
            assert scan.outputOrdering().size() > 0
            assert "hashpartitioning" in scan.outputPartitioning().toString().lower()
        # Sort nodes embedded in the cache's descriptive plan text ran once at
        # load time. Only actual tree nodes here represent per-query work.
        sorts = [node.sortOrder().toString() for node in plan_nodes if node.nodeName() == "Sort"]
        assert not any("_sender_lookup_account_id" in order or "_receiver_lookup_account_id" in order
                       for order in sorts), sorts
    finally:
        if lookup is not None:
            lookup.unpersist()
        spark.stop()
