"""The fixed lookup is loaded once; failed materialization releases its cache."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark import StorageLevel

from streaming.ingestion.spark_streaming_pipeline import load_entity_lookup


def setup_lookup(tmp_path):
    path = tmp_path / "lookup.parquet"
    path.mkdir()
    lookup = MagicMock()
    lookup.select.return_value = lookup
    lookup.dropDuplicates.return_value = lookup
    lookup.persist.return_value = lookup
    lookup.repartition.return_value = lookup
    lookup.sortWithinPartitions.return_value = lookup
    reader = MagicMock()
    reader.parquet.return_value = lookup
    return SimpleNamespace(read=reader, conf=SimpleNamespace(get=lambda *args: "8")), path, lookup


def test_static_lookup_materialized_once(tmp_path):
    spark, path, lookup = setup_lookup(tmp_path)
    assert load_entity_lookup(spark, path) is lookup
    spark.read.parquet.assert_called_once_with(str(path))
    lookup.select.assert_called_once_with("account_id", "entity_id")
    lookup.dropDuplicates.assert_called_once_with(["account_id"])
    lookup.repartition.assert_called_once_with(8, "account_id")
    lookup.sortWithinPartitions.assert_called_once_with("account_id")
    lookup.persist.assert_called_once_with(StorageLevel.MEMORY_AND_DISK)
    lookup.count.assert_called_once_with()


def test_uncached_baseline_remains_lazy(tmp_path):
    spark, path, lookup = setup_lookup(tmp_path)
    assert load_entity_lookup(spark, path, cache_lookup=False) is lookup
    lookup.persist.assert_not_called()
    lookup.count.assert_not_called()
    lookup.repartition.assert_not_called()
    lookup.sortWithinPartitions.assert_not_called()


def test_failed_load_releases_static_cache(tmp_path):
    spark, path, lookup = setup_lookup(tmp_path)
    lookup.count.side_effect = RuntimeError("materialization failed")
    with pytest.raises(RuntimeError, match="materialization failed"):
        load_entity_lookup(spark, path)
    lookup.unpersist.assert_called_once_with()
