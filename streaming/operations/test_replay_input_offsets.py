"""Malformed replay fields must survive indexing so validation can reject them."""

from types import SimpleNamespace

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from streaming.operations.validate_stateful_replay import add_input_row_offsets


def test_indexing_preserves_nullable_input_schema_and_original_offsets():
    schema = StructType([StructField("Timestamp", StringType(), True),
                         StructField("Amount Paid", DoubleType(), True)])
    values = [(None, 100.0), ("2022/09/01 00:31", None)]

    class Rows:
        def zipWithIndex(self):
            return self

        def map(self, function):
            return [function(item) for item in zip(values, range(len(values)))]

    def create(rows, schema):
        return rows, schema

    indexed, indexed_schema = add_input_row_offsets(
        SimpleNamespace(createDataFrame=create), SimpleNamespace(schema=schema, rdd=Rows()))
    assert indexed == [(None, 100.0, 0), ("2022/09/01 00:31", None, 1)]
    assert indexed_schema.fields[:2] == schema.fields
    assert indexed_schema.fields[-1] == StructField("_input_row_offset", LongType(), False)
