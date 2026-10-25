# Copyright 2026 Julien Peloton
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

import pyarrow as pa
import pytest

from avro_to_arrow import avro_to_arrow
from avro_to_arrow.converter import (
    avro_schema_to_arrow_schema,
    avro_type_to_arrow_type,
)
from avro_to_arrow.reader import AvroReader
from avro_to_arrow.schema import _get_alert_schema
from avro_to_arrow.utils import data_path


@pytest.mark.parametrize(
    "avro,schema",
    [
        (
            "ztf/part-3-305711.avro",
            "ztf/avro_schema_ftransfer_ztf_2026-03-23_11487.json",
        ),
        (
            "lsst/part-2-918301.avro",
            "lsst/avro_schema_ftransfer_lsst_2026-03-18_803281.json",
        ),
    ],
)
def test_converter(avro, schema):
    """Convert Avro records into arrow table"""
    records = AvroReader(os.path.join(data_path, avro)).to_list()
    avro_schema = _get_alert_schema(os.path.join(data_path, schema))
    table, arrow_schema = avro_to_arrow(avro_schema, records)
    assert isinstance(table, pa.Table)
    assert isinstance(arrow_schema, pa.Schema)


@pytest.mark.parametrize(
    "schema",
    [
        "ztf/avro_schema_ftransfer_ztf_2026-03-23_11487.json",
        "lsst/avro_schema_ftransfer_lsst_2026-03-18_803281.json",
    ],
)
def test_schema_converter(schema):
    """Convert Avro schema into arrow schema"""
    avro_schema = _get_alert_schema(os.path.join(data_path, schema))
    arrow_schema = avro_schema_to_arrow_schema(avro_schema)
    assert isinstance(arrow_schema, pa.Schema)


def test_wrong_schema_converter():
    """Root schema must be a record type"""
    avro_schema = {"type": None}
    with pytest.raises(ValueError):
        avro_schema_to_arrow_schema(avro_schema)


@pytest.mark.parametrize(
    ("avro_type", "expected"),
    [
        ("null", pa.null()),
        ("boolean", pa.bool_()),
        ("int", pa.int32()),
        ("long", pa.int64()),
        ("float", pa.float32()),
        ("double", pa.float64()),
        ("bytes", pa.binary()),
        ("string", pa.string()),
    ],
)
def test_primitive_types(avro_type, expected):
    assert avro_type_to_arrow_type(avro_type) == expected


@pytest.mark.parametrize(
    ("avro_type", "expected"),
    [
        (["null"], pa.null()),
        (["long", "null"], pa.int64()),
        (["null", "string"], pa.string()),
        # The implementation converts only the first non-null union member.
        (["int", "string"], pa.int32()),
        (["null", "int", "string"], pa.int32()),
    ],
)
def test_union_types(avro_type, expected):
    assert avro_type_to_arrow_type(avro_type) == expected


@pytest.mark.parametrize(
    ("avro_type", "expected"),
    [
        (
            {
                "type": "record",
                "name": "Person",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                ],
            },
            pa.struct(
                [
                    pa.field("id", pa.int64()),
                    pa.field("name", pa.string()),
                ]
            ),
        ),
        (
            {
                "type": "record",
                "name": "Empty",
                "fields": [],
            },
            pa.struct([]),
        ),
        (
            {"type": "array", "items": "int"},
            pa.list_(pa.int32()),
        ),
        (
            {"type": "map", "values": "string"},
            pa.map_(pa.string(), pa.string()),
        ),
        (
            {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Item",
                    "fields": [{"name": "value", "type": "double"}],
                },
            },
            pa.list_(pa.struct([pa.field("value", pa.float64())])),
        ),
        (
            {
                "type": "map",
                "values": {"type": "array", "items": "long"},
            },
            pa.map_(pa.string(), pa.list_(pa.int64())),
        ),
    ],
)
def test_complex_types(avro_type, expected):
    assert avro_type_to_arrow_type(avro_type) == expected


@pytest.mark.parametrize(
    ("avro_type", "exception", "message"),
    [
        ("unknown", ValueError, "Unknown Avro type"),
        (
            {"type": "enum", "name": "Color", "symbols": ["red"]},
            ValueError,
            "Unknown complex Avro type",
        ),
        (42, ValueError, "Cannot convert Avro type"),
        (None, ValueError, "Cannot convert Avro type"),
    ],
)
def test_unsupported_types_raise(avro_type, exception, message):
    with pytest.raises(exception, match=message):
        avro_type_to_arrow_type(avro_type)


def test_record_with_missing_field_type_raises():
    avro_type = {
        "type": "record",
        "name": "Invalid",
        "fields": [{"name": "value"}],
    }

    with pytest.raises(KeyError, match="type"):
        avro_type_to_arrow_type(avro_type)


def test_array_with_missing_items_raises():
    with pytest.raises(KeyError, match="items"):
        avro_type_to_arrow_type({"type": "array"})


def test_map_with_missing_values_raises():
    with pytest.raises(KeyError, match="values"):
        avro_type_to_arrow_type({"type": "map"})
