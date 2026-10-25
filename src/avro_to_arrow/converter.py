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
"""Main avro_to_arrow utilities"""

from typing import Any

import pyarrow as pa

from avro_to_arrow.logger import init_logger

_LOG = init_logger("Fink", "WARNING")


def avro_to_arrow(avro_schema: dict[str, Any], records: list[dict[str, Any]]):
    """Convert Avro records to an Arrow table.

    Parameters
    ----------
    avro_schema: dict
        Avro schema as dictionary
    records: list of dicts
        List of Avro records

    Returns
    -------
    out: PyArrow Table
    out: PyArrow Schema
    """
    # Step 1: Convert Avro schema to Arrow schema
    arrow_schema = avro_schema_to_arrow_schema(avro_schema)

    # Step 2: Convert records to Arrow-compatible format
    arrow_data = transform_records(avro_schema, records)

    # Step 3: Create and return Arrow table
    return pa.table(arrow_data, schema=arrow_schema), arrow_schema


def avro_schema_to_arrow_schema(avro_schema: dict[str, Any]) -> pa.Schema:
    """Convert Avro schema to PyArrow schema.

    Parameters
    ----------
    avro_schema: dict
        Avro schema as dictionary

    Returns
    -------
    out: pa.Schema
        PyArrow schema
    """
    if avro_schema.get("type") == "record":
        fields = []
        for field in avro_schema.get("fields", []):
            field_name = field["name"]
            field_type = avro_type_to_arrow_type(field["type"])
            fields.append(pa.field(field_name, field_type))
        return pa.schema(fields)
    else:
        raise ValueError("Root schema must be a record type")


def avro_type_to_arrow_type(avro_type: Any) -> pa.DataType:
    """Convert a single Avro type to PyArrow type.

    Notes
    -----
    Handles: primitives, records, arrays, maps, unions with null.

    Parameters
    ----------
    avro_type: Any
        Any single Avro type

    Returns
    -------
    out: pa.DataType
        Corresponding PyArrow DataType
    """
    # Handle union types (e.g., ["long", "null"])
    if isinstance(avro_type, list):
        # Filter out "null" to get the actual type
        non_null_types = [t for t in avro_type if t != "null"]

        if len(non_null_types) == 0:
            return pa.null()
        elif len(non_null_types) == 1:
            # Nullable type: convert the non-null type
            return avro_type_to_arrow_type(non_null_types[0])
        else:
            # Multiple non-null types (union) - use dense union
            # For simplicity, we'll use the first type
            return avro_type_to_arrow_type(non_null_types[0])

    # Handle string type names (primitives)
    if isinstance(avro_type, str):
        type_map = {
            "null": pa.null(),
            "boolean": pa.bool_(),
            "int": pa.int32(),
            "long": pa.int64(),
            "float": pa.float32(),
            "double": pa.float64(),
            "bytes": pa.binary(),
            "string": pa.string(),
        }
        if avro_type in type_map:
            return type_map[avro_type]
        raise ValueError(f"Unknown Avro type: {avro_type}")

    # Handle complex types (dict)
    if isinstance(avro_type, dict):
        avro_type_name = avro_type.get("type")

        # Record type
        if avro_type_name == "record":
            fields = []
            for field in avro_type.get("fields", []):
                field_name = field["name"]
                field_type = avro_type_to_arrow_type(field["type"])
                fields.append(pa.field(field_name, field_type))
            return pa.struct(fields)

        # Array type
        elif avro_type_name == "array":
            item_type = avro_type_to_arrow_type(avro_type["items"])
            return pa.list_(item_type)

        # Map type
        elif avro_type_name == "map":
            value_type = avro_type_to_arrow_type(avro_type["values"])
            return pa.map_(pa.string(), value_type)

        else:
            raise ValueError(f"Unknown complex Avro type: {avro_type_name}")

    raise ValueError(f"Cannot convert Avro type: {avro_type}")


def transform_records(
    avro_schema: dict[str, Any], records: list[dict[str, Any]]
) -> dict[str, list[Any]]:
    """Transform Avro records into Arrow-compatible format.

    Notes
    -----
    Handles nested records, arrays, maps, and null values.

    Parameters
    ----------
    avro_schema: dict
        Avro schema as dictionary
    records: list of dicts
        List of Avro records

    Returns
    -------
    out: dict
        Arrow-compatible data
    """
    fields = avro_schema.get("fields", [])
    arrow_data = {field["name"]: [] for field in fields}

    for record in records:
        for field in fields:
            field_name = field["name"]
            field_type = field["type"]
            value = record.get(field_name)

            # Transform the value based on its type
            transformed_value = transform_value(value, field_type)
            arrow_data[field_name].append(transformed_value)

    return arrow_data


def transform_value(value: Any, avro_type: Any) -> Any:
    """Transform a single Avro value to Arrow-compatible format.

    Notes
    -----
    Recursively handles nested structures.

    Parameters
    ----------
    value: Any
        Any value
    avro_type: Any
        Corresponding Avro type

    Returns
    -------
    out: Any
        Corresponding value in Arrow-compatible format
    """
    # Handle None/null
    if value is None:
        return None

    # Handle union types
    if isinstance(avro_type, list):
        non_null_types = [t for t in avro_type if t != "null"]
        if len(non_null_types) > 0:
            return transform_value(value, non_null_types[0])
        return None

    # Handle primitive types (strings)
    if isinstance(avro_type, str):
        return value  # Primitives pass through as-is

    # Handle complex types (dicts)
    if isinstance(avro_type, dict):
        avro_type_name = avro_type.get("type")

        # Record type: convert dict to tuple (for struct)
        if avro_type_name == "record":
            fields = avro_type.get("fields", [])
            transformed_record = {}
            for field in fields:
                field_name = field["name"]
                field_value = value.get(field_name) if isinstance(value, dict) else None
                transformed_record[field_name] = transform_value(
                    field_value, field["type"]
                )
            return transformed_record

        # Array type: transform each item
        elif avro_type_name == "array":
            if not isinstance(value, list):
                return None
            item_type = avro_type["items"]
            return [transform_value(item, item_type) for item in value]

        # Map type: transform each value
        elif avro_type_name == "map":
            if not isinstance(value, dict):
                return None
            value_type = avro_type["values"]
            return {k: transform_value(v, value_type) for k, v in value.items()}

    return value
