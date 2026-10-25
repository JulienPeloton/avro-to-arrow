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
import json
from pathlib import PosixPath

import fastavro
import requests
from requests.exceptions import RequestException

from avro_to_arrow.logger import init_logger

_LOG = init_logger("Fink", "WARNING")


def decode_avro_schema(schema_path: str | PosixPath, timeout: int = 1) -> dict:
    """Returns Avro schema from path

    Notes
    -----
    This function does not handle multi-files avro schema.

    Parameters
    ----------
    schema_path: str, optional
        a local path or URL where to look for schema.
    timeout: int, optional
        Timeout (sec) when attempting to get the schema from URL.
        Default is 1 second.

    Returns
    -------
    parsed_schema: dict
        Dictionary of json format schema for decoding Avro records
    """
    if (schema_path == "") or not (isinstance(schema_path, (str, PosixPath))):
        msg = """
        `schema_path` must be a non-empty string (path to a avsc file)."""
        raise OSError(msg)

    # Assuming http protocol
    if isinstance(schema_path, str) and schema_path.startswith("http"):
        try:
            r = requests.get(schema_path, timeout=timeout)
            schema = json.loads(r.text)
        except RequestException as e:
            msg = f"""
            We could not download {schema_path}.
            {e}
            """
            _LOG.critical(msg)
    else:
        # Assuming local file
        with open(schema_path) as f:
            schema = json.load(f)

    return fastavro.parse_schema(schema)
