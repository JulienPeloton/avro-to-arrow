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

import pytest

from avro_to_arrow.schema import _get_alert_schema
from avro_to_arrow.utils import data_path


def test_ztf():
    """Test ZTF avro schema"""
    schema = _get_alert_schema(
        os.path.join(data_path, "ztf/avro_schema_ftransfer_ztf_2026-03-23_11487.json")
    )
    assert isinstance(schema, dict)


def test_lsst():
    """Test LSST avro schema"""
    schema = _get_alert_schema(
        os.path.join(
            data_path, "lsst/avro_schema_ftransfer_lsst_2026-03-18_803281.json"
        )
    )
    assert isinstance(schema, dict)


def test_url_schema():
    """Test remote schema decoding"""
    url = "https://raw.githubusercontent.com/astrolabsoftware/fink-client/refs/heads/master/schemas/distribution_schema_fink_ztf_2.5_3.5.3.avsc"
    schema = _get_alert_schema(url)
    assert isinstance(schema, dict)


def test_oserror():
    """Raise error in case of non valid schema path"""
    with pytest.raises(OSError):
        _get_alert_schema("")
