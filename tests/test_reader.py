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

from avro_to_arrow.reader import AvroReader
from avro_to_arrow.utils import data_path


def test_single_avro():
    """Load a single Avro file"""
    r = AvroReader(os.path.join(data_path, "ZTF19acihgng.avro"))
    list_of_alerts = r.to_list()
    assert len(list_of_alerts) == 1, len(list_of_alerts)


def test_multiple_files():
    """Read a bunch of files"""
    r = AvroReader(os.path.join(data_path))
    records = r.to_list()
    assert "objectId" in records[0]
    assert len(records) == 18, len(records)


def test_multiple_avro_records():
    """If there are several records in one file, they are all retrieved"""
    r = AvroReader(os.path.join(data_path, "avro_multi_alerts.avro"))
    assert len(r.to_list()) == 11, len(r.to_list())


def test_multiple_avro_records_with_condition():
    """If there are several records in one file, you can read a subset"""
    r = AvroReader(os.path.join(data_path, "avro_multi_alerts.avro"))
    assert len(r.to_list(size=2)) == 2, len(r.to_list(size=2))

    r = AvroReader(os.path.join(data_path, "avro_multi_alerts_other.avro"))
    assert len(r.to_list()) == 3, len(r.to_list())


def test_list_of_files():
    """If you give a list of files"""
    files = [
        os.path.join(data_path, f) for f in ["ZTF19acihgng.avro", "ZTF19acyfkzd.avro"]
    ]
    r = AvroReader(files)
    assert len(r.to_list()) == 2


def test_read_gz_avro():
    """Test GZ decompression"""
    r = AvroReader(
        os.path.join(
            data_path, "elasticc/alert_mjd60674.0512_obj747_src1494043.avro.gz"
        )
    )
    assert len(r.to_list()) == 1, len(r.to_list())


def test_iterator():
    r = AvroReader(data_path)
    myiterator = r.to_iterator()
    assert "objectId" in next(myiterator)


def test_wrong_extension():
    with pytest.raises(OSError):
        AvroReader("a_file_with_no_extension")
