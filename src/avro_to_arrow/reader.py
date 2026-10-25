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
import glob
import gzip
import os
from collections.abc import Iterable

import fastavro
import pandas as pd
from fastavro import reader


class AvroReader:
    """Class to easily load Avro files.

    It accepts single avro file (.avro), gzipped avro file (.avro.gz), and
    folders containing several of these.

    Parameters
    ----------
    path: str
        Path to Avro file or folder containing Avro files.

    Examples
    --------
    Load a single Avro file
    >>> r = AvroReader(os.path.join(data_path, "ZTF19acihgng.avro"))
    >>> list_of_alerts = r.to_list()
    >>> print(len(list_of_alerts))
    1

    Read a bunch of files
    >>> r = AvroReader(os.path.join(data_path))
    >>> df = r.to_pandas()
    >>> assert('objectId' in  df.columns)

    If there are several alerts in one file, they are all retrieved
    >>> r = AvroReader(os.path.join(data_path, "avro_multi_alerts.avro"))
    >>> print('{} avro decoded'.format(len(r.to_list())))
    11 avro decoded

    If there are several avro in one file, they are all retrieved
    >>> r = AvroReader(os.path.join(data_path, "avro_multi_alerts_other.avro"))
    >>> print('{} avro decoded'.format(len(r.to_list())))
    3 avro decoded

    If you give a list of files
    >>> files = [os.path.join(data_path, f) for f in ["ZTF19acihgng.avro", "ZTF19acyfkzd.avro"]]
    >>> r = AvroReader(files)
    >>> print('{} avro decoded'.format(len(r.to_list())))
    2 avro decoded
    """

    def __init__(self, path: str):
        """Initialise the AvroReader class"""
        self.path = path
        self._load_avro_files()

    def _load_avro_files(self, ext_path: str | None = None):
        """Load Avro data

        Parameters
        ----------
        ext_path: str, optional
            If not None, load explicitly data under `ext_path`.
            Default is None (self.path is used).
        """
        if ext_path is not None:
            path = ext_path
        else:
            path = self.path

        if isinstance(path, list):
            self.filenames = path
        elif os.path.isdir(path):
            self.filenames = glob.glob(os.path.join(path, "*.avro*"))
        elif path == "":
            print("WARNING: path to avro files is empty")
            self.filenames = []
        elif fastavro.is_avro(path) or path.endswith("avro.gz"):
            self.filenames = [path]
        else:
            msg = f"""
            Data path not understood: {path}
            You must give an avro file with
            its extension (.avro or .avro.gz), or a folder with avro files.
            """
            raise OSError(msg)

    def _read_single_avro(self, name: str | None = None) -> dict:
        """Read an avro record, and return data as dictionary

        Parameters
        ----------
        name: str, optional
            Name of the file to read (avro format).
            Default is None (self.path is used).

        Returns
        -------
        dict
            Avro record in a dictionary

        Examples
        --------
        >>> r = AvroReader("")
        WARNING: path to avro files is empty

        >>> record = r._read_single_avro(name=os.path.join(data_path, "ZTF19acihgng.avro"))
        """
        if name is None:
            name = self.path

        data = []

        if name.endswith("avro"):
            copen = lambda x: open(x, mode="rb")  # noqa: SIM115
        elif name.endswith("avro.gz"):
            copen = lambda x: gzip.open(x, mode="rb")  # noqa: SIM115
        else:
            msg = f"""
            Avro filename should end with `avro` or `avro.gz`.
            Currently trying to read: {name}
            """
            raise NotImplementedError(msg)

        with copen(name) as fo:
            avro_reader = reader(fo)
            for record in avro_reader:
                data.append(record)  # noqa: PERF402
        return data

    def to_pandas(self) -> pd.DataFrame:
        """Read Avro file(s) and return data as Pandas DataFrame

        Returns
        -------
        pd.DataFrame
            Avro data in a pandas DataFrame

        Examples
        --------
        >>> r = AvroReader(data_path)
        >>> df = r.to_pandas()
        >>> assert('objectId' in r.to_pandas().columns)

        >>> r = AvroReader(os.path.join(data_path, "elasticc/alert_mjd60674.0512_obj747_src1494043.avro.gz"))
        >>> df = r.to_pandas()
        >>> assert('diaSource' in r.to_pandas().columns)

        """
        return pd.DataFrame(self.to_iterator())

    def to_list(self, size: int | None = None) -> list:
        """Read Avro file(s) and return data as list of dictionary

        Returns
        -------
        out: list of dictionary
            Avro data (dictionaries) in a list
        size: int, optional
            If not None, return only `size` records.
            Default is None.

        Examples
        --------
        >>> r = AvroReader(os.path.join(data_path, "ZTF19acihgng.avro"))
        >>> mylist = r.to_list()
        >>> print(len(mylist))
        1

        >>> r = AvroReader(data_path)
        >>> mylist = r.to_list(size=2)
        >>> print(len(mylist))
        2

        >>> r = AvroReader(os.path.join(data_path, "elasticc/alert_mjd60674.0512_obj747_src1494043.avro.gz"))
        >>> mylist = r.to_list()
        >>> print(len(mylist))
        1
        """
        nest = [self._read_single_avro(fn) for fn in self.filenames[:size]]
        return [item for sublist in nest for item in sublist][:size]

    def to_iterator(self) -> Iterable[dict]:
        """Return an iterator for avro data

        Returns
        -------
        out: Iterable[dict]
            Avro record (dictionaries) in an iterator

        Examples
        --------
        >>> r = AvroReader(data_path)
        >>> myiterator = r.to_iterator()
        >>> assert('objectId' in next(myiterator).keys())

        """
        for fn in self.filenames:
            yield from self._read_single_avro(fn)
