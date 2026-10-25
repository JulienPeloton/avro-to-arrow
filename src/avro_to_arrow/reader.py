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
from line_profiler import profile


class AvroReader:
    """Class to easily load Avro files.

    It accepts single avro file (.avro), gzipped avro file (.avro.gz), and
    folders containing several of these.

    Parameters
    ----------
    path: str
        Path to Avro file or folder containing Avro files.
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
        elif os.path.isfile(path) and (
            fastavro.is_avro(path) or path.endswith("avro.gz")
        ):
            self.filenames = [path]
        else:
            msg = f"""
            Data path not understood: {path}
            You must give an avro file with
            its extension (.avro or .avro.gz), or a folder with avro files.
            """
            raise OSError(msg)

    @profile
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

    @profile
    def to_pandas(self) -> pd.DataFrame:
        """Read Avro file(s) and return data as Pandas DataFrame

        Returns
        -------
        pd.DataFrame
            Avro data in a pandas DataFrame
        """
        return pd.DataFrame(self.to_iterator())

    @profile
    def to_list(self, size: int | None = None) -> list:
        """Read Avro file(s) and return data as list of dictionary

        Returns
        -------
        out: list of dictionary
            Avro data (dictionaries) in a list
        size: int, optional
            If not None, return only `size` records.
            Default is None.
        """
        nest = [self._read_single_avro(fn) for fn in self.filenames[:size]]
        return [item for sublist in nest for item in sublist][:size]

    def to_iterator(self) -> Iterable[dict]:
        """Return an iterator for avro data

        Returns
        -------
        out: Iterable[dict]
            Avro record (dictionaries) in an iterator
        """
        for fn in self.filenames:
            yield from self._read_single_avro(fn)
