# Avro to Arrow

This package provides tools to convert from Avro record format to Arrow format. 

## Installation

Simply install the package using pip:

```bash
pip install avro-to-arrow
```

## How-to

In its simplest form, you would use:

```python
from avro_to_arrow import avro_to_arrow

avro_records = ...  # list of Avro records
avro_schema = ...  # Avro schema as dict

arrow_table, arrow_schema = avro_to_arrow(avro_schema, avro_records)
```

We provide some additional methods to decode avro & read schema:

```python
from avro_to_arrow.reader import AvroReader
from avro_to_arrow.schema import decode_avro_schema
from avro_to_arrow import avro_to_arrow

avro_records = AvroReader(path_to_avro).to_list()
avro_schema = decode_avro_schema(path_to_schema)
arrow_table, arrow_schema = avro_to_arrow(avro_schema, avro_records)
```

## Performance

Note that the conversion is done per Avro records, and appended to a pyarrow table. Typical performances for large-scale survey alert data are (avro records to pyarrow table):

| | Conversion throughput |
|-|-|
| ZTF (v4.01) | 20 ms/avro |
| LSST (v11) | 150 ms/avro | 

This test has been performed using a simple [line_profiler](https://github.com/pyutils/line_profiler). You can reproduce it using:

```bash
cd profiling
./profile.sh
```

## To-do

- [ ] Vectorize `transform_records`/`transform_value`
- [ ] Enable iterators for `avro_to_arrow` to avoid loading all Avro data in memory
- [ ] Perform better cycle/memory profiling (simple `line_profiler` was used so far).
- [ ] Add CLI (avro to parquet)
