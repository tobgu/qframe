# Utility script for cross language test of arrow format.
#
# Requires that pyarrow is installed:
# pip install pyarrow
#
# Run:
# python arrow.py

import pyarrow as pa

def write_data(data_dict, file_name):
    keys = sorted(data_dict.keys())
    data = [pa.array(data_dict[k]) for k in keys]
    batch = pa.RecordBatch.from_arrays(data, keys)
    writer = pa.RecordBatchStreamWriter(file_name, batch.schema)
    writer.write(batch)
    writer.close()


write_data({'f0': [True, False, True]}, 'bool.bin')
write_data({'f0': [1.5, 2.5, None]}, 'float.bin')
write_data({'f0': ['foo', 'bar', None]}, 'string.bin')
write_data({'f0': [1, 2, 3]}, 'int.bin')
write_data({'f0': [1, 2, 3],
            'f1': [1.5, 2.5, None],
            'f2': [True, False, True],
            'f3': ['foo', 'bar', None]}, 'mixed.bin')

# TODO: dictionary/enum
# TODO: corner cases, empty arrays for example
# TODO: Test with tables/columns as well