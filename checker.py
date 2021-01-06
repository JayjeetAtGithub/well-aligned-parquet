import pyarrow.parquet as pq
import os
import sys

"""With Snappy compressed files that has dictionary encoding disabled, we can get fixed differences in between
the row groups."""

FILENAME=str(sys.argv[1])

file_size = os.stat(FILENAME).st_size
meta = pq.read_metadata(FILENAME)

print("Metadata: ", meta)

rg_padding = 0
for i in range(0, meta.num_row_groups):
	print("RowGroup row count: ", meta.row_group(i).num_rows)

