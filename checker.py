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
for i in range(0, meta.num_row_groups-1):
	rg_padding = meta.row_group(i+1).column(0).data_page_offset -  meta.row_group(i).column(meta.num_columns-1).file_offset
	print("RowGroup Padding: ", rg_padding)

total_rg_size = 0
for i in range(0, meta.num_row_groups):
	total_rg_size += meta.row_group(i).total_byte_size


HEADER = 4
FOOTER_MAGIC = 4

total = HEADER + (total_rg_size + (rg_padding * (meta.num_row_groups-1)))  + meta.serialized_size + 4 + FOOTER_MAGIC

print(total)
print(file_size)
