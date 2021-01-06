import os
import sys
import pyarrow.parquet as pq

if len(sys.argv) < 3:
    print("usage: ./splitter.py <filename>")
    sys.exit(0)

FILENAME = str(sys.argv[1])
SPLIT = int(sys.argv[2])
RG_PADDING = 84
file_size = os.stat(FILENAME).st_size

metadata = pq.read_metadata(FILENAME)

offsets = list()

num_row_groups = metadata.num_row_groups
num_columns = metadata.num_columns

offset = 4
current_start_offset = -1
current_end_offset = -1

magic_header_offset = (0,4)
print("Magic Header: ", magic_header_offset)
print("\n")
offsets.append(magic_header_offset)

row_group_offsets = list()
print("Row Groups: ")
for i in range(num_row_groups):
    current_start_offset = offset
    current_end_offset = current_start_offset + (10 * 1000 * 1000)
    row_group_offsets.append((current_start_offset, current_end_offset))
    offset = current_end_offset
print(row_group_offsets)
print("\n")
offsets.extend(row_group_offsets)

metadata_offset = (offset, offset + metadata.serialized_size)
print("Metadata: ", metadata_offset)
print("\n")
offsets.append(metadata_offset)

metadata_size_offset = (offset + metadata.serialized_size, offset + metadata.serialized_size + 4)
print("Metadata Size :", metadata_size_offset)
print("\n")
offsets.append(metadata_size_offset)

magic_footer_offset = (offset + metadata.serialized_size + 4, offset + metadata.serialized_size+ 8)
print("Magic Footer: ", magic_footer_offset)
print("\n")
offsets.append(magic_footer_offset)

print("Filesize: ", os.stat(FILENAME).st_size)

if not SPLIT:
    sys.exit(0)

print("Splitting: ")
with open(FILENAME, 'rb') as f:
    off = row_group_offsets[1][0]
    print(off)
    f.seek(off)
    data = f.read(10 * 1000 * 1000)


# now the data is in `data`
with open("row_group_2", "wb") as f:
    f.write(data)
