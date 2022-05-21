
import pyarrow
import pyarrow.parquet as pq
from hdfs import InsecureClient


def normalize(s):
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
    return s


hdfs_client = InsecureClient(f"http://dodrio.fib.upc.es:9870", user='bdm')  # Connect to HDFS

path = '/user/bdm/landingZone/opendata-income/opendatabcn-income.parquet'

hdfs_client.download(path, 'res.parquet', overwrite=True)

with open('res.parquet', 'rb') as reader_local:
    local_table = pq.read_table(reader_local)


# columns
columns = [normalize(x) for x in local_table.column_names]
columns[-1] = 'index_rfid'
print(columns)
new_table = local_table.rename_columns(columns)
with pq.ParquetWriter('opendatabcn-income-fixed.parquet', schema=new_table.schema) as writer:
            writer.write_table(new_table)
            writer.close()
