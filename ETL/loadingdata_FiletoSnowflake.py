import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from config import ACCOUNT, USER, PASSWORD, WAREHOUSE, DATABASE, SCHEMA, ROLE

conn = snowflake.connector.connect(
    account = ACCOUNT,
    user = USER,
    password = PASSWORD,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA,
    role = ROLE
)

file_path = 'C:\\Users\\Dell\\Downloads\\industry1.csv'
df = pd.read_csv(file_path)

table_name = 'INDUSTRY1'

print(df.columns)

success, nchunks, nrows, _ = write_pandas(conn, df, table_name = 'INDUSTRY1')

if success:
    print(f'Successfully uploaded {nrows} rows to {table_name} in {nchunks} chunks.')
else:
    print('Failed to upload data.')

conn.close()