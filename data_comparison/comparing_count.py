import pandas as pd
import oracledb
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime
from config import ACCOUNT, USER, PASSWORD, WAREHOUSE, DATABASE, SCHEMA, ROLE

oracle_dsn = oracledb.makedsn('localhost', '1521', sid='myyorcl')
oracle_conn = oracledb.connect(user='system', password='1234', dsn=oracle_dsn)

snowflake_conn = snowflake.connector.connect(
    account = ACCOUNT,
    user = USER,
    password = PASSWORD,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA,
    role = ROLE
)

metadata_query = "SELECT table_name, start_date, end_date FROM sys.xyz_metadata"
metadata_df = pd.read_sql_query(metadata_query, oracle_conn)

def get_record_count(conn, table, date, db_type):

    if db_type == 'oracle':
        query = f"SELECT COUNT(*) FROM sys.{table} WHERE business_date = TO_DATE('{date}', 'YYYY-MM-DD')"
    else: # snowflake
        query = f"SELECT COUNT(*) FROM {table} WHERE business_date = '{date}'"

    cursor = conn.cursor()
    cursor.execute(query)
    count = cursor.fetchone()[0]
    return count

results = []

for index, row in metadata_df.iterrows():
    table_name = row['TABLE_NAME']
    start_date = row['START_DATE']
    end_date = row['END_DATE']
    current_date = start_date

    while current_date<=end_date:
        date_str = current_date.strftime('%Y-%m-%d')

        count_oracle = get_record_count(oracle_conn, table_name, date_str, 'oracle')
        count_snowflake = get_record_count(snowflake_conn, table_name, date_str, 'snowflake')
        
        status = 'MATCHED' if count_oracle == count_snowflake else 'UNMATCHED'
        
        results.append([current_date, table_name, count_oracle, count_snowflake, status])
        
        current_date += datetime.timedelta(days=1)

results_df = pd.DataFrame(results, columns=['business_date', 'table_name', 'count_oracle', 'count_snowflake', 'status'])

create_results_table_query = """
CREATE TABLE match_result (
    business_date DATE,
    table_name VARCHAR2(50),
    count_oracle NUMBER(10),
    count_snowflake NUMBER(10),
    status VARCHAR2(10)
)
"""
oracle_cursor = oracle_conn.cursor()
oracle_cursor.execute(create_results_table_query)

insert_query = """
INSERT INTO match_result (business_date, table_name, count_oracle, count_snowflake, status) 
VALUES (:1, :2, :3, :4, :5)
"""

oracle_cursor.executemany(insert_query, results_df.values.tolist())

oracle_conn.commit()
oracle_conn.close()
snowflake_conn.close()

print("result stored in table!")