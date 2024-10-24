import pandas as pd
import oracledb

dsn_tns = oracledb.makedsn('localhost', '1521', sid='myyorcl')
conn = oracledb.connect(user='system', password='1234', dsn=dsn_tns)
cursor = conn.cursor()

# Insert data from staging_table into nrm_table with transformations
insert_query = """
    INSERT INTO sys.industry_nrm (iname, industry_no, employees, founded, sales, revenue, industry_key, audit_insert)
    SELECT
        iname,
        industry_no,
        employees,
        founded,
        sales,
        sales*12 as revenue,
        industry_no + founded as industry_key,
        SYSDATE as audit_insert
    FROM sys.industry_staging
"""

cursor.execute(insert_query)
conn.commit()
cursor.close()
conn.close()

print("Data loaded into nrm table successfully")