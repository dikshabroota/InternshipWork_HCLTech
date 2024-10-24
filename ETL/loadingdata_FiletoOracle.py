import pandas as pd
import oracledb

# Database connection details
dsn_tns = oracledb.makedsn('localhost', '1521', sid='myyorcl')
conn = oracledb.connect(user='system', password='1234', dsn=dsn_tns)

df = pd.read_csv('C:\\Users\\Dell\\Downloads\\industry1.csv')

# Create a cursor
cursor = conn.cursor()
insert_sql = """
    INSERT INTO sys.industry_staging (iname, industry_no, employees, founded, sales) VALUES (:1, :2, :3, :4, :5)
"""

# Insert each row into the table
for index, row in df.iterrows():
    cursor.execute(insert_sql, (row['iname'], row['industry_no'], row['employees'], row['founded'], row['sales']))

conn.commit()

cursor.close()
conn.close()
print("Executed successfully!")