import oracledb
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import argparse
import os
from datetime import date, datetime, timedelta
from config import ACCOUNT, USER, PASSWORD, WAREHOUSE, DATABASE, SCHEMA, ROLE


def snowflake_to_oracle(table_name_oracle, table_name_snowflake, businessdate):

    dsn_tns = oracledb.makedsn('localhost', '1521', sid='myyorcl')
    oracle_conn = oracledb.connect(user='system', password='1234', dsn=dsn_tns)

    try:
        date_stro = datetime.strptime(str(businessdate), '%d-%m-%Y').strftime('%d-%b-%Y')
    except ValueError:
        date_stro = datetime.strptime(str(businessdate), '%Y-%m-%d %H:%M:%S').strftime('%d-%b-%Y')

    query = f'''SELECT * FROM sys.{table_name_oracle} where business_date='{date_stro}' '''

    df1 = pd.read_sql_query(query, oracle_conn)

    snowflake_conn = snowflake.connector.connect(
        account = ACCOUNT,
        user = USER,
        password = PASSWORD,
        warehouse = WAREHOUSE,
        database = DATABASE,
        schema = SCHEMA,
        role = ROLE
    )

    cursor = snowflake_conn.cursor()

    try:
        date_str = datetime.strptime(str(businessdate), '%d-%m-%Y').strftime('%Y-%m-%d')
    except ValueError:
        date_str = datetime.strptime(str(businessdate), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

    query = f'''SELECT * FROM {table_name_snowflake} where business_date='{date_str}' '''
    cursor.execute(query)
    df2 = cursor.fetch_pandas_all()

    count(df1, df2)



def snowflake_to_file(file_name, table_name, businessdate):

    snowflake_conn = snowflake.connector.connect(
        account = ACCOUNT,
        user = USER,
        password = PASSWORD,
        warehouse = WAREHOUSE,
        database = DATABASE,
        schema = SCHEMA,
        role = ROLE
    )

    cursor = snowflake_conn.cursor()
    try:
        date_str = datetime.strptime(str(businessdate), '%d-%m-%Y').strftime('%Y-%m-%d')
    except ValueError:
        date_str = datetime.strptime(str(businessdate), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

    query = f'''SELECT * FROM {table_name} where business_date='{date_str}' '''

    cursor.execute(query)

    df1 = pd.read_csv(file_name)
    df2 = cursor.fetch_pandas_all()

    count(df1, df2)
    

def oracle_to_file(file_name, table_name, businessdate):
    dsn_tns = oracledb.makedsn('localhost', '1521', sid='myyorcl')
    conn = oracledb.connect(user='system', password='1234', dsn=dsn_tns)

    try:
        date_stro = datetime.strptime(str(businessdate), '%d-%m-%Y').strftime('%d-%b-%Y')
    except ValueError:
        date_stro = datetime.strptime(str(businessdate), '%Y-%m-%d %H:%M:%S').strftime('%d-%b-%Y')

    query = f'''SELECT * FROM sys.{table_name} where business_date='{date_stro}' '''

    df1 = pd.read_csv(file_name)
    df2 = pd.read_sql_query(query, conn)

    count(df1, df2)


def file_to_file(file1, file2):

    _, file_extension = os.path.splitext(file1)
        
    if file_extension in ['.csv']:
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
    elif file_extension in ['.json']:
        df1 = pd.read_json(file1)
        df2 = pd.read_json(file2)
    else:
        print("file type not supported")

    count(df1, df2)


def count(df1, df2):
    count1 = len(df1)
    count2 = len(df2)
    print(f"COUNT OF FILE1 : {count1}" )
    print(f"COUNT OF FILE2 : {count2}")

    if(count1 == count2):
        compare_dataframes(df1, df2)
    else:
        compare_unidentical_df(df1, df2)


def compare_dataframes(df1, df2):
    x=df1.columns.tolist()
    y=df2.columns.tolist()
    df1_sorted = df1.sort_values(by=x).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=y).reset_index(drop=True)

    df1_sorted, df2_sorted = df1_sorted.align(df2_sorted, join='outer', axis=0) #fill_value=float('nan')
        
    df2_sorted = df2_sorted[df1_sorted.columns]
        
    differences = df1_sorted.compare(df2_sorted, keep_shape=True, keep_equal=False, result_names=('source', 'destination'))

    result = differences.dropna(how='all')

    if(result.empty):
        print("Data is same in both files.")
    else:
        print(result)
        output_file_path = 'C:\\Users\\Dell\\Downloads\\result.xlsx'
        result.to_excel(output_file_path, index=True)
        print("result saved")


def compare_unidentical_df(df1, df2):
    x=df1.columns.tolist()
    y=df2.columns.tolist()
    df1_sorted = df1.sort_values(by=x).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=y).reset_index(drop=True)
    
    rows_only_in_df1 = df1_sorted.merge(df2_sorted, how='outer', indicator=True).query('_merge == "left_only"').assign(location='source').drop(columns='_merge')
    rows_only_in_df2 = df1_sorted.merge(df2_sorted, how='outer', indicator=True).query('_merge == "right_only"').assign(location='destination').drop(columns='_merge')

    result = pd.concat([rows_only_in_df1, rows_only_in_df2], ignore_index=True)
    
    print(result)
    output_file_path = 'C:\\Users\\Dell\\Downloads\\result.xlsx'
    result.to_excel(output_file_path, index=True)
    print("result saved")


def parse_arguments():

    parser = argparse.ArgumentParser(description="Comparison script with multiple parameters")

    parser.add_argument('--oracle_table', type=str, required=False, default=".")
    parser.add_argument('--snowflake_table', type=str, required=False, default=".")
    parser.add_argument('--filename1', type=str, required=False, default=".")
    parser.add_argument('--filename2', type=str, required=False, default=".")
    parser.add_argument('--directory1', type=str, required=False, default=".")
    parser.add_argument('--directory2', type=str, required=False, default=".")
    parser.add_argument('--business_date', type=str, required=False)
    parser.add_argument('--comparison_type', type=str, help="Type of comparison", choices=['ff', 'fo', 'fs', 'os'], required=True)
    args = parser.parse_args()

    return args

def get_business_date():
    dsn_tns = oracledb.makedsn('localhost', '1521', sid='myyorcl')
    conn = oracledb.connect(user='system', password='1234', dsn=dsn_tns)
    
    query = 'select business_date from sys.v_business_date'
    cursor = conn.cursor()
    cursor.execute(query)

    datee = cursor.fetchone()[0]

    return datee


def main():
    args = parse_arguments()

    oracle_table = args.oracle_table
    snowflake_table = args.snowflake_table
    filename1 = args.filename1
    filename2 = args.filename2
    directory1 = args.directory1
    directory2 = args.directory2
    comparison_type = args.comparison_type
    businessdate = args.business_date

    if(businessdate==None):
        businessdate = get_business_date()
    else:
        businessdate = args.business_date

    print("BUSINESS DATE : ")
    print(businessdate)

    file1 = os.path.join(directory1, filename1)
    file2 = os.path.join(directory2, filename2)

    if comparison_type == 'ff':
        file_to_file(file1, file2)
    elif comparison_type == 'fo':
        oracle_to_file(file1, oracle_table, businessdate)
    elif comparison_type == 'fs':
        snowflake_to_file(file1, snowflake_table, businessdate)
    elif comparison_type == 'os':
        snowflake_to_oracle(oracle_table, snowflake_table, businessdate)
    else:
        print("Invalid")

if __name__ == '__main__':
    main()