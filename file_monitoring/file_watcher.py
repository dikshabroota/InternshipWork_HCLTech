import oracledb
import os
import time
import shutil
import sys

def get_metadata(file_id):

    dsn_tns = oracledb.makedsn('localhost', '1521', sid='myyorcl')
    conn = oracledb.connect(user='system', password='1234', dsn=dsn_tns)

    cursor = conn.cursor()
    
    # Retrieving metadata
    cursor.execute("""
        SELECT remote_file_name, remote_dir_name, local_file_name, local_dir_name
        FROM sys.metadata_table
        WHERE file_id = :file_id
    """, [file_id])
    
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if result:
        return {
            'remote_file_name': result[0],
            'remote_dir_name': result[1],
            'local_file_name': result[2],
            'local_dir_name': result[3]
        }
    else:
        return None

def check_count(filepath):
    size = os.path.getsize(filepath)
    time.sleep(30)
    new_size = os.path.getsize(filepath)
    return size == new_size

def file_watcher(file_id):

    metadata = get_metadata(file_id)
    
    if not metadata:
        print(f"No metadata found for file_id {file_id}")
        return
    

    remote_filepath = os.path.join(metadata['remote_dir_name'], metadata['remote_file_name'])
    local_filepath = os.path.join(metadata['local_dir_name'], metadata['local_file_name'])
    
    while not os.path.exists(remote_filepath):
        print(f"File '{metadata['remote_file_name']}' not found in '{metadata['remote_dir_name']}'. Checking again in 2 minutes...")
        time.sleep(120)  # Waiting for 2 minutes
    

    print(f"File '{metadata['remote_file_name']}' found in {metadata['remote_dir_name']}. Checking if file is fully loaded...")
    
    while not check_count(remote_filepath):
        print(f"File '{remote_filepath}' is still loading. Waiting for 30 seconds...")
        time.sleep(30) # Waiting for 30 seconds

    
    print(f"File '{remote_filepath}' is fully loaded. Copying...")

    shutil.copy(remote_filepath, local_filepath)
    print(f"File copied successfully!!")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python file_watcher.py <file_id>")
    else:
        file_id = sys.argv[1]
        file_watcher(file_id)