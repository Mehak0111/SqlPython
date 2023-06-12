#db connections
import os
import shutil
import hashlib
import psycopg2

hostname = 'sql-training-db.csuzplcoqc0i.ap-south-1.rds.amazonaws.com'
database = 'Mehak'
username = 'postgres'
pwd = 'Pstraining123$'
conn = None
cur = None
source_folder = r'E:\practice\Source'
destination_folder = r'E:\practice\Target'


def calculate_checksum(file_path):
    with open(file_path, 'rb') as f:
        hasher = hashlib.md5()
        hasher.update(f.read())
        return hasher.hexdigest()


def sync_folder(source_folder, destination_folder):
    files = os.listdir(source_folder)
    for file in files:
        source_file_path = os.path.join(source_folder, file)
        destination_file_path = os.path.join(destination_folder, file)
        if os.path.isfile(source_file_path):
            if os.path.isfile(destination_file_path):
                if calculate_checksum(source_file_path) == calculate_checksum(destination_file_path):
                    continue
            shutil.copy2(source_file_path, destination_folder)
        else:
            subfolder_destination = os.path.join(destination_folder, file)
            sync_folder(source_file_path, subfolder_destination)


try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
    )
    cur = conn.cursor()

    sync_folder(source_folder, destination_folder)
    cur.execute('DROP TABLE IF EXISTS Source2')
    create_script = '''CREATE TABLE IF NOT EXISTS Source2 (
                    NAME VARCHAR(20),
                    SIZE INTEGER,
                    DATA TEXT
                    )'''
    cur.execute(create_script)
  
    cur.execute('SELECT COUNT(*) FROM Source2')  
    conn.commit()

    files = os.listdir(destination_folder)
    for file in files:
        file_path = os.path.join(destination_folder, file)
        file_size = os.path.getsize(file_path)

        with open(file_path, 'rb') as f:
            file_data = f.read()

        cur.execute('INSERT INTO Source2 (name, size, data) VALUES (%s, %s, %s)', (file, file_size, file_data))
        conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
