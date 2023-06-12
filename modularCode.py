#modular code
import os
import shutil
import hashlib
import psycopg2


class FolderSync:
    def __init__(self, hostname, database, username, password, source_folder, destination_folder):
        self.hostname = hostname
        self.database = database
        self.username = username
        self.password = password
        self.source_folder = source_folder
        self.destination_folder = destination_folder
        self.conn = None
        self.cur = None

    def calculate_checksum(self, file_path):
        with open(file_path, 'rb') as f:
            hasher = hashlib.md5()
            hasher.update(f.read())
            return hasher.hexdigest()

    def sync_folder(self, source_folder, destination_folder):
        files = os.listdir(source_folder)
        for file in files:
            source_file_path = os.path.join(source_folder, file)
            destination_file_path = os.path.join(destination_folder, file)
            if os.path.isfile(source_file_path):
                if os.path.isfile(destination_file_path):
                    if self.calculate_checksum(source_file_path) == self.calculate_checksum(destination_file_path):
                        continue
                shutil.copy2(source_file_path, destination_folder)
            else:
                subfolder_destination = os.path.join(destination_folder, file)
                self.sync_folder(source_file_path, subfolder_destination)

    def run(self):
        try:
            self.conn = psycopg2.connect(
                host=self.hostname,
                dbname=self.database,
                user=self.username,
                password=self.password
            )
            self.cur = self.conn.cursor()

            self.sync_folder(self.source_folder, self.destination_folder)
            self.cur.execute('DROP TABLE IF EXISTS Source2')
            create_script = '''CREATE TABLE IF NOT EXISTS source2 (
                            NAME TEXT,
                            SIZE INTEGER
                            )'''
            self.cur.execute(create_script)

            self.cur.execute('SELECT COUNT(*) FROM Source2')
            self.conn.commit()

            files = os.listdir(self.destination_folder)
            for file in files:
                file_path = os.path.join(self.destination_folder, file)
                file_size = os.path.getsize(file_path)

                with open(file_path, 'rb') as f:
                    file_data = f.read()

                self.cur.execute('CALL source10(%s, %s)', (file_path, file_size))
                self.conn.commit()

        except Exception as error:
            print(error)
        finally:
            if self.cur is not None:
                self.cur.close()
            if self.conn is not None:
                self.conn.close()



hostname = 'sql-training-db.csuzplcoqc0i.ap-south-1.rds.amazonaws.com'
database = 'Mehak'
username = 'postgres'
pwd = 'Pstraining123$'

source_folder = r'E:\practice\Source'
destination_folder = r'E:\practice\Target'

folder_sync = FolderSync(hostname, database, username, pwd, source_folder, destination_folder)
folder_sync.run()
