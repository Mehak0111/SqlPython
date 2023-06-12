#file syncing
import os
import shutil
import hashlib

source_folder = r'C:\Users\hp\Desktop\Source'
destination_folder = r'C:\Users\hp\Desktop\Target'

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

# Call the sync_folder function
sync_folder(source_folder, destination_folder)
