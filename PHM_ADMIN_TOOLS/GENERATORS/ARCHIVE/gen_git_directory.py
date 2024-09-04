import os
import csv
from datetime import datetime

# Function to count lines in a file
def count_lines_in_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return len(f.readlines())
    except UnicodeDecodeError:
        print(f'Could not count lines in {file_path} due to a UnicodeDecodeError.')
        return 0

# Specify the directory
directory = "pharma-data-warehouse-looker"

# Specify the output directory for the CSV file
output_directory = "pharma-data-warehouse-edna/PHM_ADMIN_TOOLS/OUTPUT_FILES"

# Prepare the metadata
metadata = []
for dirpath, dirnames, files in os.walk(directory):
    for file in files:
        path = os.path.join(dirpath, file)
        info = os.stat(path)
        metadata.append({
            'file_name': file,
            'directory': dirpath,
            'file_type': os.path.splitext(file)[1],
            'is_hidden': file.startswith('.'),
            'size': info.st_size,
            'created': datetime.fromtimestamp(info.st_ctime).strftime('%Y-%m-%d %H:%M:%S'),
            'modified': datetime.fromtimestamp(info.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
            'line_count': count_lines_in_file(path)  # Add line count
        })

# Write the metadata to a CSV file in the output directory
with open(os.path.join(output_directory, 'file_metadata.csv'), 'w', newline='') as csvfile:
    fieldnames = ['file_name', 'directory', 'file_type', 'is_hidden', 'size', 'created', 'modified', 'line_count']  # Add 'line_count' to fieldnames
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in metadata:
        writer.writerow(data)