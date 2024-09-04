import os
import pandas as pd
from datetime import datetime

# Get current date
current_date = datetime.now().strftime('%m%d%Y')

# Specify the path to the CSV file
csv_file_path = 'pharma-data-warehouse-edna/PHM_ADMIN_TOOLS/INPUT_FILES/collibra_lineage.csv'  # replace with your CSV file path

# Read data from the CSV file
data = pd.read_csv(csv_file_path)

# Group the data by 'TRGT_SCHEMA_NAME'
grouped = data.groupby('TRGT_SCHEMA_NAME')

# Iterate over each group
for name, group in grouped:
    # Create a directory for each 'TRGT_SCHEMA_NAME' if it does not exist
    dir_name = os.path.join('C:/Users/joseph.holmesmeyer/OneDrive - Cardinal Health/Data_Engineering/Collibra','Generated_Files_Production', name)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # Create the full path for the file
    file_path = os.path.join(dir_name, f"{name}_{current_date}.xlsx")

    # Drop the first two columns
    group = group.drop(columns=['SOURCE_NAME', 'SOURCE_FULL_NAME'])

    # Write the group data to an Excel file
    group.to_excel(file_path, index=False)

     # Print the path of the created file and its directory
    print(f"File created: {file_path}")

print("Data extraction completed successfully.")