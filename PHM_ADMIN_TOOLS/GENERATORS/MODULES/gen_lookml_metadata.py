import os
import re
import pandas as pd

def parse_lkml(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        content = file.read()

    # Regular expression to match dimension blocks
    dimension_pattern = re.compile(r'dimension: (\w+)\s*{([^}]*)}', re.MULTILINE | re.DOTALL)
    dimensions = dimension_pattern.findall(content)

    dimension_details = []

    for dimension in dimensions:
        name, body = dimension
        label = re.search(r'label:\s*"([^"]*)"', body)
        tags = re.search(r'tags:\s*\[([^\]]*)\]', body)
        hidden = re.search(r'hidden:\s*(yes|no)', body)
        primary_key = re.search(r'primary_key:\s*(yes|no)', body)
        sql = re.search(r'sql:\s*(.*?);;', body, re.DOTALL)
        type_ = re.search(r'type:\s*(\w+)', body)
        description = re.search(r'description:\s*"([^"]*)"', body)

        dimension_details.append({
            'name': name,
            'label': label.group(1) if label else None,
            'tags': tags.group(1).split(', ') if tags else None,
            'hidden': hidden.group(1) if hidden else 'no',
            'primary_key': primary_key.group(1) if primary_key else 'no',
            'sql': sql.group(1).strip() if sql else None,
            'type': type_.group(1) if type_ else None,
            'description': description.group(1) if description else None,
            'file_path': file_path
        })

    return dimension_details

def main():
    directory_path = 'pharma-data-warehouse-looker/views/'  # Update with your directory path
    all_dimensions = []

    # Walk through all subdirectories
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            if file_name.endswith('.lkml'):
                file_path = os.path.join(root, file_name)
                dimensions = parse_lkml(file_path)
                # Debug statement to check dimensions
                # print(f"Dimensions from {file_path}: {dimensions}")
                all_dimensions.extend(dimensions)

    # Convert to DataFrame
    df = pd.DataFrame(all_dimensions)
    # Debug statement to check DataFrame content
    print(f"DataFrame content: {df}")

    # Save to CSV
    output_file = 'dimensions_pdw.csv'
    df.to_csv(output_file, index=False)
    print(f"Dimensions details saved to {output_file}")

if __name__ == "__main__":
    main()