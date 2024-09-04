import os
import pandas as pd

# Step 1: Read the Excel file
excel_file_path = os.path.normpath('pharma-data-warehouse-edna/PHM_ADMIN_TOOLS/INPUT_FILES/lookml_dash_metadata.xlsx')
df = pd.read_excel(excel_file_path)

# Step 2: Define the base directory for the output files
base_output_dir = os.path.normpath('pharma-data-warehouse-ecom-looker/dashboards')

# Function to sanitize file and folder names
def sanitize_name(name):
    return "".join(c for c in name if c.isalnum() or c in (' ', '_')).rstrip()

# Step 3: Iterate over each row in the DataFrame
reports = []
icons = set()
for index, row in df.iterrows():
    # Check if the necessary columns are not NaN
    if pd.notna(row['folder_nm']) and pd.notna(row['file_nm']) and pd.notna(row['child_report']) and pd.notna(row['parent_report']):
        folder_nm = sanitize_name(row['folder_nm'].strip())
        file_nm = sanitize_name(row['file_nm'].strip())
        child_report = row['child_report'].strip()
        parent_report = row['parent_report'].strip()
        
        # Generate the URL using the provided pattern
        url = f"https://oeadvancedreporting.dev.cardinalhealth.com/embed/dashboards/order_express_adv_rpt::{file_nm}"
        icon = row['parent_icon'].strip()  # Default dummy value for icon
        icons.add(icon)

        # Create the directory if it doesn't exist
        output_dir = os.path.join(base_output_dir, folder_nm)
        os.makedirs(output_dir, exist_ok=True)

        # Create a .gitkeep file in the directory
        gitkeep_path = os.path.join(output_dir, '.gitkeep')
        if not os.path.exists(gitkeep_path):
            with open(gitkeep_path, 'w') as gitkeep_file:
                gitkeep_file.write('')

        # Define the content of the LookML dashboard file
        content = f"""
--- 
- dashboard: {file_nm}
  title: {child_report}
  layout: newspaper
  preferred_viewer: dashboards-next
  description: ''
  preferred_slug: yiWPUWqudJk1iKG1I0uHcy
  elements:
  - title: Coming Soon (Filler)
    name: Coming Soon (Filler)
    model: order_express_adv_rpt
    explore: billing_trnsct_oe_bwt
    type: looker_column
    fields: [billing_trnsct_oe_bwt.calc_bill_dte_date, billing_trnsct_oe_bwt.calc_dlvr_qty_sum]
    fill_fields: [billing_trnsct_oe_bwt.calc_bill_dte_date]
    filters: 
    sorts: [billing_trnsct_oe_bwt.calc_bill_dte_date desc]
    limit: 500
    column_limit: 50
    x_axis_gridlines: false
    y_axis_gridlines: false
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    color_application:
      collection_id: 7c56cc21-66e4-41c9-81ce-a60e1c3967b2
      custom:
        id: df600db5-0803-98e5-940b-f486aa65e86c
        label: Custom
        type: discrete
        colors:
        - "#D51A30"
        - "#12B5CB"
        - "#E52592"
        - "#E8710A"
        - "#F9AB00"
        - "#7CB342"
        - "#9334E6"
        - "#80868B"
        - "#079c98"
        - "#A8A116"
        - "#EA4335"
        - "#FF8168"
      options:
        steps: 5
    x_axis_zoom: true
    y_axis_zoom: true
    label_color: []
    show_null_points: true
    interpolation: linear
    defaults_version: 1
    listen:
      Invoice Date Date: billing_trnsct_oe_bwt.calc_bill_dte_date
      Mercury User Name: mag_user_rlt_cv.mrcry_user_nam
    row: 0
    col: 0
    width: 24
    height: 12
  filters:
  - name: Invoice Date Date
    title: Invoice Date Date
    type: field_filter
    default_value: 2024-05
    allow_multiple_values: true
    required: false
    ui_config:
      type: relative_timeframes
      display: inline
    model: order_express_adv_rpt
    explore: billing_trnsct_oe_bwt
    listens_to_filters: []
    field: billing_trnsct_oe_bwt.calc_bill_dte_date
  - name: Mercury User Name
    title: Mercury User Name
    type: field_filter
    default_value: Ana Rios
    allow_multiple_values: true
    required: false
    ui_config:
      type: advanced
      display: popover
    model: order_express_adv_rpt
    explore: billing_trnsct_oe_bwt
    listens_to_filters: []
    field: mag_user_rlt_cv.mrcry_user_nam
    """

        # Define the file path
        file_path = os.path.join(output_dir, f"{file_nm}.dashboard.lookml")

        # Check if the file already exists
        if not os.path.exists(file_path):
            # Write the content to the file with utf-8 encoding
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(content.strip())

        # Add report details to the reports list
        report = next((r for r in reports if r['name'] == parent_report), None)
        if not report:
            report = {'name': parent_report, 'icon': icon, 'subReports': []}
            reports.append(report)
        report['subReports'].append({
            'name': child_report,
            'url': url
        })

# Step 4: Generate the JavaScript content
js_content = ""
for icon in icons:
    icon_name = icon.replace("Icon", "")
    js_content += f"import {icon} from '@mui/icons-material/{icon_name}';\n"
js_content += "\nconst reports = [\n"
for report in reports:
    js_content += f"  {{\n    name: \"{report['name']}\",\n    icon: {report['icon']},\n    subReports: [\n"
    for sub_report in report['subReports']:
        js_content += f"      {{\n        name: \"{sub_report['name']}\",\n"
        js_content += f"        url: \"{sub_report['url']}\"\n      }},\n"
    js_content += "    ],\n  },\n"
js_content += "];\n\nexport default reports;"

# Step 5: Write the JavaScript content to reports.js in the specified directory
js_file_path = os.path.normpath('order-express-react/src/components/reports.js')
with open(js_file_path, 'w', encoding='utf-8') as js_file:
    js_file.write(js_content)

print("LookML dashboard files and reports.js generated successfully.")