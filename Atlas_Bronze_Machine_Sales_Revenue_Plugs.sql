INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_Machine_Sales_Revenue_Plugs', current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
VALUES
  (16, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
  (16, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
  (16, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
  (16, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/machine_sales_revenue_plugs/BPC Adhoc Machine Revenue CY25 December Actuals File_New Structure.xlsx', current_date(), current_date()),
  (16, 'sheet_mapping', '[
  {
    "sheet_name": "For Garrett",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BPC_Plug_Financials"
  }
]', current_date(), current_date());
