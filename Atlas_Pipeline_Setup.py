# Databricks notebook source
environment = "development"

results = []

pipeline_name = 'Atlas_Bronze_CDS_CII_Billing_Manual'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/cds_cii_billing_manual/SQL Upload - CII Manual Billing 11-2025.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "WP",
    "header_rows_to_skip": 1,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II_ManualBilling"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_ZRBINQ_Billing_Plans'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/ZRBINQ_billing_plans/SQL Upload - Billing Plans 11-2025.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "Upload",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BW_ZRBINQ_BillingPlans"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Alliant_Royalties_Alliant_Queries_Finance_12_2025'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/alliant_royalties/Alliant Queries_ Finance 12_2025.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "GL529050 & 547000",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Exp_RoyaltiesAlliant"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Alliant_Royalties_Alliant_Queries_Finance_GL_113015_12_2025'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/alliant_royalties/Alliant Queries_Finance GL 113015_12 2025.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "113015",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Exp_RoyaltiesAlliant"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Depreciation_Schedule'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/depreciation_schedule/2612 Combined NA Scheduled Depreciation-USD.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "NA Combine DEPRSCH",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Exp_Depreciation"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Daily_Fee_Poker_Install_Base'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/daily_fee_poker_install_base/December 2025 Action Poker Unit Count Data.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "December 2025",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BW_COPA_DailyFeePoker_Units"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Machine_Sales_Revenue_Plugs'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/machine_sales_revenue_plugs/BPC Adhoc Machine Revenue CY25 December Actuals File_New Structure.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "For Garrett",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BPC_Plug_Financials"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Machine_Sales_Cost_Plugs'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/machine_sales_cost_plugs/BPC Adhoc Machine Cost for GBDB Working - 2025 December Actuals.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "Total Cost by Region by CC",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BPC_Plug_Financials_MachineCosts"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Flat_Fee_Billing'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/flat_fee_billing/Hybris Billing File December 2025.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "Hybris Billing December 2025",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Peformance_FF_MJP"
  }},
  {{
    "sheet_name": "Prior Hybris Billing",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Peformance_FF_MJP_PriorMonth"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Lottery_Billing'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/lottery_billing/MJP Reclass 12 -DECEMBER 2025.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "DEL PREMIUM",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Peformance_PremLotto_DE"
  }},
  {{
    "sheet_name": "NYL PREM",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Peformance_PremLotto_NY"
  }},
  {{
    "sheet_name": "RIL PREMIUM",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Peformance_PremLotto_RI"
  }},
  {{
    "sheet_name": "Non-Premium_11_23 to 12_27",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Peformance_NonPremLotto_RI"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Africa_Billing'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/africa_billing/12 Africa 3600 ZRBINQ.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "CC3600 ZRBINQ Dec''25",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Africa"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_EMEA_Fixed_Fee_Billing'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/EMEA_fixed_fee_billing/Fixed Fee Install-based overview EMEA 2025-12 v2.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "Active",
    "header_rows_to_skip": 5,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_FixedFee"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Greece_WLA_Billing'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/flat_fee_billing/Assets_Revenue_November 25.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "Asset Data_Initial Contract",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_GreeceWLA"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_Iceland_Billing'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/iceland_billing/Oct 1stOfMonth_UIL.xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "20251101",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Iceland"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

pipeline_name = 'Atlas_Bronze_LAC_Billing'
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline
    (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
  VALUES ('{pipeline_name}', '{pipeline_name}', true, current_date(), current_date())
""")
row = spark.sql(f"""
  SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline
  WHERE pipeline_name = '{pipeline_name}'
""").collect()[0]
pid = row['pipeline_id']
print(f'  {pipeline_name} -> pipeline_id={pid}')
spark.sql(f"""
  INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_parameter (
    pipeline_id, variable_name, variable_value, created_date, modified_date
  )
  VALUES
    ({pid}, 'db_catalog', 'development_021_bronze_finance.atlas', current_date(), current_date()),
    ({pid}, 'archive_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/', current_date(), current_date()),
    ({pid}, 'failure_folder', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/', current_date(), current_date()),
    ({pid}, 'workbook_path', 's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/lac_billing/Noviembre Upload .xlsx', current_date(), current_date()),
    ({pid}, 'sheet_mapping', '[
  {{
    "sheet_name": "Num",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Intl_Performance_LAC"
  }}
]', current_date(), current_date())
""")
results.append((pipeline_name, pid))

print(f'\nCreated {len(results)} pipelines:')
for name, pid in results:
    print(f'  {pid}: {name}')
