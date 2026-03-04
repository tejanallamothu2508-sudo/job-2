INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_Participation_Billing_Regular', 'Atlas_Bronze_Participation_Billing_Regular', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/participation_billing_regular/New Participation Data - Accruals (SQL) - 01_2026.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "Participation Data",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Performance_PartPriorMonth"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_Participation_Billing_Regular';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_Participation_Billing_Accruals', 'Atlas_Bronze_Participation_Billing_Accruals', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/participation_billing_accruals/New Participation Data w_o Brand Filter (SQL) - 10_2025-12_2025.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "Participation Data",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Performance_PartAccrualMonth"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_Participation_Billing_Accruals';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_CDS_CII_Billing_Regular', 'Atlas_Bronze_CDS_CII_Billing_Regular', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/cds_cii_billing_regular/Copy of CDS CLASS II check data (Dec-25) 01-2026.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "Report 1",
    "header_rows_to_skip": 5,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_CDS_CII_Billing_Regular';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_WAP_Canada_Discounts', 'Atlas_Bronze_WAP_Canada_Discounts', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/wap_canada_discounts/WAP CAN Land Based Final - By EOD 2026-01.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "MZ Format",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Performance_WAP_NewCanada"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_WAP_Canada_Discounts';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_WAP_Billing', 'Atlas_Bronze_WAP_Billing', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/wap_billing/WAP_DATA_-_DISCOUNTS 2026-01.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "Report 1 (1)",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.RPM_Performance_WAP"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_WAP_Billing';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1000', 'Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1000', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/ZRBINQ_billing_plans/ZD5 CORE FLAT FEE CoCode 1000 JANUARY 2026.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "ZRBINQ",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BW_ZRBINQ_BillingPlans"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1000';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1460', 'Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1460', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/ZRBINQ_billing_plans/ZD5 CORE FLAT FEE CoCode 1460 JANUARY 2026.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "ZRBINQ",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BW_ZRBINQ_BillingPlans"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1460';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_GSMA', 'Atlas_Bronze_ZRBINQ_Billing_Plans_GSMA', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/ZRBINQ_billing_plans/ZDJ GSMA FLAT FEE JANUARY 2026.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "ZRBINQ",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BW_ZRBINQ_BillingPlans"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_ZRBINQ_Billing_Plans_GSMA';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1000', 'Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1000', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/ZRBINQ_billing_plans/ZDL HYBRID CoCode 1000 January 2026.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "ZRBINQ",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BW_ZRBINQ_BillingPlans"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1000';


INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, pipeline_description, pipeline_enabled, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1460', 'Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1460', true, current_date(), current_date());

INSERT INTO development_011_bronze_core.db_admin.pipeline_parameter (
  pipeline_id, variable_name, variable_value, created_date, modified_date
)
SELECT p.pipeline_id, pp.variable_name, pp.variable_value, current_date(), current_date()
FROM development_011_bronze_core.db_admin.pipeline p
CROSS JOIN (
  VALUES
    ('db_catalog',      'development_021_bronze_finance.atlas'),
    ('archive_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/archive/excel/'),
    ('failure_folder',  's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/failure/excel/'),
    ('workbook_path',   's3://cluster-private-bucket-481980074735-us-east-1/intake/atlas/manual/ZRBINQ_billing_plans/ZDL HYBRID CoCode 1460 JANUARY 2026.xlsx'),
    ('sheet_mapping',   '[
  {
    "sheet_name": "ZRBINQ",
    "header_rows_to_skip": 0,
    "footer_rows_to_skip": 0,
    "has_header_row": true,
    "destination_table": "development_021_bronze_finance.atlas.BW_ZRBINQ_BillingPlans"
  }
]')
) AS pp(variable_name, variable_value)
WHERE p.pipeline_name = 'Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1460';
