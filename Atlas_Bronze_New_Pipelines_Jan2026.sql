-- =============================================================================
-- Atlas Pipeline — New Pipelines (January 2026 files)
-- Source: Atlas_Migration (1).xlsx — Source_System_Inventory sheet
-- Pattern: matches existing Atlas_Bronze_* pipeline inserts
-- Uses subquery for pipeline_id — safe regardless of auto-increment state
-- =============================================================================


-- =============================================================================
-- 1. PARTICIPATION BILLING REGULAR
--    Source ID 1 → RPM_Performance_PartPriorMonth
--    File: New Participation Data - Accruals (SQL) - 01_2026.xlsx
--    Sheet: Participation Data
--    Owner: Maria | System: Phoenix
--    S3 subfolder: participation_billing_regular/
-- =============================================================================

INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_Participation_Billing_Regular', current_date(), current_date());

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


-- =============================================================================
-- 2. PARTICIPATION BILLING ACCRUALS
--    Source ID 2 → RPM_Performance_PartAccrualMonth
--    Files (rolling 6-month lookback, load one at a time):
--      - New Participation Data w_o Brand Filter (SQL) - 10_2025-12_2025.xlsx
--      - New Participation Data w_o Brand Filter (SQL) - 7_2025-9_2025.xlsx
--    Sheet: Participation Data
--    Owner: Maria | System: Phoenix
--    S3 subfolder: participation_billing_accruals/
--    NOTE: Upload one file, run job, then upload the next file and run again.
--          workbook_path below is for the Oct-Dec file.
--          For Jul-Sep file, update workbook_path to point to that file.
-- =============================================================================

INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_Participation_Billing_Accruals', current_date(), current_date());

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


-- =============================================================================
-- 3. CDS CII BILLING REGULAR
--    Source ID 5 → RPM_Performance_CDS_Class_II
--    File: Copy of CDS CLASS II check data (Dec-25) 01-2026.xlsx
--    Sheet: Report 1 (main data — rolling 4 months, 1 month in arrears)
--    Owner: Josh | System: Phoenix
--    S3 subfolder: cds_cii_billing_regular/
-- =============================================================================

INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_CDS_CII_Billing_Regular', current_date(), current_date());

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


-- =============================================================================
-- 4. WAP CANADA DISCOUNTS
--    Source ID 4 → RPM_Performance_WAP_NewCanada
--    File: WAP CAN Land Based Final - By EOD 2026-01.xlsx
--    Sheet: MZ Format
--    Owner: Maria | System: Phoenix/BRIM
--    S3 subfolder: wap_canada_discounts/
-- =============================================================================

INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_WAP_Canada_Discounts', current_date(), current_date());

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


-- =============================================================================
-- 5. WAP BILLING
--    Source ID 3 → RPM_Performance_WAP
--    File: WAP_DATA_-_DISCOUNTS 2026-01.xlsx
--    Sheet: Report 1 (1)
--    Owner: Maria | System: Hana DB
--    S3 subfolder: wap_billing/
--    NOTE: Maria truncates and reloads a rolling 6 months every month.
--          This pipeline should run monthly with the latest file.
-- =============================================================================

INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_WAP_Billing', current_date(), current_date());

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


-- =============================================================================
-- 6–10. ZRBINQ BILLING PLANS (Source ID 8 → BW_ZRBINQ_BillingPlans)
--       ALL 5 files go to the SAME table — ZRBINQ sheet only
--       Owner: Josh | System: Accounting
--       S3 subfolder: ZRBINQ_billing_plans/ (same as existing pipeline)
--
--       !! IMPORTANT: All 5 pipelines load into BW_ZRBINQ_BillingPlans.
--          Since the notebook TRUNCATES on each run, run these pipelines
--          in sequence. The table will contain only the LAST file's data
--          unless the notebook is changed to APPEND instead of TRUNCATE
--          for subsequent files. Confirm with Teja whether to:
--          (a) TRUNCATE+RELOAD each file separately (each replaces prior data), or
--          (b) APPEND all 5 files into the same table in one batch.
-- =============================================================================

-- 6. ZD5 CORE FLAT FEE CoCode 1000
INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1000', current_date(), current_date());

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


-- 7. ZD5 CORE FLAT FEE CoCode 1460
INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_CoCode_1460', current_date(), current_date());

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


-- 8. ZDJ GSMA FLAT FEE
INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_GSMA', current_date(), current_date());

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


-- 9. ZDL HYBRID CoCode 1000
INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1000', current_date(), current_date());

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


-- 10. ZDL HYBRID CoCode 1460
INSERT INTO development_011_bronze_core.db_admin.pipeline
  (pipeline_name, created_date, modified_date)
VALUES ('Atlas_Bronze_ZRBINQ_Billing_Plans_Hybrid_1460', current_date(), current_date());

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
