"""
Constants related to databases shared across 90 Seconds Airflow.
"""

from spider.util.secret import get_secret

# Schema refers to `database_id` in BigQuery
DWH_SCHEMA = get_secret('dwh_schema')
DWH_CX_SCHEMA = get_secret('dwh_cx_schema')
DWH_FINANCE_SCHEMA = get_secret('dwh_finance_schema')
DWH_GENERAL_SCHEMA = get_secret('dwh_general_schema')
DWH_REVENUE_SCHEMA = get_secret('dwh_revenue_schema')
DWH_MARKETING_SCHEMA = get_secret('dwh_marketing_schema')
DWH_PRODUCT_SCHEMA = get_secret('dwh_product_schema')
DWH_DESCRIPTION_SCHEMA = get_secret('dwh_description_schema')
DWH_SNAPSHOT_SCHEMA = get_secret('dwh_snapshot_schema')
BACKEND_SCHEMA = get_secret('backend_schema')
BACKEND_AUTOMATION_SCHEMA = get_secret('backend_automation_schema')
EXTERNAL_SCHEMA = get_secret('external_schema')
EXTERNAL_BACKEND_SCHEMA = get_secret('external_backend_schema')
SALESFORCE_SCHEMA = get_secret('salesforce_schema')
GOOGLE_SHEET_SCHEMA = get_secret('google_sheet_schema')
EMAIL_FIVETRAN_SCHEMA = get_secret('email_fivetran_schema')
METADATA_SCHEMA = get_secret('metadata_schema')
GOOGLE_ADS_SCHEMA = get_secret('google_ads_schema')
BACKEND_AIRFLOW_SCHEMA = get_secret('backend_airflow_schema')

DWH_SCHEMAS = [
    DWH_SCHEMA, DWH_CX_SCHEMA,
    DWH_FINANCE_SCHEMA, DWH_REVENUE_SCHEMA,
    DWH_MARKETING_SCHEMA, DWH_PRODUCT_SCHEMA
]
