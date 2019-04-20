import os
from datetime import datetime, timedelta

from spider.constant import IS_PRODUCTION

GOOGLE_CLOUD_CONNECTION_STAGING_ID = 'google_cloud_staging'
GOOGLE_CLOUD_CONNECTION_PRODUCTION_ID = 'google_cloud_production'
GOOGLE_CLOUD_CONNECTION_ID = GOOGLE_CLOUD_CONNECTION_PRODUCTION_ID \
    if IS_PRODUCTION else GOOGLE_CLOUD_CONNECTION_STAGING_ID
GOOGLE_CLOUD_CONNECTION_TYPE = 'google_cloud_platform'
BIGQUERY_SCOPE = 'https://www.googleapis.com/auth/bigquery'
CLOUD_STORAGE_SCOPE = 'https://www.googleapis.com/auth/devstorage.read_write'

POSTGRES_CONNECTION_ID = 'backend_google_cloud_postgresql'
POSTGRES_CONNECTION_TYPE = 'postgres'
POSTGRES_HOST = '127.0.0.1'
POSTGRES_SCHEMA = 'vaas_production'
POSTGRES_USER = os.getenv('BACKEND_POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('BACKEND_POSTGRES_PASSWORD')
POSTGRES_PORT = '5431'

DATA_TEAM_EMAIL = 'clevel@chatoyance.org'

DATA_POOL = 'data'
DATA_POOL_SLOTS = 16

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': [DATA_TEAM_EMAIL],
    'email_on_failure': True if IS_PRODUCTION else False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'pool': DATA_POOL
}
