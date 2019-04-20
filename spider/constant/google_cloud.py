"""
Constants related to google cloud shared across 90 Seconds Airflow.
"""

import os
import re
from functools import partial

from google.cloud import bigquery

from spider.constant import IS_PRODUCTION
from spider.constant.secret import SECRET_DIR


def _read_file_id(id_file):  # Same as get_secret but needs to re-def to prevent circular import.
    return open(_from_secret_dir(id_file), 'r').read().rstrip()


_from_secret_dir = partial(os.path.join, SECRET_DIR)
GCP_CREDENTIALS_PRODUCTION = _from_secret_dir('data-production.json')
GCP_CREDENTIALS_STAGING = _from_secret_dir('data-staging.json')
GCP_CREDENTIALS = GCP_CREDENTIALS_PRODUCTION if IS_PRODUCTION else GCP_CREDENTIALS_STAGING

GCP_PROJECT_PRODUCTION_ID = _read_file_id('gcp_project_production_id')
GCP_PROJECT_STAGING_ID = _read_file_id('gcp_project_staging_id')
GCP_PROJECT_ID = GCP_PROJECT_PRODUCTION_ID if IS_PRODUCTION else GCP_PROJECT_STAGING_ID

BIGQUERY_LOCATION = 'US'
BIGQUERY_CLIENT = bigquery.Client.from_service_account_json(GCP_CREDENTIALS)

GCS_BACKEND_STAGING_ID = '90seconds-backend-staging-sync'
GCS_BACKEND_PRODUCTION_ID = '90seconds-backend-production-sync'
GCS_BACKEND_DEVELOPMENT_ID = '90seconds-backend-development-sync'

# https://cloud.google.com/storage/docs/naming#requirements
GCS_REGEX = re.compile(r"^gs://(?!g(o|0){2}g)[a-z\d][a-z\d\-_]{2,62}/\S+\.[a-zA-Z\d]{,3}$")
