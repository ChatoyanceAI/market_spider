"""
Constants related to files shared across 90 Seconds Airflow.
"""

import os

DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAG_DIR = os.path.join(DIR, 'dag')
