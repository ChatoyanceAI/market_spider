import functools
import os
import re
from typing import Union, Callable, Any

import aiofiles
import isodate

from spider.constant import GCS_REGEX
from spider.util import log_info


def read_file(filepath: str, method: str = 'r') -> str:
    with open(filepath, method) as file:
        read_data = file.read()
    return read_data


def read_sql(sql_filepath: str, sql_param: dict = {}) -> str:
    return apply_default_query_parameters(read_file(sql_filepath, 'r'), **sql_param)


def is_gc_path(filepath: str) -> bool:
    return bool(re.match(GCS_REGEX, filepath))


def apply_default_query_parameters(query: str, **non_default) -> str:
    # unable to import at the beginning of the file
    from spider.constant import (
        DWH_SCHEMA, DWH_GENERAL_SCHEMA, EXTERNAL_SCHEMA, EXTERNAL_BACKEND_SCHEMA,
        DWH_SNAPSHOT_SCHEMA, SALESFORCE_SCHEMA, GOOGLE_SHEET_SCHEMA, EMAIL_FIVETRAN_SCHEMA,
        GOOGLE_ADS_SCHEMA, BACKEND_AUTOMATION_SCHEMA
    )
    return query.format(
        backend_schema=EXTERNAL_BACKEND_SCHEMA,
        backend_automation_schema=BACKEND_AUTOMATION_SCHEMA,
        dwh_schema=DWH_SCHEMA,
        dwh_general_schema=DWH_GENERAL_SCHEMA,
        dwh_snapshot_schema=DWH_SNAPSHOT_SCHEMA,
        external_schema=EXTERNAL_SCHEMA,
        salesforce_schema=SALESFORCE_SCHEMA,
        google_sheet_schema=GOOGLE_SHEET_SCHEMA,
        email_fivetran_schema=EMAIL_FIVETRAN_SCHEMA,
        google_ads_schema=GOOGLE_ADS_SCHEMA,
        **non_default
    )


def get_dir_name(path: str) -> str:
    return os.path.basename(os.path.dirname(path))


def get_file_name(path: str,
                  with_extension: bool = False) -> str:
    basename = os.path.basename(path)
    return basename if with_extension else os.path.splitext(basename)[0]


def iso8601_duration_to_seconds(iso8601_duration: str) -> int:
    return isodate.parse_duration(
        iso8601_duration).total_seconds()


def include_keys(keys):
    """
    Decorator to only return dictionary with only keys in provided list.
    """
    assert isinstance(keys, list)

    def dict_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            dictionary = func(*args, **kwargs)
            key_set = set(keys) & set(dictionary.keys())
            return {key: dictionary.get(key) for key in key_set}

        return func_wrapper

    return dict_decorator


async def async_read_file(filepath: str, method: str = 'r') -> Union[str, bytes]:
    async with aiofiles.open(filepath, method) as f:
        try:
            return await f.read()
        except UnicodeDecodeError:
            if method == 'r':
                return await async_read_file(filepath, 'rb')
            else:
                raise UnicodeDecodeError


def replace_if_invalid(to_validate: Any, replacement: Any, validate_with: Callable):
    try:
        validate_with(to_validate)
        return to_validate
    except Exception as e:
        log_info(e)
        return replacement
