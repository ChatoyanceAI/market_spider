import functools
import logging

from spider.constant import IS_PRODUCTION

# Silence error since it is not supported:
# https://github.com/google/google-api-python-client/issues/299
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logging.getLogger().setLevel(logging.INFO if IS_PRODUCTION else logging.DEBUG)


def log_info(string):
    logging.info(string)


def log_exception_and_continue(function):
    """
    A decorator that wraps the passed in function and logs
    exceptions should one occur
    """
    logger = logging.getLogger()

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as e:
            logger.info(
                f"There was an exception {e} in {function.__name__}")
            # do not raise the exception

    return wrapper
