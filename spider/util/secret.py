import os
from spider.constant.secret import SECRET_DIR
from spider.util.file import read_file


def get_secret(secret_key: str) -> str:
    return read_file(os.path.join(
        SECRET_DIR, secret_key)).rstrip()
