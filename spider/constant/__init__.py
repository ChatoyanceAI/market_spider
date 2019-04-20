import os

CUR_ENV = os.getenv('ENV', default='staging')
IS_PRODUCTION = True if CUR_ENV == 'production' else False
DIR = os.path.dirname(os.path.realpath(__file__))
