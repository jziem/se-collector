import os


def get_int(param_name: str, default: int) -> int:
    v = os.environ.get(param_name)
    return int(v) if v is not None else default


def get_str(param_name: str, default: str = None) -> str:
    v = os.environ.get(param_name)
    return str(v) if v is not None else default


DBSCHEMA = get_str('DB_SCHEMA', 'lsx')
DB_TBL_PREFIX = get_str('DB_TABLE_PREFIX', '')
DBNAME = get_str('DB_NAME', 'lsx')
DBUSER = get_str('DB_USR', 'lsx')
DBPWD = get_str('DB_PASS', '4lsx')
DBHOST = get_str('DB_HOST', 'localhost')
