import wrapt
import psycopg2

from aws_xray_sdk.ext.dbapi2 import XRayTracedConn


PG_ATTR = {
    'dbname': 'name',
    'user': 'user',
}


def patch():

    wrapt.wrap_function_wrapper(
        'psycopg2',
        'connect',
        _xray_traced_connect
    )


def _xray_traced_connect(wrapped, instance, args, kwargs):

    conn = wrapped(*args, **kwargs)
    meta = {}

    dsn = conn.get_dsn_parameters()
    for attr, key in PG_ATTR.items():
        value = dsn.get(attr)
        if value:
            meta[key] = value

    if hasattr(conn, '_server_version'):
        version = sanitize_db_ver(getattr(conn, '_server_version'))
        if version:
            meta['database_version'] = version

    return XRayTracedConn(conn, meta)


def sanitize_db_ver(raw):

    if not raw or not isinstance(raw, tuple):
        return raw

    return '.'.join(str(num) for num in raw)
