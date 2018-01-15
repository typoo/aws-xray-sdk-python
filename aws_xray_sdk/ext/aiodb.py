import asyncio

import copy
import wrapt

from aws_xray_sdk.core import xray_recorder


class XRayTracedAsyncConn(wrapt.ObjectProxy):

    _xray_meta = None

    def __init__(self, conn, meta={}):

        super(XRayTracedAsyncConn, self).__init__(conn)
        self._xray_meta = meta

        class XRayTracedCursor(self.cursorclass):
            def __init__(self, *args, **kwargs):
                self._xray_meta = meta

                super(XRayTracedCursor, self).__init__(*args, **kwargs)

                # we preset database type if db is framework built-in
                #if not self._xray_meta.get('database_type'):
                #    db_type = super().__class__.__module__.split('.')[0]
                #    self._xray_meta['database_type'] = db_type

            @xray_recorder.capture_async()
            @asyncio.coroutine
            def execute(self, query, *args, **kwargs):

                self._xray_meta['sanitized_query'] = query
                add_sql_meta(self._xray_meta)
                result = yield from super().execute(query, *args, **kwargs)
                return result

            @xray_recorder.capture_async()
            @asyncio.coroutine
            def executemany(self, query, *args, **kwargs):

                self._xray_meta['sanitized_query'] = query
                add_sql_meta(self._xray_meta)
                result = yield from super().executemany(query, *args, **kwargs)
                return result

            @xray_recorder.capture_async()
            @asyncio.coroutine
            def callproc(self, proc, args):

                self._xray_meta['sanitized_query'] = query
                add_sql_meta(self._xray_meta)
                result = yield from super().callproc(proc, args)
                return result

        # Here we wrap the cursor class instead of the cursor itself
        self.cursorclass = XRayTracedCursor


def add_sql_meta(meta):

    subsegment = xray_recorder.current_subsegment()

    if not subsegment:
        return

    if meta.get('name', None):
        subsegment.name = meta['name']

    sql_meta = copy.copy(meta)
    if sql_meta.get('name', None):
        del sql_meta['name']
    subsegment.set_sql(sql_meta)
    subsegment.namespace = 'remote'
