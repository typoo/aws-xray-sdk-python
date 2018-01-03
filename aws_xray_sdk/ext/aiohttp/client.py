import asyncio

import wrapt

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.models import http
from aws_xray_sdk.ext.util import inject_trace_header


def patch():

    wrapt.wrap_function_wrapper(
        'aiohttp',
        'ClientSession._request',
        _xray_traced_requests
    )

    wrapt.wrap_function_wrapper(
        'aiohttp',
        'ClientSession._prepare_headers',
        _inject_header
    )


@asyncio.coroutine
def _xray_traced_requests(wrapped, instance, args, kwargs):

    url = kwargs.get('url') or args[1]

    result = yield from xray_recorder.record_subsegment_async(
        wrapped, instance, args, kwargs,
        name=url,
        namespace='remote',
        meta_processor=requests_processor,
    )
    return result


def _inject_header(wrapped, instance, args, kwargs):
    headers = kwargs.get('headers', {})
    inject_trace_header(headers, xray_recorder.current_subsegment())

    return wrapped(*args, **kwargs)


def requests_processor(wrapped, instance, args, kwargs,
                       return_value, exception, subsegment, stack):

    method = kwargs.get('method') or args[0]
    url = kwargs.get('url') or args[1]

    subsegment.put_http_meta(http.METHOD, method)
    subsegment.put_http_meta(http.URL, url)

    if return_value is not None:
        subsegment.put_http_meta(http.STATUS, return_value.status)
    elif exception:
        subsegment.add_exception(exception, stack)
