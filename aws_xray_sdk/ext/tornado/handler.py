# coding: utf8
import logging
import traceback
import functools

import tornado

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.models import http
from aws_xray_sdk.core.models.subsegment import Subsegment
from aws_xray_sdk.ext.util import (
    calculate_sampling_decision,
    calculate_segment_name,
    construct_xray_header
)


class DummyXRayHandler(tornado.web.RequestHandler):
    """ Dummy Handler"""
    def _xray_hook_before(self, *args, **kwargs):
        pass

    def _xray_hook_after(self, *args, **kwargs):
        pass

    def raise_exception_to_xray(self, exc):
        pass


class XRayHandler(tornado.web.RequestHandler):
    def _xray_hook_before(self, *args, **kwargs):
        request = self.request

        xray_header = construct_xray_header(request.headers)

        host = request.headers.get('Host')
        name = calculate_segment_name(host, xray_recorder)

        sampling_decision = calculate_sampling_decision(
            trace_header=xray_header,
            recorder=xray_recorder,
            service_name=host,
            method=request.method,
            path=request.path,
        )

        segment = xray_recorder.begin_segment(
            name=name,
            traceid=xray_header.root,
            parent_id=xray_header.parent,
            sampling=sampling_decision,
        )

        segment.put_http_meta(http.URL, request.path)
        segment.put_annotation('query', request.query)
        segment.put_http_meta(http.METHOD, request.method)

        user_agent = request.headers.get('User-Agent')
        if user_agent:
            segment.put_http_meta(http.USER_AGENT, user_agent)

        x_forwarded_for = request.headers.get('X-Forwarded-For')
        remote_ip = request.headers.get('remote_ip')

        if x_forwarded_for:
            # X_FORWARDED_FOR may come from untrusted source so we
            # need to set the flag to true as additional information
            segment.put_http_meta(http.CLIENT_IP, x_forwarded_for)
            segment.put_http_meta(http.X_FORWARDED_FOR, True)
        elif remote_ip:
            segment.put_http_meta(http.CLIENT_IP, remote_ip)

    def _xray_hook_after(self, *args, **kwargs):
        segment = xray_recorder.current_segment()

        segment.put_http_meta(http.STATUS, self._status_code)

        content_length = self._headers.get('Content-Length')
        if content_length:
            segment.put_http_meta(http.CONTENT_LENGTH, int(content_length))

        xray_recorder.end_segment()

    def raise_exception_to_xray(self, err):
        # Store exception information including the stacktrace to the segment
        segment = xray_recorder.current_segment()
        segment.put_http_meta(http.STATUS, 500)
        stack = traceback.extract_stack(limit=xray_recorder._max_trace_back)
        segment.add_exception(err, stack)


class SyncXRayHandler(XRayHandler):
    """ 同步版本"""
    def prepare(self, *args, **kwargs):
        return self._xray_hook_before(*args,**kwargs)

    def on_finish(self, *args, **kwargs):
        return self._xray_hook_after(*args,**kwargs)
