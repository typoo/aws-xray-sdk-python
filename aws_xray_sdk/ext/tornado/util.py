# coding: utf8
import functools
import asyncio


def as_asyncio_task(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        coro = func(self, *args, **kwargs)
        return await asyncio.ensure_future(coro)
    return wrapper


def hooked(func):
    @as_asyncio_task
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        self._xray_hook_before()
        result = await func(self, *args, **kwargs)
        self._xray_hook_after()
        return result

    return wrapper


def patch_handler(handler):
    for method in map(str.lower, handler.SUPPORTED_METHODS):
        func = getattr(handler, method)
        if func:
            setattr(handler, method, hooked(func))
