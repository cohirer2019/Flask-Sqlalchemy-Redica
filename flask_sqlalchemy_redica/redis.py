# -*- coding: utf-8 -*-
from __future__ import absolute_import

import functools

from redis import BlockingConnectionPool
from dogpile.cache.region import make_region
from dogpile.cache import register_backend
from dogpile.cache.backends.redis import RedisBackend

from .utils import _md5_key_mangler


class ExtendRedisBackend(RedisBackend):
    def __init__(self, arguments):
        self.key_mangler = arguments.pop('key_mangler', None)
        super(ExtendRedisBackend, self).__init__(arguments)

    def keys(self, pattern, raw=False):
        if not raw and self.key_mangler:
            pattern = self.key_mangler(pattern)
        return self.client.keys(pattern)

    def pipeline(self):
        return self.client.pipeline()


def make_redis_region(app, prefix):
    expiration_time = app.config.setdefault(
        'REDICA_DEFAULT_EXPIRE', 3600)
    key_mangler = functools.partial(_md5_key_mangler, prefix)
    redica_cache_url = app.config.get('REDICA_CACHE_URL')
    cfg = {
        'backend': 'extended_redis_backend',
        'expiration_time': expiration_time,
        'arguments': {
            'redis_expiration_time': expiration_time + 30,
            'key_mangler': key_mangler,
        }
    }
    if app.config.get('REDICA_CACHE_POOL_BLOCKING', True):
        cfg['arguments']['connection_pool'] = BlockingConnectionPool.from_url(
            redica_cache_url)
    else:
        cfg['arguments']['url'] = redica_cache_url

    return dict(
        default=make_region().configure(**cfg)
    )


register_backend(
    "extended_redis_backend", "flask_sqlalchemy_redica.redis",
    "ExtendRedisBackend")
