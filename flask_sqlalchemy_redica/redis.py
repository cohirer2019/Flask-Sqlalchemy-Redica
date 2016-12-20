# -*- coding: utf-8 -*-
from dogpile.cache import register_backend
from dogpile.cache.backends.redis import RedisBackend


class ExtendRedisBackend(RedisBackend):
    def __init__(self, arguments):
        self.key_mangler = arguments.pop('key_mangler', None)
        super(ExtendRedisBackend, self).__init__(arguments)

    def keys(self, pattern):
        if self.key_mangler:
            pattern = self.key_mangler(pattern)
        return self.client.keys(pattern)

register_backend(
    "extended_redis_backend", "flask_sqlalchemy_redica.redis", "ExtendRedisBackend")
