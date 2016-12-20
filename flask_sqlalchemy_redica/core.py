# -*- coding: utf-8 -*-
import functools
import importlib

from dogpile.cache.region import make_region
from sqlalchemy.ext.declarative import declarative_base

try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack

from flask_sqlalchemy import SQLAlchemy, Model, _BoundDeclarativeMeta, \
    _QueryProperty

from .utils import md5_key_mangler
from .cache import CachingQuery, query_callable
from .model import CacheableMixin, CachingInvalidator


class CachingModel(Model, CacheableMixin):
    cache_enable = False
    query_class = CachingQuery


class CachingSQLAlchemy(SQLAlchemy):
    def __init__(self, app=None, **kwargs):
        self.app = app
        self.regions = kwargs.pop('regions', None)
        self.key_prefix = kwargs.pop('key_prefix', None)

        self.cache_invalidator_class = kwargs.pop(
            'invalidator_class', CachingInvalidator)
        self.cache_invalidator_sync = kwargs.pop(
            'invalidator_sync', False)

        self.query_cls = query_callable(self.regions)

        if 'query_class' in kwargs:
            self.query_cls = kwargs.setdefault('query_class', self.query_cls)
        else:
            self.query_cls = kwargs.setdefault(
                'session_options', {}).setdefault('query_cls', self.query_cls)

        CachingModel.query_class = self.query_cls

        super(CachingSQLAlchemy, self).__init__(app, **kwargs)

    def init_app(self, app):
        self.init_regions(app)

        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['sqlalchemy_redica'] = self

        super(CachingSQLAlchemy, self).init_app(app)

    def init_regions(self, app):
        if not self.regions:
            expiration_time = app.config.setdefault(
                'REDICA_DEFAULT_EXPIRE', 3600)
            redica_cache_url = app.config.get('REDICA_CACHE_URL')
            key_mangler = functools.partial(md5_key_mangler, self.key_prefix)

            self.regions = dict(
                default=make_region().configure({
                    'backend': 'extended_redis_backend',
                    'expiration_time': expiration_time,
                    'arguments': {
                        'url': redica_cache_url,
                        'redis_expiration_time': expiration_time,
                        'key_mangler': key_mangler
                    }
                })
            )

    def make_declarative_base(self, metadata=None):
        """Creates the declarative base."""
        base = declarative_base(cls=CachingModel, name='Model',
                                metadata=metadata,
                                metaclass=_BoundDeclarativeMeta)
        base.query = _QueryProperty(self)
        return base

    @property
    def cache_invalidator(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, '_redica_invalidator'):
                ctx._redica_invalidator = CachingInvalidator(
                    self.cache_invalidator_sync)
            return ctx._redica_invalidator

