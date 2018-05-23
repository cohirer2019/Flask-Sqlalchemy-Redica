# -*- coding: utf-8 -*-
import functools

from dogpile.cache.region import make_region
from sqlalchemy import event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack

from flask_sqlalchemy import SQLAlchemy, _QueryProperty, Model

from .utils import _md5_key_mangler
from .cache import CachingQuery
from .model import CachingInvalidator, CachingMeta, CeleryCachingInvalidator, \
    Cache

DEFAULT_REDICA_KEY_PREFIX = 'redica'


class CachingSQLAlchemy(SQLAlchemy):
    def __init__(self, app=None, **kwargs):
        self.app = app
        self.regions = kwargs.pop('regions', None)
        self.prefix = kwargs.pop('prefix', DEFAULT_REDICA_KEY_PREFIX)

        self.cache_invalidator_class = kwargs.pop(
            'invalidator_class', None)
        self.cache_invalidator_callback = kwargs.pop(
            'invalidator_callback', None)

        if 'query_class' in kwargs:
            self.query_cls = kwargs.setdefault('query_class', CachingQuery)
        else:
            kwargs['query_class'] = self.query_cls = kwargs.setdefault(
                'session_options', {}).setdefault('query_cls', CachingQuery)

        Model.query_class = CachingQuery

        super(CachingSQLAlchemy, self).__init__(app, **kwargs)

    def init_app(self, app):
        self.init_regions(app)
        self.init_events()

        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['sqlalchemy_redica'] = self

        super(CachingSQLAlchemy, self).init_app(app)

    def init_regions(self, app):
        if not self.regions:
            expiration_time = app.config.setdefault(
                'REDICA_DEFAULT_EXPIRE', 3600)
            redica_cache_url = app.config.get('REDICA_CACHE_URL')
            if not self.cache_invalidator_class:
                redica_invalidator_type = app.config.get('REDICA_INVALIDATOR_TYPE')
                if redica_invalidator_type == 'celery':
                    self.cache_invalidator_class = CeleryCachingInvalidator
                else:
                    self.cache_invalidator_class = CachingInvalidator

            key_mangler = functools.partial(_md5_key_mangler, self.prefix)

            self.regions = dict(
                default=make_region().configure(**{
                    'backend': 'extended_redis_backend',
                    'expiration_time': expiration_time,
                    'arguments': {
                        'url': redica_cache_url,
                        'redis_expiration_time': expiration_time + 30,
                        'key_mangler': key_mangler
                    }
                })
            )

            Cache.default_regions = self.regions
            CachingQuery.default_regions = self.regions

    def make_declarative_base(self, model, metadata=None):
        """Creates the declarative base."""
        base = declarative_base(cls=model, name='Model',
                                metadata=metadata,
                                metaclass=CachingMeta)
        base.query = _QueryProperty(self)
        return base

    @property
    def cache_invalidator(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'redica_invalidator'):
                ctx.redica_invalidator = self.cache_invalidator_class(
                    self.cache_invalidator_callback)
            return ctx.redica_invalidator

    def init_events(self):
        event.listen(Session, 'after_commit', self.cache_flush)

    @staticmethod
    def cache_flush(session):
        ctx = stack.top
        if ctx is not None and hasattr(ctx, 'redica_invalidator'):
            ctx.redica_invalidator.flush()

