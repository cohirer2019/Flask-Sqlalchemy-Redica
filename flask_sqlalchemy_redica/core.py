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

from flask_sqlalchemy import SQLAlchemy, Model, _QueryProperty

from .utils import _md5_key_mangler
from .cache import CachingQuery, query_callable
from .model import CachingInvalidator, CachingMeta

DEFAULT_REDICA_KEY_PREFIX = 'redica'


class CachingModel(Model):
    cache_regions = None
    query_class = CachingQuery


class CachingSQLAlchemy(SQLAlchemy):
    def __init__(self, app=None, **kwargs):
        self.app = app
        self.regions = kwargs.pop('regions', None)
        self.prefix = kwargs.pop('prefix', DEFAULT_REDICA_KEY_PREFIX)

        self.cache_invalidator_class = kwargs.pop(
            'invalidator_class', CachingInvalidator)
        self.cache_invalidator_callback = kwargs.pop(
            'invalidator_callback', None)

        self.query_cls = query_callable(self.regions)

        if 'query_class' in kwargs:
            self.query_cls = kwargs.setdefault('query_class', self.query_cls)
        else:
            self.query_cls = kwargs.setdefault(
                'session_options', {}).setdefault('query_cls', self.query_cls)

        CachingModel.query_class = self.query_cls
        CachingModel.cache_regions = self.regions

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
            key_mangler = functools.partial(_md5_key_mangler, self.prefix)

            self.regions = dict(
                default=make_region().configure(**{
                    'backend': 'extended_redis_backend',
                    'arguments': {
                        'url': redica_cache_url,
                        'redis_expiration_time': expiration_time,
                        'key_mangler': key_mangler
                    }
                })
            )

            CachingModel.cache_regions = self.regions

    def make_declarative_base(self, metadata=None):
        """Creates the declarative base."""
        base = declarative_base(cls=CachingModel, name='Model',
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

    def cache_flush(self, session):
        ctx = stack.top
        if ctx is not None and hasattr(ctx, 'redica_invalidator'):
            ctx.redica_invalidator.flush()

