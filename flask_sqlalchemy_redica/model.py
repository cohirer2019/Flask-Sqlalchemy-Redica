# -*- coding: utf-8 -*-
import importlib
import itertools

import functools
from blinker import signal
from dogpile.cache.api import NO_VALUE
from flask import current_app
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import event, inspect
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import get_history
from sqlalchemy.orm.base import PASSIVE_NO_INITIALIZE
from werkzeug.local import LocalProxy

from .cache import FromCache


class Cache(object):
    def __init__(self, model, regions, label, exclude_columns=None):
        self.model = model
        self.regions = regions
        self.label = label
        self.pk = getattr(model, 'cache_pk', 'id')
        self.exclude_columns = set(exclude_columns) \
            if exclude_columns else set()
        self.columns = None

        if not regions:
            redica_ext = current_app.extensions['sqlalchemy_redica']
            self.regions = redica_ext.regions

    def get(self, pk):
        return self.model.query.options(self.from_cache(pk=pk)).get(pk)

    def filter(self, **kwargs):
        limit = kwargs.pop('limit', None)
        offset = kwargs.pop('offset', None)
        order_by = kwargs.pop('order_by', 'asc')

        query_kwargs = {}
        if kwargs:
            if len(kwargs) > 1:
                raise TypeError(
                    'filter accept only one attribute for filtering')
            key, value = kwargs.items()[0]
            if key == self.pk:
                yield self.get(value)
                return

            if key not in self._columns():
                raise TypeError('%s does not have an attribute %s' % self, key)
            query_kwargs[key] = value

        cache_key = self.cache_key(**kwargs)
        pks = self.regions[self.label].get(cache_key)

        if pks is NO_VALUE:
            pks = [o.id for o in self.model.query.filter_by(
                **query_kwargs).with_entities(getattr(self.model, self.pk))]
            self.regions[self.label].set(cache_key, pks)

        if order_by == 'desc':
            pks.reverse()

        if offset is not None:
            pks = pks[pks:]

        if limit is not None:
            pks = pks[:limit]

        keys = [self.cache_key(pk) for pk in pks]
        for pos, obj in enumerate(self.regions[self.label].get_multi(keys)):
            if obj is NO_VALUE:
                yield self.get(pks[pos])
            else:
                yield obj[0]

    def flush(self, key):
        self.regions[self.label].delete(key)

    def keys(self, key_pattern):
        return self.regions[self.label].backend.keys(key_pattern)

    def flush_multi(self, key_pattern):
        if not key_pattern.endswith('*'):
            key_pattern += '*'
        backend = self.regions[self.label].backend
        keys = backend.keys(key_pattern)
        if len(keys) > 0:
            backend.delete_multi(keys)

    def _columns(self):
        if not self.columns:
            self.columns = [
                c.name for c in self.model.__table__.columns
                if c.name != self.pk and c.name not in self.exclude_columns]

        return self.columns

    def from_cache(self, cache_key=None, pk=None, prefix=None):
        if pk:
            cache_key = self.cache_key(pk)
        return FromCache(self.label, cache_key, query_prefix=prefix)

    def cache_key(self, pk='all', **kwargs):
        q_filter = ''.join('{}={}'.format(k, v) for k, v in kwargs.items()) \
            or self.pk
        return "{}:{}:object:{}".format(self.model.__table__, pk, q_filter)

    def cache_relationship_key(self, pk, relation_name):
        return '{}:{}:relationship:{}'.format(
            self.model.__tablename__, pk, relation_name)

    def cache_query_key(self, pk, query_name):
        return '{}:{}:query:{}'.format(self.model.__tablename__, pk, query_name)

    def flush_filters(self, obj):
        for column in self._columns():
            added, _, deleted = get_history(
                obj, column, passive=PASSIVE_NO_INITIALIZE)
            for value in itertools.chain(added or (), deleted or ()):
                self.flush(self.cache_key(**{column: value}))

    def flush_caches(self, obj_pk):
        self.flush(self.cache_key(obj_pk))
        self.flush_multi(self.cache_relationship_key(obj_pk, '*'))
        self.flush_multi(self.cache_query_key(obj_pk, '*'))

    def flush_all(self, obj):
        self.flush_filters(obj)
        self.flush(self.cache_key())

        obj_pk = getattr(obj, self.pk)
        if obj_pk:
            self.flush_caches(obj_pk)


_flush_signal = signal('_flask_sqlalchemy_redica_flush')


class CacheableMixin(object):
    cache_enable = True
    cache_label = 'default'
    cache_regions = None
    cache_expiration_time = 3600
    cache_columns = ()
    # these columns will not produce cache indices
    cache_exclude_columns = ()

    cache_invalidate = True
    cache_invalidate_columns = ()
    cache_invalidate_queries = ()
    cache_invalidate_relationships = ()
    # these columns changes will not produce cache flush
    cache_invalidate_exclude_columns = {'update_at', 'create_at'}

    cache_invalidate_notify = True
    cache_invalidate_notify_relationships = ()

    _initialized = False
    _all_columns = set()

    @declared_attr.cascading
    def cache(cls):
        if cls.cache_enable:
            return Cache(
                cls, cls.cache_regions, cls.cache_label,
                exclude_columns=cls.cache_exclude_columns
            )

    @classmethod
    def _flush_all(cls, target_id, target):
        if cls.cache_enable and cls.cache_invalidate:
            if target:
                cls.cache.flush_all(target)
            elif target_id:
                cls.cache.flush_caches(target_id)

    @classmethod
    def relationship_cache_key(cls, pk, relation_name):
        return cls.cache.cache_relationship_key(pk, relation_name)

    @classmethod
    def query_cache_key(cls, pk, query_name):
        return cls.cache.cache_query_key(pk, query_name)

    @classmethod
    def on_invalidate_notify(cls, sender, **kw):
        target = kw.get('target')
        target_id = kw.get('target_id')
        src = kw.get('src')
        ev = kw.get('event')

        cls._flush_all(target_id, target)

        if src == 'on_model_changed' \
                and ev == 'update' \
                and not cls.has_changes(target):
            # for self update, if no changes, then do not notify others
            return

        delay = True
        if ev == 'delete' or src != 'on_model_changed':
            # deletion need flush right away
            delay = False

        cls._notify_all(target, ev, delay=delay)

    @classmethod
    def listen_mapper_events(cls, mapper, sender, callback):
        object_update_callback = functools.partial(
            callback, sender, 'update')
        object_delete_callback = functools.partial(
            callback, sender, 'delete')
        object_insert_callback = functools.partial(
            callback, sender, 'insert')
        event.listen(mapper, 'after_update', object_update_callback)
        event.listen(mapper, 'after_delete', object_delete_callback)
        event.listen(mapper, 'after_insert', object_insert_callback)

    @classmethod
    def on_model_changed(cls, *args):
        sender, ev, _, _, target = args
        kwargs = dict(module=cls.__module__, model=sender,
                      target=target, event=ev, src='on_model_changed')
        _flush_signal.send(sender, **kwargs)

    @classmethod
    def init_invalidate_columns(cls, mapper):
        cls._all_columns = set(mapper.attrs.keys())
        cls.cache_invalidate_columns = cls.cache_invalidate_columns or \
            cls._all_columns - cls.cache_invalidate_exclude_columns

    @classmethod
    def has_changes(cls, target, use_all=False):
        columns = cls._all_columns if use_all else cls.cache_invalidate_columns
        for column in columns:
            if get_history(target, column,
                           passive=PASSIVE_NO_INITIALIZE).has_changes():
                return True

    @classmethod
    def relation_changes(cls, target, attr, r):
        history = get_history(target, r)
        if attr.back_populates \
                and attr.cascade_backrefs \
                and history.has_changes():
            # backref can update itself
            # no need to broadcast signals
            dataset = history.unchanged or ()
        else:
            dataset = history.sum()

        for obj in dataset:
            if not obj or obj.id is None:
                # for new obj, it will flush by itself,
                # no need to broadcast signal
                continue

            yield obj

    @classmethod
    def _notify_all(cls, target, ev, delay=False):
        if not target:
            return

        mapper = inspect(cls).mapper
        for r in cls.cache_invalidate_notify_relationships:
            attr = mapper.attrs.get(r)
            sender = attr.mapper.class_.__name__
            for obj in cls.relation_changes(target, attr, r):
                kwargs = dict(
                    module=attr.mapper.class_.__module__, model=sender,
                    target=target, target_id=obj.id, event=ev, src='on_notify')
                if delay:
                    cache_invalidator.invalidate(**kwargs)
                else:
                    _flush_signal.send(sender, **kwargs)

    @classmethod
    def __declare_last__(cls):
        if cls._initialized:
            return

        if not cls.cache_enable and \
                not cls.cache_invalidate and \
                not cls.cache_invalidate_notify:
            return

        mapper = inspect(cls).mapper
        sender = mapper.class_.__name__
        cls.listen_mapper_events(mapper, sender, cls.on_model_changed)
        _flush_signal.connect(
            cls.on_invalidate_notify, sender=sender, weak=False)

        base_mapper = inspect(cls).mapper.base_mapper
        if base_mapper != mapper:
            sender = base_mapper.class_.__name__
            cls.listen_mapper_events(base_mapper, sender,
                                     cls.on_model_changed)
            _flush_signal.connect(
                cls.on_invalidate_notify, sender=sender, weak=False)

        cls.init_invalidate_columns(mapper)
        cls._initialized = True


def do_flush(invalidator):
    session = current_app.extensions['sqlalchemy_redica'].session
    invalidator.invalidate_nofity(session)


class CachingInvalidator:
    def __init__(self, async=False, callback=None):
        self.items = []
        self.async = async
        self.callback = callback or do_flush

    def invalidate(self, **kwargs):
        if self.async:
            kwargs.pop('target', None)
        self.items.append(kwargs)

    def invalidate_nofity(self, session):
        for info in self.items:
            info['src'] = 'on_flush'
            module = info.get('module')
            model = info.get('model')
            target = info.get('target')
            target_id = info.get('target_id')
            if not target and target_id:
                model_cls = getattr(importlib.import_module(module), model)
                info['target'] = session.query(model_cls).get(target_id)
            _flush_signal.send(model, **info)

    def flush(self):
        self.callback(self)


def _get_cache_invalidator():
    redica_ext = current_app.extensions['sqlalchemy_redica']
    return redica_ext.cache_invalidator

cache_invalidator = LocalProxy(lambda: _get_cache_invalidator())


@event.listens_for(Session, 'after_commit')
def cache_after_commit(session):
    cache_invalidator.flush()
