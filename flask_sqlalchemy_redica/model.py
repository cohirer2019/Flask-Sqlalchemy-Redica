# -*- coding: utf-8 -*-
import importlib
import itertools

import functools
from blinker import signal
from dogpile.cache.api import NO_VALUE
from flask_sqlalchemy import _BoundDeclarativeMeta, Model
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import event, inspect
from sqlalchemy.orm.attributes import get_history
from sqlalchemy.orm.base import PASSIVE_NO_INITIALIZE

from .utils import current_redica
from .cache import FromCache


class Cache(object):
    default_regions = None

    def __init__(self, model, regions, label,
                 columns=None, exclude_columns=None,
                 invalidate_queries=None, invalidate_relationships=None,
                 expiration_time=None):
        self.model = model
        self.cache_regions = regions
        self.label = label
        self.pk = getattr(model, 'cache_pk', 'id')
        self.exclude_columns = set(exclude_columns) \
            if exclude_columns else set()
        self.columns = set(columns) if columns else set()
        self.invalidate_queries = invalidate_queries
        self.invalidate_relationships = invalidate_relationships
        self.expiration_time = expiration_time

    @property
    def regions(self):
        return self.cache_regions or self.default_regions

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

            if key not in self._columns:
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
        self.regions[self.label].delete(key, key_mangle=True)

    def keys(self, key_pattern):
        return self.regions[self.label].backend.keys(key_pattern)

    def flush_multi(self, key_pattern):
        if not key_pattern.endswith('*'):
            key_pattern += '*'
        backend = self.regions[self.label].backend
        keys = backend.keys(key_pattern)
        if len(keys) > 0:
            backend.delete_multi(keys)

    @property
    def _columns(self):
        if not self.columns:
            self.columns = set([
                c.name for c in self.model.__table__.columns
                if c.name != self.pk and c.name not in self.exclude_columns])

        return self.columns

    def from_cache(self, cache_key=None, pk=None, prefix=None):
        if pk:
            cache_key = self.cache_key(pk)
        return FromCache(
            self.label, cache_key, query_prefix=prefix,
            cache_regions=self.regions, expiration_time=self.expiration_time)

    def cache_key(self, pk='all', **kwargs):
        q_filter = ''.join('{}={}'.format(k, v) for k, v in kwargs.items()) \
                   or self.pk
        return "{}:{}:object:{}".format(
            self.model.__table__, pk, q_filter)

    def cache_relationship_key(self, pk, relation_name):
        return '{}:{}:relationship:{}'.format(
            self.model.__tablename__, pk, relation_name)

    def cache_query_key(self, pk, query_name):
        if query_name:
            return '{}:{}:query:{}'.format(
                self.model.__tablename__, pk, query_name)
        else:
            return '{}:{}:query'.format(self.model.__tablename__, pk)

    def flush_filters(self, obj):
        keys = self._filter_keys(obj)
        keys.append(self.cache_key())

        obj_pk = getattr(obj, self.pk)
        if obj_pk:
            keys.append(self.cache_key(obj_pk))

        if len(keys) > 0:
            self.regions[self.label].delete_multi(keys)

    def _filter_keys(self, obj):
        keys = []
        for column in self._columns:
            added, _, deleted = get_history(
                obj, column, passive=PASSIVE_NO_INITIALIZE)
            for value in itertools.chain(added or (), deleted or ()):
                keys.append(self.cache_key(**{column: value}))
        return keys

    def flush_caches(self, obj_pk):
        patterns = self._pattern_keys(obj_pk)

        backend = self.regions[self.label].backend
        ppl = backend.pipeline()
        for p in itertools.imap(lambda k: backend.key_mangler(k), patterns):
            ppl.keys(p)

        keys = []
        for rs in ppl.execute():
            if not rs:
                continue
            keys.extend(rs)
        if len(keys) > 0:
            backend.delete_multi(keys)

    def _pattern_keys(self, obj_pk):
        keys = []

        if self.invalidate_relationships:
            for r in self.invalidate_relationships:
                keys.append(self.cache_relationship_key(obj_pk, r))
        else:
            keys.append(self.cache_relationship_key(obj_pk, '*'))

        if self.invalidate_queries:
            for q in self.invalidate_queries:
                keys.append(self.cache_query_key(obj_pk, q))
        else:
            keys.append(self.cache_query_key(obj_pk, '*'))

        return keys

    def flush_all(self, obj):
        self.flush_filters(obj)

        obj_pk = getattr(obj, self.pk)
        if obj_pk:
            self.flush_caches(obj_pk)


_flush_signal = signal('flask_sqlalchemy_redica_flush_signal')


class CachingConfigure(object):
    #: enable cache
    cache_enable = True

    #: specify user custom dogpile regions
    #: if not specifed, use the default regions created by redica
    cache_regions = None

    #: specify which dogpile region to use
    cache_label = 'default'

    #: cache expiration time, default is 1 hour
    cache_expiration_time = 3600

    #: if not specified, cache will expire all queries of this object
    cache_queries = ()

    #: if not specified, cache will expire all relationships of this object
    cache_relationships = ()

    #: only these columns will produce cache indices
    cache_columns = ()

    #: these columns will not produce cache indices
    cache_exclude_columns = ()

    #: enable cache invalidation
    #: if disabled, cache will only expired until timeout
    #: if enabled, when object changes, cache will invalidate automatically
    cache_invalidate = True

    #: only these columns will produce cache invalidate
    cache_invalidate_columns = ()

    #: these columns changes will not produce cache invalidate
    cache_invalidate_exclude_columns = ()

    #: enable cache invalidate notification
    #: some mapper class can be used only for notification
    #: itself cannot cache and invalidate
    cache_invalidate_notify = False

    #: which relation objects will be notified
    cache_invalidate_notify_relationships = ()

    # private properties
    _initialized = False
    _all_columns = ()


class CachingMixin(CachingConfigure):
    """mixin for caching models."""

    @declared_attr.cascading
    def cache(cls):
        """cache object implementation, will be used like::

                obj = SomeModel.cache.get(id)
        """
        if cls.cache_enable:
            return Cache(
                cls, cls.cache_regions, cls.cache_label,
                columns=cls.cache_columns,
                exclude_columns=cls.cache_exclude_columns,
                invalidate_relationships=cls.cache_relationships,
                invalidate_queries=cls.cache_queries,
                expiration_time=cls.cache_expiration_time
            )

    @declared_attr.cascading
    def use_cache(cls):
        """Helpers for return if this object use cache
        """
        return hasattr(cls, 'cache') and getattr(cls, 'cache_enable')

    @classmethod
    def from_cache(cls, pk='all'):
        query_prefix = cls.query_cache_key(pk, '')
        return cls.cache.from_cache(prefix=query_prefix)

    @classmethod
    def relationship_cache_key(cls, pk, relation_name):
        return cls.cache.cache_relationship_key(pk, relation_name)

    @classmethod
    def query_cache_key(cls, pk, query_name):
        return cls.cache.cache_query_key(pk, query_name)

    @staticmethod
    def invalidator():
        if current_redica:
            return current_redica.cache_invalidator

    @classmethod
    def __declare_last__(cls):
        if cls._initialized:
            return

        if cls.cache_enable is False:
            cls.cache_invalidate = False

        if len(cls.cache_invalidate_notify_relationships) > 0:
            cls.cache_invalidate_notify = True

        if cls.cache_invalidate or cls.cache_invalidate_notify:
            cls.configure_caching()

        cls._initialized = True

    @classmethod
    def configure_caching(cls):
        mapper = inspect(cls).mapper
        sender = mapper.class_.__name__
        cls.listen_mapper_events(mapper, sender, cls.on_model_change)
        _flush_signal.connect(
            cls.on_model_invalidate, sender=sender, weak=False)

        base_mapper = inspect(cls).mapper.base_mapper
        if base_mapper != mapper:
            sender = base_mapper.class_.__name__

            if cls.cache_invalidate or cls.cache_invalidate_notify:
                cls.listen_mapper_events(base_mapper, sender,
                                         cls.on_model_change)

            _flush_signal.connect(
                cls.on_model_invalidate, sender=sender, weak=False)

        cls.init_invalidate_columns(mapper)

    @classmethod
    def init_invalidate_columns(cls, mapper):
        cls._all_columns = set(mapper.attrs.keys())
        cls.cache_invalidate_columns = \
            cls.cache_invalidate_columns or \
            set(cls._all_columns) - set(cls.cache_invalidate_exclude_columns)

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
    def on_model_change(cls, *args):
        sender, ev, _, _, target = args
        kwargs = dict(module=cls.__module__, model=sender, target=target,
                      target_id=target.id, event=ev, src='on_model_change')
        _flush_signal.send(sender, **kwargs)

    @classmethod
    def on_model_invalidate(cls, sender, **kw):
        target = kw.get('target')
        target_id = kw.get('target_id')
        src = kw.get('src')
        ev = kw.get('event')

        if cls.cache_invalidate:
            cls._flush_all(target_id, target)

        if not cls.cache_invalidate_notify:
            return

        if src == 'on_model_change' \
                and ev == 'update' \
                and not cls.has_changes(target):
            # for self update, if no changes, then do not notify others
            return

        delay = True
        if ev == 'delete' or src != 'on_model_change':
            # deletion need flush right away
            delay = False

        cls._notify_all(target, ev, delay=delay)

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
            change_set = history.unchanged or ()
        else:
            change_set = history.sum()

        for obj in change_set:
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
                    target=obj, target_id=obj.id, event=ev, src='on_notify')
                if delay:
                    invalidator = cls.invalidator()
                    if invalidator:
                        invalidator.invalidate(**kwargs)
                else:
                    _flush_signal.send(sender, **kwargs)

    @classmethod
    def _flush_all(cls, target_id, target):
        if cls.cache_enable and cls.cache_invalidate:
            if target:
                cls.cache.flush_all(target)
            elif target_id:
                cls.cache.flush_caches(target_id)


class CachingInvalidator(object):
    def __init__(self, callback=None):
        self.items = []
        self.callback = callback or self.do_flush

    def invalidate(self, **kwargs):
        self.items.append(kwargs)

    @staticmethod
    def do_flush(items):
        if current_redica:
            session = current_redica.create_scoped_session()
            for info in items:
                info['src'] = 'on_flush'
                module = info.get('module')
                model = info.get('model')
                target_id = info.get('target_id')
                model_cls = getattr(importlib.import_module(module), model)
                info['target'] = session.query(model_cls).get(target_id)
                _flush_signal.send(model, **info)
            session.close()

    def flush(self):
        items = list(self.items)
        self.items = []
        self.callback(items)


class CeleryCachingInvalidator(CachingInvalidator):
    def invalidate(self, **kwargs):
        kwargs.pop('target', None)
        super(CeleryCachingInvalidator, self).invalidate(**kwargs)

    def flush(self):
        items = list(self.items)
        self.items = []
        self.callback.delay(items)


def default_caching_invalidate(items):
    CachingInvalidator.do_flush(items)

caching_attributes = [
    (k, v) for k, v in CachingConfigure.__dict__.items()
    if not k.startswith('__')]


class CachingMeta(_BoundDeclarativeMeta):
    def __init__(cls, *args):
        name, bases, dct = args
        super(CachingMeta, cls).__init__(*args)

        if any(itertools.imap(
                lambda x: x != Model and issubclass(x, Model), bases)):
            for k, v in caching_attributes:
                if k not in dct:
                    setattr(cls, k, v)
